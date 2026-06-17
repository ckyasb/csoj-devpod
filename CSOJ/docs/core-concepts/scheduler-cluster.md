# Scheduler & Cluster

CSOJ is designed for scalability. It can distribute the judging load across multiple machines, which are organized into clusters. The Scheduler is the brain that manages this process.

## Key Concepts

- **Node**: A scheduler resource target. It can be a Docker host or a Kubernetes-backed virtual node that submits judging Pods to a Kubernetes namespace.
- **Cluster / Partition**: A logical grouping of one or more Nodes. You might have a cluster of high-CPU machines, a cluster of machines with GPUs, or just a single "default" cluster. The scheduler treats clusters as Slurm-like partitions.
- **Scheduler**: The central component in CSOJ that receives all submissions and assigns them to an appropriate Node for execution.
- **QoS / Account / Reservation**: Optional Slurm-like controls for priority, limits, associations, and reserved node/time windows.
- **Fairshare Decay**: Account priority can be reduced by recent historical billing usage recorded in accounting, with older usage decaying by half-life.
- **Preemption**: A high-priority QoS can preempt running submissions from configured lower QoS classes when resources are otherwise unavailable.
- **Job Array**: A single submission request can expand into multiple array task submissions, each with its own submission ID and `array_task_id`.
- **TRES / Billing / Accounting**: CSOJ can convert CPU, memory, GRES, and custom TRES into billing units, enforce billing limits, consume configured `license/...` pools, and record lifecycle events in an accounting table queried through the Admin API.
- **Slurm State View**: APIs keep CSOJ's native submission status, but also expose derived `slurm_state` and `slurm_reason` values such as `PENDING`, `RUNNING`, `SUSPENDED`, `COMPLETED`, `CANCELLED`, `TIMEOUT`, `PREEMPTED`, and `FAILED`.
- **Slurm Command View**: The Admin API exposes `/slurm/sbatch`, `/slurm/scrontab`, `/slurm/salloc`, `/slurm/sbcast`, `/slurm/srun`, `/slurm/sattach`, `/slurm/sstat`, `/slurm/sinfo`, `/slurm/squeue`, `/slurm/sacct`, `/slurm/sreport`, `/slurm/seff`, `/slurm/sacctmgr`, `/slurm/sprio`, `/slurm/sshare`, `/slurm/strigger`, `/slurm/scancel`, and `/slurm/scontrol/...` JSON endpoints so external tools can use Slurm-shaped job, scheduling, accounting, efficiency, account/user/QoS management, priority, fairshare, trigger, and node operations without parsing text.
- **Interactive Allocation**: `/slurm/salloc` can reserve CPU, memory, node, and license resources without launching a workflow, returning Slurm-like environment values. `/slurm/srun` can run a command in that allocation through a short-lived Docker container or Kubernetes Pod, or create a temporary allocation automatically when no `allocation_id` is supplied.

## Configuration

Clusters and nodes are defined in the main `config.yaml` file.

```yaml
# config.yaml
cluster:
  - name: "default-cluster" # Cluster 1
    state: "up"
    priority_tier: 1
    node:
      - name: "node-1"
        cpu: 4    # 4 CPU cores available
        memory: 4096 # 4096 MB memory available
        state: "idle"
        features: ["avx2"]
        runtime: "docker"
        docker:
          host: "tcp://192.168.1.101:2375"
      - name: "node-2"
        cpu: 8
        memory: 8192
        runtime: "kubernetes"
        kubernetes:
          namespace: "csoj-judge"
          kubeconfig: "/etc/csoj/kubeconfig"
          storage_class_name: "fast"
          workdir_size: "2Gi"
  
  - name: "gpu-cluster" # Cluster 2
    node:
      - name: "gpu-node-1"
        cpu: 16
        memory: 32768
        runtime: "kubernetes"
        kubernetes:
          namespace: "csoj-gpu"
          node_selector:
            accelerator: "nvidia"
```

Problems are then assigned to a specific cluster in their `problem.yaml` configuration.

```yaml
# problem.yaml
id: "cuda-problem"
# ...
cluster: "gpu-cluster" # This problem will only be judged on the gpu-cluster
cpu: 2                 # It requires 2 CPU cores
memory: 4096           # It requires 4096 MB of memory
scheduling:
  qos: "urgent"
  nodelist: "gpu-node-[1-4]"
  exclude: "gpu-node-3"
  constraint: "gpu"
  gres: "gpu:1"
  tres: "license/foo:1"
  array: "0-9%2"
# ...
```

## The Scheduling Process

1.  **Submission Received**: A user submits a solution to the "cuda-problem".
2.  **Queueing**: The Scheduler sees that this problem belongs to the `"gpu-cluster"`. It places the submission into that cluster's pending queue.
3.  **Policy Check**: The Scheduler evaluates hold state, begin time, deadline, dependencies, partition state, QoS/account limits, reservations, requested/excluded nodes, node features, GRES, and global license availability.
4.  **Priority and Backfill**: Runnable submissions are sorted by a Slurm-like priority score built from partition tier, QoS priority, account fairshare, decayed historical billing usage, queue age, nice value, manual priority, and job size. Backfill is enabled by default, so a lower-priority runnable submission may start when earlier submissions are waiting for dependencies or resources.
5.  **Resource Check and Preemption**: The Scheduler checks nodes within the `"gpu-cluster"` for CPU, memory, state, requested/excluded node lists, feature, GRES, and reservation compatibility. If resources are unavailable and the queued job's QoS can preempt another QoS, running lower-QoS submissions are cleaned up and their resources are released. Jobs with `requeue` return to the pending queue after `Preempted`/`Requeued` accounting records are written; other preempted jobs are marked failed with reason `Preempted`.
6.  **Resource Allocation**: Let's say `"gpu-node-1"` is currently idle. Its available resources are 16 CPU and 32768 MB. This is sufficient. The Scheduler:
      - **Finds a contiguous block of 2 CPU cores** and sufficient memory. For example, cores `[0, 1]` might be available.
      - **Locks** the requested resources on `"gpu-node-1"` and consumes any requested global licenses such as `license/foo`. The node's available resources are now tracked internally as 14 CPU and 28672 MB.
      - Assigns the submission to `"gpu-node-1"`.
      - Updates the submission's status to `Running`.
      - For multi-node batch jobs or interactive allocations, repeats this process across distinct scheduler nodes and stores both a Slurm nodelist and a full node-to-core map.
7.  **Dispatching**: The submission is dispatched to the [Judger Workflow](./judger-workflow.md) for execution on `"gpu-node-1"`. Docker nodes run containers through the Docker API and support runtime suspend/resume through Docker pause/unpause. Kubernetes nodes create a per-submission PVC and one runner Pod per workflow step, then execute commands through `kubectl exec`; CPU and memory are expressed as Pod resource requests/limits so Kubernetes performs the final node placement. Kubernetes suspend/resume and signal operations are translated into in-container process signals such as `STOP`, `CONT`, `TERM`, or `USR1`. For multi-node batch jobs and multi-node interactive allocations, CSOJ reserves resources on every allocated scheduler node while the current workflow or `srun` runtime executes on the first allocated node.
8.  **Resource Release**: Once the judging process is complete (whether it succeeds or fails), or when an interactive allocation is released, the allocated resources (2 CPU, 4096 MB) and held licenses are released, and the available resources on `"gpu-node-1"` are updated back to 16 CPU and 32768 MB. The Scheduler can now assign another task to it.

This resource-aware scheduling ensures that nodes are not overloaded and that submissions are processed efficiently as resources become available.

For Kubernetes-backed nodes, `network: false` in a workflow step is exposed as a Pod annotation (`csoj.zjusc.org/network: "false"`). Enforce actual network isolation with namespace-level NetworkPolicies or admission controls in the target cluster.
