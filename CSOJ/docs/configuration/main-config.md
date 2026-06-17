# Main Config (config.yaml)

`config.yaml` is the primary configuration file for the CSOJ system. It defines the core behavior of the service, including listen addresses, storage paths, authentication methods, and judger clusters.

## Full Configuration Example

```yaml
# Listen address for the user-facing API
listen: ":8080"

# Configuration for the Admin API service
admin:
  enabled: true
  listen: ":8081"

# Logger configuration
logger:
  level: "debug" # Options: "debug", "production"
  file: ""       # Path to log file. If empty, logs to console.

# Storage path configuration
storage:
  user_avatar: "data/avatars"        # User avatars
  submission_content: "data/submissions" # User-submitted files
  database: "data/csoj.db"           # SQLite database file
  submission_log: "data/logs"        # Logs from judging containers

# Authentication configuration
auth:
  jwt:
    secret: "a_very_secret_key_change_me" # JWT signing secret, MUST be changed
    expire_hours: 72                     # JWT expiration time in hours
  
  # Local username/password authentication
  local:
    enabled: true
  
  # GitLab OAuth2 authentication
  gitlab:
    url: "[https://gitlab.com](https://gitlab.com)"
    client_id: "YOUR_GITLAB_CLIENT_ID"
    client_secret: "YOUR_GITLAB_CLIENT_SECRET"
    redirect_uri: "http://localhost:8080/api/v1/auth/gitlab/callback"
    frontend_callback_url: "http://localhost:3000/callback" # URL for frontend to handle the final redirect with the token

# Cross-Origin Resource Sharing (CORS) configuration
cors:
  allowed_origins:
    - "http://localhost:3000"
    - "[http://127.0.0.1:3000](http://127.0.0.1:3000)"

# Dynamic links for the frontend navigation bar
links:
  - name: "Project Source"
    url: "[https://github.com/ZJUSCT/CSOJ](https://github.com/ZJUSCT/CSOJ)"
  - name: "About"
    url: "/about"

# Interactive DevPod environments backed by devpod.io CRDs
devpod:
  enabled: true
  mode: "crd"
  kubectl: "kubectl"
  kubeconfig: "/etc/csoj/kubeconfig"
  context: "production"
  namespace: "devpods"
  service_account: "default"
  storage_class_name: "fast"
  gateway:
    host: "gateway.example.com"
    port: 22
    backend_port: 2222
    namespace: "devpod-system"
  defaults:
    image: "ubuntu:24.04"
    cpu: 1
    memory_mb: 2048
    storage_gb: 10
    idle_timeout_seconds: 3600
    max_lifetime_seconds: 86400
    network_profile: "default"
    shell: "bash"
    mount_path: "/home/devpod"
    command: ["sleep", "infinity"]
  limits:
    max_pods_per_user: 3
    max_cpu_per_pod: 8
    max_memory_mb_per_pod: 32768
    max_gpu_per_pod: 1
    max_storage_gb_per_pod: 100
    max_env_vars_per_pod: 64
    max_command_args: 32
  privileged_user_tags: ["admin", "devpod-admin", "mpi"]
  images:
    - name: "Ubuntu 24.04"
      image: "ubuntu:24.04"
      allowed: true
    - name: "MPI"
      image: "registry.local/csoj/mpi:latest"
      allowed: true
      profiles: ["mpi"]
  network_profiles:
    - name: "default"
      allow_internet: false
      host_network: false
    - name: "internet"
      allow_internet: true
      host_network: false
    - name: "mpi-hostnet"
      allow_internet: false
      host_network: true
      admin_only: true
      mpi: true
      node_selector:
        csoj.zjusct.io/network-profile: "mpi-hostnet"
  host_network_node_selector:
    csoj.zjusct.io/network-profile: "mpi-hostnet"

# Judger cluster configuration
cluster:
  - name: "default-cluster" # Cluster name, referenced in problem configs
    state: "up"             # up, drain, down, inactive
    priority_tier: 1        # Higher tiers receive more scheduling priority
    max_time: 300           # Optional max job wall time in seconds
    allow_qos: ["normal", "urgent"]
    node:
      - name: "node-1"
        cpu: 4           # Total CPU cores available for judging
        memory: 4096       # Total memory (in MB) available for judging
        state: "idle"       # idle, drain, down
        reason: ""          # Optional drain/down reason shown in Slurm-compatible APIs
        features: ["avx2"]
        gres: []
        weight: 1
        runtime: "docker"
        docker: # Docker Daemon connection settings for this node
          host: "tcp://192.168.1.101:2375"
          tls_verify: false
          # ca_cert: "/path/to/ca.pem"
          # cert: "/path/to/cert.pem"
          # key: "/path/to/key.pem"
      - name: "node-2"
        cpu: 8
        memory: 8192
        runtime: "kubernetes"
        kubernetes:
          namespace: "csoj-judge"
          kubeconfig: "/etc/csoj/kubeconfig"
          context: "production"
          storage_class_name: "fast"
          workdir_size: "2Gi"
          startup_timeout_seconds: 120
          node_selector:
            pool: "judge"
          priority_class_name: "judge-high"

# Slurm-like scheduling controls
scheduler:
  queue_size: 1024
  backfill: true
  priority_weights:
    partition: 10000
    qos: 1000
    age: 1
    nice: 1
    job_size: 1
    fairshare: 10
  billing_weights:
    cpu: 1
    mem: 0.001
    gpu: 10
    license/foo: 2
  licenses:
    license/foo: 4
    license/bar: 1
  fairshare_decay:
    enabled: true
    half_life_hours: 24
    usage_weight: 10
  qos:
    - name: "normal"
      priority: 1
      max_jobs_per_user: 4
      max_billing_per_job: 32
    - name: "urgent"
      priority: 20
      max_jobs_per_user: 1
      preempt: ["normal"]
  accounts:
    - name: "course-a"
      users: ["alice", "bob"]
      allow_qos: ["normal", "urgent"]
      max_jobs: 8
      max_submit: 32
      max_billing_running: 128
      max_billing_submit: 512
      fairshare: 100
  reservations:
    - name: "gpu-maintenance"
      cluster: "default-cluster"
      nodes: ["node-2"]
      starttime: "2025-10-01T09:00:00+08:00"
      endtime: "2025-10-01T10:00:00+08:00"

# Path to the root directory containing all contest folders
contests_root: "contests"
```

-----

## Field Reference

### `listen`

  - **Type**: `string`
  - **Required**: Yes
  - **Description**: The listen address and port for the user-facing API service.

-----

### `admin`

  - **Type**: `object`
  - **Required**: No
  - **Description**: Configuration for the Admin API service.
      - `enabled`: (boolean) Whether to enable the Admin API service.
      - `listen`: (string) The listen address and port for the Admin API service.

-----

### `logger`

  - **Type**: `object`
  - **Required**: Yes
  - **Description**: Configuration for the logging system.
      - `level`: (string) Log level. `debug` provides more verbose output, while `production` is more concise.
      - `file`: (string) Path to a log file. If left empty, logs are written to standard output/error (the console).

-----

### `storage`

  - **Type**: `object`
  - **Required**: Yes
  - **Description**: Defines storage paths for various system files.
      - `user_avatar`: (string) Directory to store user-uploaded avatars.
      - `submission_content`: (string) Directory to store user-submitted code/files.
      - `database`: (string) Path to the SQLite database file.
      - `submission_log`: (string) Directory to store log files generated by each judging container.

-----

### `auth`

  - **Type**: `object`
  - **Required**: Yes
  - **Description**: User authentication settings.
      - `jwt`: (object)
          - `secret`: (string) The secret key used to sign and verify JWTs. **You must change this to a complex random string in production.**
          - `expire_hours`: (integer) The validity period of a JWT, in hours.
      - `local`: (object)
          - `enabled`: (boolean) Whether to enable the local username and password registration/login feature.
      - `gitlab`: (object)
          - `url`: (string) The URL of your GitLab instance's OIDC provider.
          - `client_id`: (string) The Client ID obtained after creating an application in GitLab.
          - `client_secret`: (string) The Client Secret obtained after creating an application in GitLab.
          - `redirect_uri`: (string) The callback URL configured in your GitLab application, which must exactly match this URI.
          - `frontend_callback_url`: (string) The URL on your frontend application where users are redirected after a successful login. The JWT will be appended as a `?token=` query parameter.

-----

### `cors`

  - **Type**: `object`
  - **Required**: No
  - **Description**: Configures Cross-Origin Resource Sharing (CORS) for the API.
      - `allowed_origins`: (array of strings) A list of origins that are allowed to access the API. You can add your frontend application's address here. Supports `*` as a wildcard.

-----

### `links`

  - **Type**: `array of objects`
  - **Required**: No
  - **Description**: Defines a list of dynamic links to be displayed in the frontend's main navigation bar.
      - `name`: (string) The text to display for the link.
      - `url`: (string) The destination URL. Can be an internal path (e.g., `/about`) or an external URL (e.g., `https://github.com/ZJUSCT/CSOJ`).

-----

### `devpod`

  - **Type**: `object`
  - **Required**: No
  - **Description**: Enables interactive development containers backed by `devpod.io/v1alpha1` CRDs and the devpods SSH gateway.
      - `enabled`: (boolean) Enables the user-facing DevPod API.
      - `mode`: (string) Currently supports `crd`. CSOJ creates DevPod CRDs and lets the devpods controller render Pods, Services, PVCs, and host keys.
      - `kubectl`, `kubeconfig`, `context`, `namespace`: Kubernetes client target used by the CSOJ backend. These values are never returned to the frontend.
      - `gateway`: Gateway connection information returned to users. `host` and `port` are for SSH clients; `backend_port` is the in-cluster supervisor sshd port used by NetworkPolicy; `namespace` is where the gateway pods run.
      - `defaults`: Default image, resources, lifetime, shell, persistence mount path, and command when the create request omits those values.
      - `limits`: Per-pod and per-user limits enforced by the backend before any Kubernetes object is created.
      - `privileged_user_tags`: Comma-separated `User.tags` values that allow GPU, MPI hostNetwork, and admin-only network profiles.
      - `images`: Allowed image list. Images not listed with `allowed: true` are rejected. Set `profiles: ["mpi"]` for MPI-enabled images.
      - `network_profiles`: Named network profiles. `default` denies public egress, `internet` allows public egress, and `host_network` profiles require privileged tags plus a dedicated node selector.
      - `node_selector`, `host_network_node_selector`, `tolerations`, `image_pull_secrets`, `priority_class_name`, `runtime_class_name`: Optional scheduling controls copied into the DevPod workload PodSpec.

DevPod users must upload at least one SSH public key through `POST /api/v1/user/ssh_keys` before creating a DevPod. CSOJ syncs those keys into the cluster-scoped devpods `User` CRD and returns SSH commands in the form `ssh <owner>+<devpod-name>@<gateway-host>`.

-----

### `cluster`

  - **Type**: `array of objects`
  - **Required**: Yes
  - **Description**: Defines one or more judger clusters. A cluster is the CSOJ equivalent of a Slurm partition and consists of one or more judger nodes.
      - `name`: (string) A unique name for the cluster. This name is used in problem configurations to specify which cluster to use for judging.
      - `state`: (string, optional) `up`, `drain`, `down`, or `inactive`. Drained/down partitions do not accept new queued submissions.
      - `priority_tier`: (integer, optional) Partition priority tier used by the scheduler.
      - `max_time`: (integer, optional) Maximum wall time in seconds for jobs in this cluster.
      - `max_jobs`: (integer, optional) Maximum concurrent running submissions in this cluster.
      - `allow_users`, `allow_accounts`, `allow_qos`, `deny_qos`: (array of strings, optional) Association controls.
      - `node`: (array of objects) The list of judger nodes in this cluster.
          - `name`: (string) A unique name for the node.
          - `cpu`: (integer) The total number of CPU cores that the scheduler can use on this node.
          - `memory`: (integer) The total amount of memory (in MB) that the scheduler can use on this node.
          - `state`: (string, optional) `idle`, `drain`, or `down`. Drained/down nodes do not receive new work.
          - `reason`: (string, optional) Operator-visible node reason shown by Slurm-compatible `sinfo` and `scontrol show node`.
          - `features`: (array of strings, optional) Node features used by problem `scheduling.constraint`.
          - `gres`: (array of strings, optional) Generic resources such as `gpu:2`.
          - `weight`: (integer, optional) Lower-weight nodes are preferred when multiple nodes satisfy a job.
          - `runtime`: (string, optional) Execution runtime for this scheduler node. Supported values are `docker` and `kubernetes`/`k8s`. Defaults to `docker`.
          - `docker`: (object) The connection settings for the Docker Daemon on this node when `runtime` is `docker`.
              - `host`: (string) The API address, typically a TCP address like `tcp://127.0.0.1:2375`.
              - `tls_verify`: (boolean, optional) Whether to use TLS to connect to the daemon.
              - `ca_cert`, `cert`, `key`: (string, optional) Paths to TLS certificate files if `tls_verify` is true.
          - `kubernetes`: (object) Kubernetes execution settings when `runtime` is `kubernetes`. CSOJ creates one PVC per submission and one Pod per workflow step, then uses `kubectl cp`, `kubectl exec`, and Pod deletion for the step lifecycle.
              - `kubectl`: (string, optional) Path to the `kubectl` binary. Defaults to `kubectl`.
              - `kubeconfig`, `context`, `namespace`: (string, optional) Kubernetes client target. `namespace` defaults to `default`.
              - `service_account`: (string, optional) ServiceAccount used by judging Pods.
              - `image_pull_secrets`: (array of strings, optional) Image pull secret names.
              - `node_selector`: (map, optional) Pod node selector.
              - `tolerations`: (array, optional) Pod tolerations.
              - `priority_class_name`, `runtime_class_name`: (string, optional) Pod priority/runtime class.
              - `storage_class_name`: (string, optional) StorageClass for per-submission workdir PVCs. If omitted, the cluster default StorageClass is used.
              - `workdir_size`: (string, optional) PVC request size for `/mnt/work`. Defaults to `1Gi`.
              - `startup_timeout_seconds`: (integer, optional) Timeout while waiting for the runner Pod to become Ready. Defaults to `120`.
              - `runner_container_name`: (string, optional) Container name inside each judging Pod. Defaults to `runner`.
              - `runner_command`: (array of strings, optional) Long-running command used to keep the Pod alive for `kubectl exec`. Defaults to a shell sleep command.

-----

### `scheduler`

  - **Type**: `object`
  - **Required**: No
  - **Description**: Slurm-like scheduling policy.
      - `queue_size`: (integer, optional) Buffered queue size per cluster. Defaults to `1024`.
      - `backfill`: (boolean, optional) Allows runnable lower-priority jobs to start when earlier jobs are waiting. Defaults to `true`.
      - `priority_weights`: (object, optional) Weights for `partition`, `qos`, `age`, `nice`, `job_size`, and `fairshare`.
      - `billing_weights`: (object, optional) TRES billing weights. Built-in keys include `cpu`, `mem`, and GRES/TRES names such as `gpu` or `license/foo`.
      - `licenses`: (map, optional) Global consumable license pools keyed by TRES names such as `license/foo`. Jobs that request matching `scheduling.tres` hold licenses while running and wait with reason `Licenses` when the pool is exhausted.
      - `fairshare_decay`: (object, optional) Decays completed/failed/preempted/interrupted batch accounting billing usage and released interactive allocation billing usage by `half_life_hours`, then subtracts it from account fairshare priority using `usage_weight`.
      - `qos`: (array, optional) QoS definitions with priority, per-user/job limits, billing limits, and optional `preempt` lists. If a queued job's QoS can preempt running lower QoS submissions, CSOJ cleans their containers up, releases resources, and records accounting. Preempted jobs with `requeue` return to the pending queue; other preempted jobs are failed with reason `Preempted`.
      - `accounts`: (array, optional) Account/user/QoS association definitions. `max_jobs`, `max_submit`, `max_billing_running`, `max_billing_submit`, and `fairshare` participate in scheduling.
      - `reservations`: (array, optional) Time-window reservations for partitions or nodes. Positive `cpu` and `memory` values cap active resources allocated through that reservation.

-----

### `mail`

  - **Type**: `object`
  - **Required**: No
  - **Description**: SMTP settings for Slurm-compatible `mail_type`/`mail_user` notifications. Notifications are disabled unless `enabled` is true.
      - `enabled`: (boolean, optional) Enables outbound SMTP mail.
      - `host`: (string) SMTP server host.
      - `port`: (integer, optional) SMTP server port. Defaults to `25`.
      - `username`, `password`: (string, optional) SMTP credentials. If `username` is set, CSOJ uses SMTP PLAIN authentication.
      - `from`: (string, optional) Sender address. Defaults to `username`, then `csoj@localhost`.

-----

### `contests_root`

  - **Type**: `string`
  - **Required**: Yes
  - **Description**: The path to the root directory that contains all contest configuration directories. CSOJ scans this directory on startup to load contest and problem information.
