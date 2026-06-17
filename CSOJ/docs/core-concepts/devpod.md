# DevPod Integration

CSOJ DevPods provide interactive development containers through the existing devpods controller and SSH gateway.

## Architecture

CSOJ stores a `DevPodSession` record for each user request, validates the requested image/resources/network profile, syncs the user's SSH public keys into the cluster-scoped devpods `User` CRD, then creates a namespaced `devpod.io/v1alpha1` `DevPod` object.

The devpods controller remains responsible for rendering the workload Pod, Service, PVC, host key Secret, supervisor bootstrap, and SSH endpoint. CSOJ does not return kubeconfig or Kubernetes API credentials to the frontend.

## SSH Flow

Users upload public keys through `POST /api/v1/user/ssh_keys`. CSOJ writes those keys into the devpods `User` CRD. After a DevPod is created, CSOJ returns:

```bash
ssh <owner>+<devpod-name>@<gateway-host>
```

The gateway authenticates the public key and dials the endpoint from DevPod status. The rendered workload uses the devpods supervisor, so SSH lands in the user workspace container.

## Network Profiles

CSOJ applies a namespace default-deny policy for pods labeled `app=csoj-devpod` and a per-session allow policy.

- `default`: allows gateway ingress and DNS egress only.
- `internet`: allows gateway ingress, DNS egress, and public IPv4 egress excluding RFC1918 ranges.
- `mpi-hostnet`: enables `hostNetwork`; it requires a privileged user tag and a dedicated node selector.

NetworkPolicy does not protect hostNetwork pods. HostNetwork profiles must be isolated with dedicated node pools, taints/tolerations, admission policy, and audit review.

## Permission Model

Regular users can create, view, stop, start, delete, and fetch SSH info for their own DevPods. They cannot request arbitrary PodSpecs, hostPath volumes, privileged containers, hostPID, hostIPC, or hostNetwork.

Privileged users are identified by configured `User.tags`, such as `admin`, `devpod-admin`, or `mpi`. They can use admin-only profiles and GPU resources when limits allow.

The Admin API can list and delete all DevPods, but CSOJ's Admin API is intentionally network-protected rather than JWT-protected in this codebase.

## Frontend Contract

The user frontend should add pages for:

- `/devpods`: list current user's DevPods.
- `/devpods/new`: load `GET /api/v1/devpods/options`, submit `POST /api/v1/devpods`.
- `/devpods/:id`: show status, SSH command, resource request, logs, and stop/start/delete actions.

The frontend should also expose SSH key management in the user profile using `/api/v1/user/ssh_keys`.
