# CSOJ DevPods Integration

This repository packages the CSOJ + devpods integration work.

## Layout

- `CSOJ/`: CSOJ backend with DevPod configuration, data models, APIs, Kubernetes CRD/NetworkPolicy generation, audit records, and tests.
- `CSOJ-WebUI/`: CSOJ user frontend with DevPod list/create/detail pages and SSH key management.
- `task.md`: Original task brief used for the implementation.

## Backend

The backend adds:

- `devpod:` configuration in the main config model and docs.
- `DevPodSession`, `UserSSHKey`, and `DevPodAuditRecord` GORM models.
- User APIs under `/api/v1/devpods` and `/api/v1/user/ssh_keys`.
- Admin APIs under `/api/v1/devpods`.
- CRD mode integration using `devpods.devpod.io` and `users.devpod.io`.
- Per-session NetworkPolicy templates for default-deny, gateway ingress, DNS egress, optional public egress, and MPI session traffic.
- Unit tests for resource/profile validation and generated Kubernetes manifests.

Verified from `CSOJ/`:

```bash
/tmp/go1.24.0/bin/go test ./...
/tmp/go1.24.0/bin/go build ./cmd/CSOJ
```

## Frontend

The frontend adds:

- `/devpods`: DevPod list and query-parameter detail view.
- `/devpods/new`: DevPod creation form driven by `/api/v1/devpods/options`.
- Profile SSH key management using `/api/v1/user/ssh_keys`.
- Navigation entries for DevPods.
- Offline-safe system font configuration for static export builds.

Verified from `CSOJ-WebUI/`:

```bash
corepack pnpm install --frozen-lockfile --registry=https://registry.npmmirror.com
corepack pnpm build
```

The project uses `output: "export"`, so DevPod details are exposed as `/devpods?id=<session-id>` rather than an unknown dynamic static route.
