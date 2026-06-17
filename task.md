# 开发任务：将 CSOJ 与 devpods 集成，实现基于 Kubernetes Pod 的交互式开发容器功能

你现在需要在现有 CSOJ 项目基础上实现一个“交互式开发容器 / DevPod”功能。目标是让用户在 CSOJ 前端选择容器需求，后端将这些需求转换成 Kubernetes Pod 或 DevPod 资源，并返回一个可通过 SSH 连接的容器环境。这个功能需要参考 devpods 项目的 CRD、SSH Gateway、sidecar sshd、nsenter、NetworkPolicy、多租户隔离设计，同时结合 CSOJ 已有的用户系统、题目系统、评测配置和前端结构。

## 一、项目背景

当前已有两个参考项目：

1. CSOJ
   CSOJ 是现有在线评测系统，已有用户登录、题目、提交、评测配置、Docker-based judger workflow 等基础能力。当前需求是在 CSOJ 中增加“交互式容器环境”功能，使用户可以在网页端创建、查看、停止、删除自己的开发容器，并获取 SSH 连接方式。

2. devpods
   devpods 是 Kubernetes-native 多租户远程开发环境。它通过 DevPod CRD 表达一个用户的开发环境，通过 controller 创建实际 Pod/PVC/Service/NetworkPolicy，并通过中央 SSH Gateway 暴露统一连接入口。Pod 模式下使用 sshd sidecar 与 nsenter，让 SSH 进入用户容器的 mount/pid/net namespace，而不是进入 sidecar 自身。

本任务不是重写 CSOJ，也不是完整复刻 devpods，而是把 devpods 的核心模型集成到 CSOJ 中。

## 二、总体目标

实现一个 CSOJ DevPod 功能模块：

用户在 CSOJ 前端选择：

* 镜像 image
* CPU 核数
* 内存大小
* GPU 数量，可选
* 运行时长
* 是否持久化工作目录
* 持久化存储大小
* 是否允许外网
* 是否启用 MPI 模式
* MPI 网络模式
* 是否需要 hostNetwork，只允许管理员或特权 profile 使用
* 环境变量
* 启动命令，可选
* 挂载的数据集或题目目录，可选

前端提交后：

* CSOJ 后端校验用户身份和资源权限
* 后端创建 DevPod 记录
* 后端调用 Kubernetes API 创建 DevPod CRD，或直接创建 Pod/PVC/Service/NetworkPolicy
* controller 或后端等待 Pod Ready
* 系统返回 SSH 连接信息
* 前端展示连接命令，例如：

```bash
ssh <username>+<podName>@<gatewayHost>
```

或者返回 Web Terminal 所需信息：

```json
{
  "status": "Running",
  "sshCommand": "ssh alice+devpod-xxx@gateway.example.com",
  "gatewayHost": "gateway.example.com",
  "sshUser": "alice+devpod-xxx",
  "expiresAt": "...",
  "podName": "...",
  "namespace": "..."
}
```

## 三、推荐架构

实现时优先采用下面的架构：

```text
CSOJ WebUI
  └── DevPod 页面
        ├── 创建开发容器
        ├── 查看我的容器
        ├── 查看连接命令
        ├── 打开 Web Terminal，可选
        ├── 停止 / 唤醒 / 删除容器
        └── 查看资源使用情况

CSOJ Backend
  └── DevPod Service
        ├── 用户身份校验
        ├── 资源请求校验
        ├── DevPod 配额检查
        ├── 生成 Kubernetes 资源
        ├── 查询 Pod 状态
        ├── 返回 SSH 连接信息
        └── 审计日志

Kubernetes Cluster
  ├── devpod-controller，可选
  ├── devpod-gateway
  ├── user DevPod / Pod
  ├── PVC
  ├── Service
  ├── NetworkPolicy
  ├── ResourceQuota
  └── LimitRange
```

实现方式允许两种：

### 方案 A：优先推荐，集成 devpods CRD

CSOJ 后端不直接创建裸 Pod，而是创建 DevPod CRD：

```yaml
apiVersion: devpod.io/v1alpha1
kind: DevPod
metadata:
  name: csoj-<user-id>-<session-id>
  namespace: devpods
  labels:
    app: csoj-devpod
    csoj.zjusct.io/user-id: "<user-id>"
    csoj.zjusct.io/session-id: "<session-id>"
spec:
  owner: "<username>"
  running: true
  idleTimeoutSeconds: 3600
  persistence:
    size: 20Gi
    storageClassName: "<storage-class>"
  pod:
    metadata:
      labels:
        app: csoj-devpod
    spec:
      containers:
        - name: workspace
          image: "<user-selected-image>"
          command: ["sleep", "infinity"]
          resources:
            requests:
              cpu: "<cpu>"
              memory: "<memory>"
            limits:
              cpu: "<cpu-limit>"
              memory: "<memory-limit>"
```

优点：

* 更接近 devpods 的设计
* SSH Gateway、sidecar、nsenter、status.endpoint 可以复用
* 后续支持 hibernate、PVC、Web Terminal 更自然

### 方案 B：短期可落地，CSOJ 后端直接创建 Pod

CSOJ 后端直接创建：

* Pod
* PVC
* Service
* NetworkPolicy
* Secret，可选
* SSH sidecar，可选

这种方案实现更快，但后续维护成本更高。

除非当前 devpods controller 尚未稳定，否则优先采用方案 A。

## 四、后端任务

### 1. 新增数据模型

在 CSOJ 后端新增 DevPod 相关模型。

建议表结构：

```go
type DevPodSession struct {
    ID              string
    UserID          string
    Username        string
    Name            string
    DisplayName     string
    Image           string
    CPU             int
    MemoryMB        int
    GPU             int
    StorageGB       int
    Persistent      bool
    NetworkMode     string
    MPIEnabled      bool
    HostNetwork     bool
    Status          string
    Namespace       string
    K8sResourceName string
    SSHUser         string
    SSHHost         string
    SSHPort         int
    SSHCommand      string
    CreatedAt       time.Time
    UpdatedAt       time.Time
    ExpiresAt       time.Time
    LastActivityAt  *time.Time
}
```

状态至少包含：

```text
Pending
Creating
Running
Stopped
Failed
Deleting
Deleted
Expired
```

### 2. 新增配置项

在 CSOJ 主配置中增加 devpod 配置段：

```yaml
devpod:
  enabled: true
  mode: "crd" # crd 或 direct-pod
  namespace: "devpods"
  gateway:
    host: "gateway.example.com"
    port: 22
  defaults:
    image: "ubuntu:24.04"
    cpu: 1
    memory_mb: 2048
    storage_gb: 10
    idle_timeout_seconds: 3600
    max_lifetime_seconds: 86400
  limits:
    max_pods_per_user: 3
    max_cpu_per_pod: 8
    max_memory_mb_per_pod: 32768
    max_gpu_per_pod: 1
    max_storage_gb_per_pod: 100
  images:
    - name: "Ubuntu 24.04"
      image: "ubuntu:24.04"
      allowed: true
    - name: "HPC C++"
      image: "registry.local/csoj/hpc-cpp:latest"
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
```

### 3. 新增 API

实现以下用户 API：

```http
GET    /api/v1/devpods/options
POST   /api/v1/devpods
GET    /api/v1/devpods
GET    /api/v1/devpods/:id
POST   /api/v1/devpods/:id/stop
POST   /api/v1/devpods/:id/start
DELETE /api/v1/devpods/:id
GET    /api/v1/devpods/:id/ssh
GET    /api/v1/devpods/:id/logs
```

其中：

`GET /api/v1/devpods/options` 返回前端表单需要的镜像、资源限制、网络 profile、MPI profile。

`POST /api/v1/devpods` 接收创建请求：

```json
{
  "displayName": "my-dev-env",
  "image": "registry.local/csoj/hpc-cpp:latest",
  "cpu": 2,
  "memoryMB": 4096,
  "gpu": 0,
  "persistent": true,
  "storageGB": 20,
  "idleTimeoutSeconds": 3600,
  "networkProfile": "default",
  "mpiEnabled": false,
  "env": {
    "OMP_NUM_THREADS": "2"
  }
}
```

返回：

```json
{
  "id": "...",
  "status": "Pending",
  "sshCommand": "ssh alice+csoj-xxx@gateway.example.com",
  "sshHost": "gateway.example.com",
  "sshUser": "alice+csoj-xxx",
  "sshPort": 22
}
```

### 4. 资源校验

后端必须做资源校验，不能直接信任前端参数。

需要校验：

* 用户是否登录
* 用户是否被允许创建 DevPod
* 用户当前运行中的 DevPod 数是否超限
* CPU 是否超过配置上限
* memory 是否超过配置上限
* GPU 是否超过配置上限
* storage 是否超过配置上限
* image 是否在允许列表中
* networkProfile 是否允许当前用户使用
* hostNetwork 是否只有管理员或特权 profile 可以使用
* MPI profile 是否只允许特定镜像或特定用户组使用
* 不允许用户传入 arbitrary privileged/securityContext/hostPath

### 5. Kubernetes 资源生成

如果使用 DevPod CRD，后端只创建 DevPod 对象，不直接创建 Pod。

如果 direct-pod 模式，则必须创建：

* Pod
* PVC，可选
* Service
* NetworkPolicy
* Secret，可选

Pod 默认安全上下文：

```yaml
securityContext:
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  capabilities:
    drop:
      - ALL
```

默认禁止：

```yaml
privileged: true
hostPID: true
hostIPC: true
hostNetwork: true
hostPath volume
SYS_ADMIN capability
NET_ADMIN capability
```

只有当网络 profile 明确允许 MPI host network 时，才可以启用：

```yaml
hostNetwork: true
dnsPolicy: ClusterFirstWithHostNet
```

但这必须满足：

* 当前用户是管理员，或者属于允许的 MPI 用户组
* 当前题目或实验要求 MPI host network
* Pod 调度到专用 MPI 节点池
* 节点有明确 label，例如：

```yaml
nodeSelector:
  csoj.zjusct.io/network-profile: mpi-hostnet
```

不允许普通容器和 MPI hostNetwork 容器混跑在同一批节点上。

### 6. SSH 接入

优先复用 devpods 的 SSH Gateway 模式。

连接格式：

```bash
ssh <username>+<devpod-name>@<gateway-host>
```

后端返回：

```json
{
  "sshCommand": "ssh alice+csoj-12345@gateway.example.com",
  "sshHost": "gateway.example.com",
  "sshPort": 22,
  "sshUser": "alice+csoj-12345"
}
```

不要把 Kubernetes kubeconfig 直接返回给前端。

不要让前端直接访问 Kubernetes API Server。

如果需要 Web Terminal，前端应连接 CSOJ Backend 的 WebSocket，由后端代理到 SSH Gateway 或 Kubernetes exec。优先用 SSH Gateway，不要让浏览器直接拿到集群权限。

### 7. 用户 SSH Key 管理

需要实现至少一种用户 SSH Key 来源：

方案 A：

* 用户在 CSOJ 个人设置中上传 SSH public key
* CSOJ 后端同步创建或更新 devpods 的 User CRD
* Gateway 根据 User CRD 完成公钥认证

方案 B：

* CSOJ 后端作为 trusted upstream proxy
* 用户通过 CSOJ 登录
* 前端 Web Terminal 连接 CSOJ 后端
* 后端代表用户连接 gateway
* gateway 信任 CSOJ proxy key

短期建议先做方案 A，因为标准 SSH 客户端可直接使用。后续再做 Web Terminal proxy。

### 8. 网络隔离

默认网络策略：

* 禁止不同用户 DevPod 互访
* 只允许 gateway 访问 DevPod 的 22 端口
* 默认禁止公网访问
* 默认允许 DNS
* 如果用户选择 internet profile，才允许 egress 到公网
* 如果用户选择 MPI profile，允许同一 MPI job / 同一用户 / 同一 session 内 Pod 互联

示例策略：

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: csoj-devpod-default-deny
  namespace: devpods
spec:
  podSelector:
    matchLabels:
      app: csoj-devpod
  policyTypes:
    - Ingress
    - Egress
```

按 owner 生成 allow policy：

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: csoj-devpod-allow-owner-<user-id>
  namespace: devpods
spec:
  podSelector:
    matchLabels:
      csoj.zjusct.io/user-id: "<user-id>"
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: devpod-gateway
      ports:
        - protocol: TCP
          port: 22
    - from:
        - podSelector:
            matchLabels:
              csoj.zjusct.io/user-id: "<user-id>"
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
```

MPI hostNetwork 模式下不能只依赖 NetworkPolicy，需要依靠：

* 独立节点池
* nodeSelector / taints / tolerations
* admission webhook 校验
* RBAC 限制
* 审计日志
* 用户组白名单

### 9. MPI 支持

MPI 模式分两种：

#### 普通 MPI 模式

适合教学、演示、小规模 MPI：

* 不启用 hostNetwork
* 使用 headless service 提供 Pod DNS
* 多 Pod MPI 后续可以扩展为 JobSet / MPIJob
* 当前第一阶段只需要支持单 Pod 内多进程 MPI

#### HPC/MPI hostNetwork 模式

适合需要接近宿主机网络性能的场景：

* 需要显式选择 `mpi-hostnet` profile
* 只允许管理员或特定用户组
* 只允许调度到 MPI 专用节点
* 必须记录审计日志
* 必须拒绝任意用户自定义 hostNetwork
* 必须禁止 hostPID、hostIPC、hostPath、privileged

当前任务第一阶段只实现单 Pod MPI profile。多节点 MPI 作为后续扩展，不要在本次任务里过度设计。

### 10. 前端任务

在 CSOJ-WebUI 中新增 DevPod 页面。

页面路径建议：

```text
/devpods
/devpods/new
/devpods/:id
```

页面功能：

1. 我的开发容器列表

展示：

* 名称
* 镜像
* CPU
* 内存
* GPU
* 状态
* 创建时间
* 过期时间
* SSH 连接命令
* 操作按钮

2. 创建开发容器页面

表单字段：

* 容器名称
* 镜像选择
* CPU
* 内存
* GPU，可选
* 持久化开关
* 存储大小
* 网络 profile
* MPI profile
* 运行时长
* 环境变量，高级选项
* 启动命令，高级选项

3. 容器详情页

展示：

* 状态
* SSH 命令
* 一键复制
* 资源信息
* 网络模式
* 持久化信息
* 最近活动时间
* 日志
* 停止 / 唤醒 / 删除

4. 可选：Web Terminal

如果实现 Web Terminal：

* 使用 xterm.js
* 前端连接 CSOJ 后端 WebSocket
* 后端代理到 SSH Gateway 或 Kubernetes exec
* 前端不持有 kubeconfig
* 前端不直接访问 K8s API

### 11. 权限模型

需要设计角色：

```text
normal user:
  - 创建普通 DevPod
  - 查看、停止、删除自己的 DevPod
  - 使用默认网络 profile
  - 使用 internet profile，取决于配置
  - 不允许 hostNetwork
  - 不允许 privileged
  - 不允许 hostPath

privileged user:
  - 可以使用 MPI profile
  - 可以申请 GPU
  - 可以使用更高资源上限

admin:
  - 查看所有 DevPod
  - 删除任意 DevPod
  - 使用 hostNetwork profile
  - 修改全局配置
```

后端必须做权限校验。

Kubernetes RBAC 只授予 CSOJ 后端 ServiceAccount 最小权限：

* create/get/list/watch/delete DevPod CRD
* get/list/watch Pod
* get/list/watch PVC
* get/list/watch Service
* get/list/watch NetworkPolicy
* patch status，仅在需要时
* 不授予 cluster-admin
* 不授予 secrets 全量读取，除非明确需要

如果使用 devpods gateway，则 gateway 权限也必须最小化：

* 只读 User
* 只读 DevPod
* patch DevPod status
* 读取指定 key Secret
* 不允许写 DevPod spec
* 不允许创建 Pod

### 12. 审计日志

每次操作都要记录：

```text
user_id
username
action
devpod_id
resource_name
image
cpu
memory
gpu
network_profile
host_network
mpi_enabled
source_ip
result
error_message
created_at
```

SSH session 需要记录：

```text
user
devpod
auth_path
client_ip
open_time
close_time
duration
result
```

### 13. 错误处理

需要覆盖：

* 镜像不存在
* 资源超限
* 用户没有权限
* Pod 创建失败
* PVC 创建失败
* Gateway 未就绪
* SSH endpoint 未生成
* Pod Pending 太久
* ImagePullBackOff
* CrashLoopBackOff
* hostNetwork profile 被拒绝
* MPI profile 节点不足
* 用户删除正在运行的 Pod

API 错误返回统一格式：

```json
{
  "error": "RESOURCE_LIMIT_EXCEEDED",
  "message": "requested memory exceeds your quota",
  "details": {}
}
```

### 14. 验收标准

实现完成后必须满足：

1. 普通用户可以在 CSOJ 前端创建一个普通开发容器。
2. 创建成功后，前端能显示 SSH 命令。
3. 用户可以使用 SSH 命令进入容器。
4. SSH 进入后实际落在用户容器环境，而不是 ssh sidecar 环境。
5. 用户只能看到和管理自己的 DevPod。
6. 普通用户无法创建 hostNetwork、privileged、hostPath 容器。
7. 用户资源请求超过限制时，后端拒绝创建。
8. 不同用户的 DevPod 默认网络隔离。
9. 默认容器不能访问公网，除非选择允许公网的 profile。
10. 管理员可以查看和删除所有 DevPod。
11. DevPod 删除后，相关 Pod/Service/NetworkPolicy 能被清理。
12. 如果启用持久化，停止/唤醒后数据仍保留。
13. MPI profile 至少能创建带有 MPI 环境的容器。
14. hostNetwork MPI profile 必须有权限校验，普通用户不能使用。
15. 所有创建、删除、SSH 连接操作都有审计日志。
16. 前端不会收到 kubeconfig 或 Kubernetes API Server 凭证。
17. 后端 ServiceAccount 不使用 cluster-admin。

### 15. 不要做的事情

本次任务不要做：

* 不要重写 CSOJ 的评测系统
* 不要把 OJ submission 和 DevPod 混成同一个模型
* 不要把 kubeconfig 返回给前端
* 不要让普通用户任意指定 PodSpec
* 不要允许普通用户指定 hostPath
* 不要允许普通用户指定 privileged
* 不要默认启用 hostNetwork
* 不要默认允许公网访问
* 不要在用户容器里直接写入所有用户的 SSH key
* 不要把 SSH Gateway 私钥暴露给业务容器
* 不要使用 cluster-admin 权限图省事

### 16. 实现顺序

请按以下顺序开发：

#### M1：后端模型与配置

* 增加 DevPod 配置段
* 增加 DevPodSession 数据模型
* 增加基本 CRUD API
* 增加资源校验逻辑
* 单元测试覆盖参数校验和权限校验

#### M2：Kubernetes 资源创建

* 实现 DevPod CRD 创建，或者 direct Pod 创建
* 实现状态同步
* 实现删除清理
* 实现 SSH 连接信息返回
* 增加集成测试

#### M3：前端页面

* 增加 DevPod 列表页
* 增加创建页
* 增加详情页
* 增加复制 SSH 命令
* 增加停止/启动/删除按钮

#### M4：权限与网络隔离

* 实现普通 profile
* 实现 internet profile
* 实现 MPI profile
* 实现 hostNetwork profile 的管理员校验
* 实现 NetworkPolicy
* 实现审计日志

#### M5：Web Terminal，可选

* 使用 xterm.js
* 前端通过 WebSocket 连接 CSOJ 后端
* 后端代理到 SSH Gateway 或 K8s exec
* 不暴露 kubeconfig

### 17. 代码质量要求

* 保持 CSOJ 现有代码风格
* 所有新增 API 必须有清晰错误处理
* 所有权限判断必须在后端完成
* 所有 Kubernetes 资源名称必须可追踪，例如包含 userID/sessionID
* 所有资源必须带 labels
* 所有资源必须支持清理
* 不允许硬编码 gateway host、namespace、镜像列表、资源上限
* 配置项要有默认值和文档
* 关键逻辑需要单元测试
* Kubernetes 资源生成逻辑需要独立函数，便于测试
* 前端组件要避免把业务逻辑散落在页面中，API 调用封装到独立模块

### 18. 最终交付

最终需要提交：

1. 后端 DevPod API
2. 前端 DevPod 页面
3. Kubernetes 资源生成逻辑
4. 权限校验逻辑
5. MPI / hostNetwork profile 校验逻辑
6. NetworkPolicy 模板
7. 配置文档
8. 使用文档
9. 基础测试
10. 示例配置文件

请在实现前先阅读 CSOJ 的项目结构、配置加载方式、路由注册方式、用户鉴权方式和现有评测任务调度方式，再开始编码。

