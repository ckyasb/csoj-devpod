export type Status = "Queued" | "Running" | "Success" | "Failed";
export type Level = "platinum" | "gold" | "bronze" | "silver" | "hard" | "medium" | "easy" | "";

export interface User {
  id: string;
  username: string;
  nickname: string;
  signature: string;
  avatar_url: string;
  tags: string;
}

export interface Announcement {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
  description: string;
}

export interface Contest {
  id: string;
  name: string;
  starttime: string;
  endtime: string;
  problem_ids: string[];
  description: string;
  announcements?: Announcement[];
}

export interface WorkflowStep {
  name: string;
  show: boolean;
}

export interface ScoreConfig {
  max_performance_score: number;
  mode: string;
}

export interface Problem {
    id: string;
    name: string;
    starttime: string;
    endtime: string;
    level: Level;
    cluster: string;
    cpu: number;
    memory: number;
    upload: {
        max_num: number;
        max_size: number;
        upload_form?: boolean;
        upload_files?: string[];
        editor?: boolean;
        editor_files?: string[];
    };
    score: {
      max_performance_score: number;
      mode: string;
    }
    workflow: WorkflowStep[];
    description: string;
}

export interface Container {
  id: string;
  submission_id: string;
  image: string;
  status: Status;
  exit_code: number;
  started_at: string;
  finished_at: string;
  log_file_path: string;
}

export interface ProblemForSubmission {
  id: string;
  name: string;
  workflow: WorkflowStep[];
  score: ScoreConfig;
}

export interface Submission {
  id: string;
  CreatedAt: string;
  UpdatedAt: string;
  problem_id: string;
  user_id: string;
  user: User;
  status: Status;
  current_step: number;
  cluster: string;
  node: string;
  score: number;
  performance: number;
  info: { [key: string]: any };
  is_valid: boolean;
  problem?: ProblemForSubmission;
  containers: Container[];
}

export interface LeaderboardEntry {
  user_id: string;
  username: string;
  nickname: string;
  avatar_url: string;
  tags: string;
  disable_rank: boolean;
  total_score: number;
  problem_scores: Record<string, number>;
}

export interface ScoreHistoryPoint {
  time: string;
  score: number;
  problem_id: string;
}

export interface TrendEntry {
  user_id: string;
  username: string;
  nickname: string;
  history: ScoreHistoryPoint[];
}

export interface Attempts {
    limit: number | null;
    used: number;
    remaining: number | null;
}

export interface AuthStatus {
  local_auth_enabled: boolean;
}

export interface LinkItem {
    name: string;
    url: string;
}

export type DevPodStatus =
  | "Pending"
  | "Creating"
  | "Running"
  | "Stopped"
  | "Failed"
  | "Deleting"
  | "Deleted"
  | "Expired";

export interface DevPodImageOption {
  name: string;
  image: string;
  allowed: boolean;
  profiles?: string[];
  allowed_tags?: string[];
}

export interface DevPodNetworkProfile {
  name: string;
  allow_internet: boolean;
  host_network: boolean;
  admin_only: boolean;
  mpi: boolean;
}

export interface DevPodOptions {
  enabled: boolean;
  mode: string;
  gateway: {
    host: string;
    port: number;
    backend_port: number;
    namespace: string;
  };
  defaults: {
    image: string;
    cpu: number;
    memory_mb: number;
    gpu: number;
    storage_gb: number;
    persistent: boolean;
    network_profile: string;
    idle_timeout_seconds: number;
    max_lifetime_seconds: number;
    shell: string;
    mount_path: string;
    command: string[];
  };
  limits: {
    max_pods_per_user: number;
    max_cpu_per_pod: number;
    max_memory_mb_per_pod: number;
    max_gpu_per_pod: number;
    max_storage_gb_per_pod: number;
    max_env_vars_per_pod: number;
    max_command_args: number;
  };
  images: DevPodImageOption[];
  network_profiles: DevPodNetworkProfile[];
  ssh_key_required: boolean;
}

export interface DevPodSession {
  id: string;
  name: string;
  displayName: string;
  image: string;
  cpu: number;
  memoryMB: number;
  gpu: number;
  storageGB: number;
  persistent: boolean;
  networkProfile: string;
  mpiEnabled: boolean;
  hostNetwork: boolean;
  status: DevPodStatus;
  namespace: string;
  podName: string;
  k8sResourceName: string;
  sshCommand: string;
  sshHost: string;
  sshPort: number;
  sshUser: string;
  expiresAt: string;
  lastActivityAt?: string | null;
  createdAt: string;
  updatedAt: string;
  idleTimeoutSeconds: number;
  lastError?: string;
}

export interface UserSSHKey {
  id: string;
  user_id: string;
  name: string;
  public_key: string;
  fingerprint: string;
  CreatedAt: string;
  UpdatedAt: string;
}
