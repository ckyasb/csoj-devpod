# User API Reference

The User API is the primary way for regular users to interact with the CSOJ system. All User API routes are prefixed with `/api/v1`.

## Authentication

- **JWT**: Most authenticated endpoints are secured using an `Authorization: Bearer <token>` HTTP header.
- **Obtaining a Token**: Users obtain a JWT through one of the login endpoints.

---

### Auth

#### `GET /auth/status`
- **Description**: Gets the status of available authentication methods.
- **Authentication**: None
- **Success Response** (`200 OK`):
  ```json
  {
    "code": 0,
    "data": {
      "local_auth_enabled": true
    },
    "message": "Auth status retrieved"
  }
  ```

#### `POST /auth/local/register`

  - **Description**: Registers a new user (when local auth is enabled).

  - **Authentication**: None

  - **Request Body** (`application/json`):

    ```json
    {
      "username": "newuser",
      "password": "password123",
      "nickname": "New User"
    }
    ```

  - **Success Response** (`200 OK`):

    ```json
    {
      "code": 0,
      "data": { "id": "user-uuid", "username": "newuser" },
      "message": "User registered successfully"
    }
    ```

#### `POST /auth/local/login`

  - **Description**: Logs in a user with a username and password (when local auth is enabled).
  - **Authentication**: None
  - **Request Body** (`application/json`):
    ```json
    {
      "username": "newuser",
      "password": "password123"
    }
    ```
  - **Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": { "token": "your_jwt_token_here" },
      "message": "Login successful"
    }
    ```

#### `GET /auth/gitlab/login`

  - **Description**: Redirects the user to GitLab for OAuth2 authentication.
  - **Authentication**: None

#### `GET /auth/gitlab/callback`

  - **Description**: The callback URL for GitLab OAuth2. On success, it returns a JWT.
  - **Authentication**: None

-----

### General Info

#### `GET /links`

  - **Description**: Gets the list of dynamic navigation links configured in `config.yaml`.
  - **Authentication**: None
  - **Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": [
        { "name": "Source Code", "url": "[https://github.com/ZJUSCT/CSOJ](https://github.com/ZJUSCT/CSOJ)" },
        { "name": "About", "url": "/about" }
      ],
      "message": "Links retrieved successfully"
    }
    ```

-----

### Contests

#### `GET /contests`

  - **Description**: Gets a list of all available contests.
  - **Authentication**: None
  - **Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": {
        "contest-id-1": { "id": "...", "name": "...", "problem_ids": [...] },
        "contest-id-2": { "id": "...", "name": "...", "problem_ids": [...] }
      },
      "message": "Contests loaded"
    }
    ```

#### `GET /contests/:id`

  - **Description**: Gets detailed information for a single contest. If the contest has not started or has ended, the `problem_ids` array will be empty.
  - **Authentication**: None
  - **Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": {
        "id": "sample-contest",
        "name": "Sample Introductory Contest",
        "starttime": "...",
        "endtime": "...",
        "problem_ids": ["aplusb", "fizzbuzz"],
        "description": "Contest description...",
        "announcements": []
      },
      "message": "Contest found"
    }
    ```

#### `GET /contests/:id/announcements`

  - **Description**: Gets the list of announcements for a specific contest. Announcements are only visible after the contest has started.
  - **Authentication**: None

#### `GET /contests/:id/leaderboard`

  - **Description**: Gets the leaderboard for a contest.
  - **Authentication**: None

#### `GET /contests/:id/trend`

  - **Description**: Gets the score trend data for the top 10 users (plus ties) in a contest.
  - **Authentication**: None

#### `POST /contests/:id/register`

  - **Description**: Registers the current user for an ongoing contest.
  - **Authentication**: JWT
  - **Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": null,
      "message": "Successfully registered for contest"
    }
    ```

#### `GET /contests/:id/history`

  - **Description**: Gets the score change history for the current user in a contest.
  - **Authentication**: JWT

-----

### Problems

#### `GET /problems/:id`

  - **Description**: Gets detailed information for a single problem. Only accessible after the contest and problem have both started.
  - **Authentication**: None

#### `POST /problems/:id/submit`

  - **Description**: Submits code/files for a problem. The request must be of type `multipart/form-data`. **The user must be registered for the contest before submitting.**
  - **Authentication**: JWT
  - **Request Body** (`multipart/form-data`):
      - `files`: One or more file fields, preserving directory structure.
      - `array`: (optional) Slurm-like job array spec such as `0-9%2`. If omitted, the problem's `scheduling.array` default is used.
  - **Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": { "submission_id": "new-submission-uuid" },
      "message": "Submission received"
    }
    ```
  - **Array Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": {
        "submission_id": "first-task-submission-uuid",
        "array_job_id": "array-job-uuid",
        "submission_ids": ["task-submission-uuid"],
        "task_ids": [0],
        "array_max_running": 2
      },
      "message": "Submission received"
    }
    ```

#### `GET /problems/:id/attempts`

  - **Description**: Gets information about the current user's submission attempts for a problem.
  - **Authentication**: JWT
  - **Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": {
          "limit": 10,  // Submission limit, or null if unlimited
          "used": 2,    // Submissions used
          "remaining": 8 // Submissions remaining, or null if unlimited
      },
      "message": "Submission attempts retrieved successfully"
    }
    ```

-----

### Submissions

#### `GET /submissions`

  - **Description**: Gets all submissions for the current user. Submission records include Slurm job metadata such as `job_name`, `work_dir`, `stdin_path`, `stdout_path`, `stderr_path`, `open_mode`, `comment`, `mail_type`, `mail_user`, `exclusive`, `requeue`, `export`, `environment`, `ntasks`, `cpus_per_task`, `nodes`, `nodelist`, `exclude_nodes`, and `licenses`, plus derived `slurm_state` and `slurm_reason` fields in addition to the native CSOJ `status`.
  - **Authentication**: JWT

#### `GET /submissions/:id`

  - **Description**: Gets a specific submission for the current user, including Slurm job metadata plus derived `slurm_state` and `slurm_reason`.
  - **Authentication**: JWT

#### `POST /submissions/:id/interrupt`

  - **Description**: Interrupts a submission that is currently queued, running, or suspended. The native status becomes `Failed`, the reason becomes `Interrupted`, and the derived Slurm state becomes `CANCELLED`.
  - **Authentication**: JWT

#### `GET /submissions/:id/queue_position`

  - **Description**: Gets the queue position for a queued submission. Returns `0` if the submission is not in the queue.
  - **Authentication**: JWT

#### `GET /submissions/:id/containers/:conID/log`

  - **Description**: Gets the full log for a specific step (container) of a submission. The step must be configured with `show: true` in `problem.yaml`. The log is returned in NDJSON format.
  - **Authentication**: JWT

-----

### User Profile

#### `GET /user/profile`

  - **Description**: Gets the current user's profile.
  - **Authentication**: JWT

#### `GET /users/:id`

  - **Description**: Gets the publicly available profile information for any user by their ID.
  - **Authentication**: None

#### `PATCH /user/profile`

  - **Description**: Updates the current user's nickname and signature.
  - **Authentication**: JWT
  - **Request Body** (`application/json`):
    ```json
    {
      "nickname": "My New Nickname",
      "signature": "Hello World!"
    }
    ```

#### `POST /user/avatar`

  - **Description**: Uploads and updates the current user's avatar.
  - **Authentication**: JWT
  - **Request Body** (`multipart/form-data`):
      - `avatar`: An image file field (JPG, PNG, WEBP; max 1MB).

-----

### SSH Keys

DevPod SSH access uses standard OpenSSH public keys. Users must upload at least one key before creating a DevPod.

#### `GET /user/ssh_keys`

  - **Description**: Lists SSH public keys for the current user.
  - **Authentication**: JWT

#### `POST /user/ssh_keys`

  - **Description**: Adds an OpenSSH-format public key and syncs the devpods `User` CRD.
  - **Authentication**: JWT
  - **Request Body** (`application/json`):
    ```json
    {
      "name": "laptop",
      "publicKey": "ssh-ed25519 AAAAC3... alice@laptop"
    }
    ```

#### `DELETE /user/ssh_keys/:id`

  - **Description**: Deletes a key and updates or removes the devpods `User` CRD.
  - **Authentication**: JWT

-----

### DevPods

DevPod endpoints create and manage interactive development containers. The frontend never receives kubeconfig or Kubernetes API server credentials.

DevPod-specific errors use this shape:

```json
{
  "code": -1,
  "error": "RESOURCE_LIMIT_EXCEEDED",
  "message": "requested memory exceeds the configured limit",
  "details": {}
}
```

#### `GET /devpods/options`

  - **Description**: Returns enabled images, resource limits, defaults, gateway info, and network profiles for the create form.
  - **Authentication**: JWT

#### `POST /devpods`

  - **Description**: Creates a DevPod session record, syncs the user's SSH keys to devpods `User`, applies a `devpod.io/v1alpha1` DevPod CRD, and applies CSOJ NetworkPolicy templates.
  - **Authentication**: JWT
  - **Request Body** (`application/json`):
    ```json
    {
      "displayName": "my-dev-env",
      "image": "registry.local/csoj/mpi:latest",
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
      },
      "command": ["sleep", "infinity"]
    }
    ```
  - **Success Response** (`200 OK`):
    ```json
    {
      "code": 0,
      "data": {
        "id": "session-uuid",
        "status": "Creating",
        "sshCommand": "ssh alice+csoj-123456789abc@gateway.example.com",
        "sshHost": "gateway.example.com",
        "sshUser": "alice+csoj-123456789abc",
        "sshPort": 22,
        "expiresAt": "..."
      },
      "message": "DevPod creation requested successfully"
    }
    ```

#### `GET /devpods`

  - **Description**: Lists DevPods owned by the current user.
  - **Authentication**: JWT

#### `GET /devpods/:id`

  - **Description**: Gets one DevPod owned by the current user and refreshes status from Kubernetes when possible.
  - **Authentication**: JWT

#### `POST /devpods/:id/stop`

  - **Description**: Hibernates the DevPod by patching `spec.running=false`. Persistent PVCs are left for the devpods controller to preserve.
  - **Authentication**: JWT

#### `POST /devpods/:id/start`

  - **Description**: Wakes the DevPod by patching `spec.running=true`.
  - **Authentication**: JWT

#### `DELETE /devpods/:id`

  - **Description**: Deletes the DevPod CRD and the per-session CSOJ NetworkPolicy, then marks the session deleted.
  - **Authentication**: JWT

#### `GET /devpods/:id/ssh`

  - **Description**: Returns SSH connection info for a DevPod owned by the current user.
  - **Authentication**: JWT

#### `GET /devpods/:id/logs`

  - **Description**: Returns recent logs from the rendered workload Pod's `workspace` container when the workload is ready.
  - **Authentication**: JWT

-----

### Assets

These endpoints serve static assets. Some require authentication, while others are public.

#### `GET /assets/avatars/:filename`

  - **Description**: Gets a user avatar image.
  - **Authentication**: None

#### `GET /assets/contests/:id/*assetpath`

  - **Description**: Gets a static asset referenced in a contest's `index.md` description.
  - **Authentication**: JWT

#### `GET /assets/problems/:id/*assetpath`

  - **Description**: Gets a static asset referenced in a problem's `index.md` statement.
  - **Authentication**: JWT

-----

### WebSocket

#### `GET /ws/submissions/:subID/containers/:conID/logs?token=<jwt>`

  - **Description**: Establishes a WebSocket connection to stream the log from a judging container, if permitted by the `show: true` flag in the problem's workflow step. For finished containers, it streams the saved log file. For running containers, it streams logs in real-time.
  - **Authentication**: JWT passed via the `token` query parameter.
  - **Message Format** (JSON):
    ```json
    {
      "stream": "stdout", // "stdout", "stderr", "info", or "error"
      "data": "log content line"
    }
    ```
