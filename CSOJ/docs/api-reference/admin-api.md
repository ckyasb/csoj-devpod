# Admin API Reference

The Admin API provides a set of powerful endpoints for system maintenance and management. By default, the Admin API service is separate from the User API and runs on a different port (which must be enabled and configured in `config.yaml`).

## Authentication

The current version of the Admin API has **no built-in authentication mechanism**. It is crucial to ensure that the Admin API's listen address is **only accessible from trusted network environments (e.g., an internal network or localhost)**, or to add an authentication layer using a reverse proxy.

---

### System Management

#### `POST /reload`

- **Description**: Hot-reloads all contest and problem configurations from disk.
  - The system rescans the directory specified in `contests_root` in `config.yaml`.
  - New or modified contests/problems will be loaded.
  - If a problem is deleted, all submission records associated with that problem will also be **permanently deleted from the database**, including any running containers associated with them.
- **Success Response** (`200 OK`):
  ```json
  {
    "code": 0,
    "data": {
      "contests_loaded": 2,
      "problems_loaded": 15,
      "submissions_deleted": 5
    },
    "message": "Reload successful"
  }
  ```

-----

### User Management

#### `GET /users`

  - **Description**: Gets a list of all users. Can be filtered by a `query` parameter that searches User ID, username, and nickname.

#### `POST /users`

  - **Description**: Manually creates a new user.
  - **Request Body** (`application/json`):
    ```json
    {
      "username": "admin_created_user",
      "password_hash": "$2a$14$....", // bcrypt hash, required for local auth users
      "nickname": "Test User"
    }
    ```

#### `GET /users/:id`

  - **Description**: Gets a single user by their ID.

#### `PATCH /users/:id`

  - **Description**: Updates a user's nickname and signature.

#### `DELETE /users/:id`

  - **Description**: Deletes a user by their ID.

#### `POST /users/:id/reset-password`

  - **Description**: Resets the password for a local-auth user.
  - **Request Body** (`application/json`): `{"password": "new_secure_password"}`

#### `POST /users/:id/register-contest`

  - **Description**: Manually registers a user for a specific contest.
  - **Request Body** (`application/json`): `{"contest_id": "contest-id-here"}`

#### `GET /users/:id/history`

  - **Description**: Gets a user's score history for a specific contest.
  - **Query Parameter**: `contest_id` (required).

#### `GET /users/:id/scores`

  - **Description**: Gets a user's best scores for all problems they have submitted to.

-----

### DevPod Management

These endpoints manage interactive DevPod sessions across all users. The Admin API has no built-in authentication, so expose it only behind a trusted network boundary or reverse proxy.

#### `GET /devpods`

  - **Description**: Lists all DevPod sessions, including owner, resource request, network profile, status, SSH command, and expiration time. The handler refreshes Kubernetes phase when possible.

#### `GET /devpods/:id`

  - **Description**: Gets one DevPod session by ID, regardless of owner.

#### `DELETE /devpods/:id`

  - **Description**: Deletes the DevPod CRD and CSOJ per-session NetworkPolicy, marks the session `Deleted`, and records an audit event.

-----

### Contest & Problem Management

#### `GET /contests`

  - **Description**: Gets a list of all loaded contests, regardless of start/end times.

#### `POST /contests`

  - **Description**: Creates a new contest by creating the necessary directory and `contest.yaml` file on disk. Requires a `reload` to be active.
  - **Request Body**: A full `Contest` JSON object.

#### `GET /contests/:id`

  - **Description**: Gets details for a specific contest, regardless of start/end times.

#### `PUT /contests/:id`

  - **Description**: Updates the `contest.yaml` file for a contest. Triggers a system `reload`.
  - **Request Body**: A full `Contest` JSON object.

#### `DELETE /contests/:id`

  - **Description**: Deletes a contest's directory and all its contents from disk. Triggers a system `reload`.

#### `POST /contests/:id/problems`

  - **Description**: Creates a new problem within a contest. Triggers a system `reload`.
  - **Request Body**: A full `Problem` JSON object.

#### `GET /problems`

  - **Description**: Gets a list of all loaded problems.

#### `GET /problems/:id`

  - **Description**: Gets the full definition of a single problem.

#### `PUT /problems/:id`

  - **Description**: Updates a `problem.yaml` file. Triggers a system `reload`.
  - **Request Body**: A full `Problem` JSON object.

#### `DELETE /problems/:id`

  - **Description**: Deletes a problem's directory from disk. Triggers a system `reload`.

-----

### Contest Assets & Announcements

#### `GET /contests/:id/assets`

  - **Description**: Lists all static assets for a contest.

#### `POST /contests/:id/assets`

  - **Description**: Uploads one or more asset files to a contest's `index.assets` directory.

#### `DELETE /contests/:id/assets`

  - **Description**: Deletes an asset (file or directory) from a contest.

#### `GET /contests/:id/announcements`

  - **Description**: Gets all announcements for a contest.

#### `POST /contests/:id/announcements`

  - **Description**: Creates a new announcement for a contest.

#### `PUT /contests/:id/announcements/:announcementId`

  - **Description**: Updates an existing announcement.

#### `DELETE /contests/:id/announcements/:announcementId`

  - **Description**: Deletes an announcement.

-----

### Problem Assets

#### `GET /problems/:id/assets`

  - **Description**: Lists all static assets for a problem.

#### `POST /problems/:id/assets`

  - **Description**: Uploads one or more asset files to a problem's `index.assets` directory.

#### `DELETE /problems/:id/assets`

  - **Description**: Deletes an asset (file or directory) from a problem.

-----

### Submission Management

#### `GET /submissions`

  - **Description**: Gets a paginated list of all submissions. Supports filtering by `problem_id`, `status`, and `user_query`. Supports pagination with `page` and `limit`. Submission records include derived `slurm_state` and `slurm_reason` fields in addition to the native CSOJ `status`.

#### `GET /submissions/:id`

  - **Description**: Gets detailed information for a single submission, including derived `slurm_state` and `slurm_reason`.

#### `GET /submissions/:id/content`

  - **Description**: Downloads the content of a submission as a zip archive.

#### `PATCH /submissions/:id`

  - **Description**: Manually updates a submission. Supported fields include `status`, `score`, `performance`, `info`, Slurm job metadata fields such as `job_name`, `work_dir`, `stdin_path`, `stdout_path`, `stderr_path`, `open_mode`, `comment`, `mail_type`, `mail_user`, `exclusive`, `requeue`, `export`, and `environment`, plus scheduling fields such as `account`, `qos`, `priority`, `nice`, `hold`, `cpus`, `ntasks`, `cpus_per_task`, `nodes`, `memory`, `begin_time`, `deadline`, `time_limit`, `dependencies`, `reservation`, `nodelist`, `exclude_nodes`, `constraint`, `gres`, `tres`, `licenses`, `billing_units`, `reason`, and job-array metadata. Score-affecting updates trigger score recalculation.

#### `DELETE /submissions/:id`

  - **Description**: Permanently deletes a submission record and its content from disk.

#### `POST /submissions/:id/rejudge`

  - **Description**: Re-judges an existing submission.
      - The system marks the original submission as invalid (`is_valid: false`).
      - It then copies the original submission's content, creates a new submission record, and adds it to the judging queue.
      - The scoring system automatically handles score changes resulting from the re-judge.

#### `POST /submissions/:id/requeue`

  - **Description**: Requeues an existing finished submission using the same submission ID. Old container records are removed, runtime fields are reset, and the submission is added back to the scheduler. Running submissions must be interrupted before requeueing.

#### `PATCH /submissions/:id/validity`

  - **Description**: Manually marks a submission as valid or invalid. This **triggers a full score recalculation** for the user on that problem.
  - **Request Body** (`application/json`): `{"is_valid": false}`

#### `POST /submissions/:id/interrupt`

  - **Description**: Forcibly interrupts a queued, running, or suspended submission, marking it as `Failed` with reason `Interrupted`; derived Slurm state is `CANCELLED`.

#### `POST /submissions/:id/hold`

  - **Description**: Holds a queued submission so the scheduler will not start it.

#### `POST /submissions/:id/release`

  - **Description**: Releases a queued held submission.

#### `GET /submissions/:id/containers/:conID/log`

  - **Description**: Gets the full log for any step (container) of any submission, regardless of the `show` flag. The log is returned in NDJSON format.

-----

### Score & Leaderboard Management

#### `POST /scores/recalculate`

  - **Description**: Triggers a score recalculation for a specific user on a specific problem.
  - **Request Body** (`application/json`): `{"user_id": "user-uuid", "problem_id": "problem-id"}`

#### `GET /contests/:id/leaderboard`

  - **Description**: Gets the leaderboard for a contest.

#### `GET /contests/:id/trend`

  - **Description**: Gets score trend data for top users. Supports a `maxnum` query parameter to control the number of users.

-----

### Accounting

#### `GET /accounting`

  - **Description**: Gets Slurm `sacct`-like accounting records for submission and container lifecycle events, including TRES and billing units.
  - **Query Parameters**: Supports `submission_id`, `submission_ids`, `user_id`, `problem_id`, `job_name`, `cluster`, `node`, `account`, `qos`, `array_job_id`, `array_task_id`, `event`, `state`, `start_time`, `end_time`, `page`, and `limit`. Exact-match selectors accept comma, semicolon, or space separated lists; `job_name` uses comma or semicolon lists so names with spaces remain valid.
  - **Events**: Includes `Submitted`, `Started`, `Completed`, `Failed`, `Preempted`, `ContainerStarted`, `ContainerFinished`, `Held`, `Released`, `Requeued`, `Interrupted`, `Suspended`, `Resumed`, `Signaled`, `Allocated`, `AllocationReleased`, `RunStarted`, `RunCompleted`, and `RunFailed`.

-----

### Slurm-Compatible Command API

These endpoints expose Slurm command-shaped JSON views over the same scheduler, accounting, node, and submission state. They are intended for wrappers that want `squeue`/`sacct`/`scontrol` semantics without scraping text output.
State filters accept `state` or `states` and understand common Slurm short codes such as `PD`, `R`, `S`, `CD`, `CA`, `F`, `TO`, `NF`, `OOM`, and `PR`, while responses keep the existing long state names.
Field projection accepts `fields`, `format`, `Format`, `o`, or `O`. Values may use internal snake_case names such as `job_id,state` or common Slurm format tokens such as `%i,%P,%j,%u,%t,%D,%R`; width modifiers like `%.18i` are ignored for JSON projection.

#### `POST /slurm/sbatch`

  - **Description**: Submits a queued batch job for an existing CSOJ problem, using Slurm-style scheduling fields. It writes optional JSON-provided files into the submission directory, expands job arrays, records `Submitted` accounting events, and enqueues the resulting submissions.
  - **Request Body**: Requires `user_id`/`user` and `problem_id`. Supports `name`/`job_name`, `work_dir`/`chdir`, `stdin_path`/`input`, `stdout_path`/`output`, `stderr_path`/`error`, `open_mode`, `comment`, `mail_type`, `mail_user`, `exclusive`, `requeue`, `export`, `environment`, `partition`/`cluster`, `account`, `qos`, `priority`, `nice`, `hold`, `cpus`, `ntasks`, `cpus_per_task`, `nodes`, `memory`/`mem`, `mem_per_cpu`/`memory_per_cpu`, `begin`/`begin_time`/`start_time`, `deadline`, `time`/`time_limit`, `dependencies`, `reservation`, `nodelist`/`node_list`/`nodeslist`, `exclude`/`exclude_nodes`, `constraint`, `gres`, `tres`, `licenses`, `array`, `files`, `files_base64`, `script`, `script_path`, and `wrap`. If `script` is omitted and `wrap` is provided, CSOJ writes a minimal `sbatch.sh` that runs the wrapped command.
  - **Script Directives**: If `script` contains leading `#SBATCH` lines, CSOJ parses common directives as defaults: `-J`/`--job-name`, `-D`/`--chdir`, `-i`/`--input`, `-o`/`--output`, `-e`/`--error`, `--open-mode`, `--comment`, `--mail-type`, `--mail-user`, `--exclusive`, `--requeue`, `--no-requeue`, `--export`, `-p`/`--partition`, `-A`/`--account`, `--qos`, `--priority`, `--nice`, `-n`/`--ntasks`, `-N`/`--nodes`, `-c`/`--cpus-per-task`, `--cpus-per-job`, `--mem`, `--mem-per-cpu`, `--hold`, `--begin`, `--deadline`, `-t`/`--time`, `-d`/`--dependency`, `--reservation`, `-w`/`--nodelist`/`--nodeslist`, `-x`/`--exclude`, `-C`/`--constraint`, `--gres`, `--tres`/`--tres-per-job`, `--licenses`, and `-a`/`--array`. Common short options also accept compact Slurm form such as `-Jname`, `-pdebug`, `-n4`, `-c2`, `-wn[01-03]`, and `-t01:00:00`. Explicit JSON fields override script directives.
  - **Preemption Requeue**: If a running job with `requeue`/`#SBATCH --requeue` is preempted by a higher QoS, CSOJ records `Preempted` and `Requeued` accounting events, clears old runtime/container state, and returns the same submission ID to the pending queue. Jobs without `requeue` remain failed with reason `Preempted`.
  - **Licenses**: `licenses` and `#SBATCH --licenses=foo:2` are stored as Slurm license requests and merged into internal TRES as `license/foo:2`, so configured license pools are consumed by the existing scheduler.
  - **Node Selection**: `nodelist`/`#SBATCH -w` restricts eligible scheduler nodes, while `exclude`/`#SBATCH -x` removes nodes from consideration. Values accept comma, semicolon, or space separated node names and Slurm-style bracket ranges such as `n[01-03]`. Multi-node jobs choose distinct nodes from the filtered set.
  - **Resource Shape**: `cpus` is treated as total CPUs. If omitted, total CPUs are derived from `ntasks * cpus_per_task`, defaulting the missing side to `1`. JSON memory fields and `#SBATCH --mem`/`--mem-per-cpu` accept integer MB values or strings with `K`/`KB`/`KiB`, `M`/`MB`/`MiB`, `G`/`GB`/`GiB`, and `T`/`TB`/`TiB` suffixes, normalized to MB. JSON `time`/`time_limit` accept integer seconds or Slurm-style strings, and `#SBATCH -t/--time` accepts Slurm-style `minutes`, `minutes:seconds`, `hours:minutes:seconds`, `days-hours`, `days-hours:minutes`, and `days-hours:minutes:seconds` formats. Batch `time_limit` is enforced as total job wall time by capping each workflow step to the remaining job time. JSON `begin`/`begin_time`/`start_time`, `deadline`, `#SBATCH --begin`, and `#SBATCH --deadline` accept RFC3339 plus `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS`, and `YYYY-MM-DD` strings. `exclusive`/`#SBATCH --exclusive` waits for idle nodes and reserves every core on each allocated node so no other job or allocation can share them. `#SBATCH -N/--nodes` accepts Slurm-style `min-max` ranges and stores the minimum node count. Multi-node batch jobs reserve CPU and memory across distinct scheduler nodes, expose the full nodelist through Slurm fields and environment variables, and run the workflow controller container on the first allocated node.
  - **Batch Environment**: Runtime containers receive Slurm-like variables including `SLURM_JOB_ID`, `SLURM_JOB_NAME`, `SLURM_JOB_PARTITION`, `SLURM_JOB_NODELIST`, `SLURM_NTASKS`, `SLURM_CPUS_PER_TASK`, `SLURM_CPUS_ON_NODE`, `SLURM_NNODES`, `SLURM_JOB_NUM_NODES`, `SLURM_JOB_CPUS_PER_NODE`, `SLURM_MEM_PER_NODE`, and array variables for array tasks. For multi-node batch jobs, `SLURM_CPUS_ON_NODE` describes the first execution node and `SLURM_JOB_CPUS_PER_NODE` describes the full allocation.
  - **Batch I/O**: `stdout_path`/`#SBATCH -o` defaults to `slurm-%j.out` for `sbatch` requests, and `stderr_path`/`#SBATCH -e` mirrors to the stdout path when omitted. Captured runtime streams are written into files under the submission content directory. Relative output paths are resolved under `work_dir` when that work directory is inside `/mnt/work`; absolute output paths must also point under `/mnt/work`. Filename patterns support `%%`, `%j`, `%A`, `%a`, `%x`, `%u`, and `%N`. `open_mode` accepts `truncate` (default) or `append`. Explicit `work_dir`/`#SBATCH -D` and `stdin_path`/`#SBATCH -i` wrap workflow commands with a shell `cd` and stdin redirection inside the runtime container.
  - **Mail Notifications**: `mail_type`/`#SBATCH --mail-type` and `mail_user`/`#SBATCH --mail-user` send SMTP notifications when top-level `mail.enabled` is configured. Supported mail types are `BEGIN`, `END`, `FAIL`, `TIME_LIMIT`, `REQUEUE`, `ALL`, and `NONE`; `FAIL` also receives time-limit failures. `BEGIN` fires when the scheduler starts the job, `END` after successful completion, `FAIL`/`TIME_LIMIT` when a queued or running batch job fails, and `REQUEUE` when preemption returns a requeueable job to the pending queue.
  - **Environment Export**: `environment` is a JSON object of variables added to the runtime container. `export` accepts Slurm-style comma separated entries such as `ALL,FOO=bar`; `ALL`, `NONE`, and `NIL` are accepted as compatibility markers, but HTTP submissions cannot copy an implicit caller shell environment.
  - **Dependencies**: Supports `afterok:<job_id>[:<job_id>...]`, `afterany:<job_id>[:<job_id>...]`, `afternotok:<job_id>[:<job_id>...]`, `after:<job_id>[:<job_id>...]`, `aftercorr:<array_job_id>`, and `singleton`. Dependency job IDs may also be Slurm-style array selectors such as `array_id_7` or `array_id_[1-3:2]`; referencing an array ID without a task selector waits on the whole array. `aftercorr` is for job arrays and waits for the corresponding task ID in the dependency array to succeed. `singleton` waits for active jobs from the same user with the same Slurm job name; explicit `job_name` wins, otherwise the problem ID is the fallback name. Comma, semicolon, or space separated clauses are treated as AND. `?` separates OR alternatives, so `afterok:a?afterok:b` can start after either dependency succeeds.
  - **Response Fields**: Includes `job_id`, `submission_id`, `name`, `job_name`, `problem_id`, `state`, `partition`, `cpus`, `ntasks`, `cpus_per_task`, `nodes`, and `licenses`; array submissions also include `array_job_id`, `submission_ids`, `task_ids`, and `array_max_running`.

#### `GET /slurm/sinfo`

  - **Description**: Gets partition/node availability in a `sinfo`-like shape.
  - **Query Parameters**: Supports `partition`/`cluster`, `node`/`nodelist`, `state`/`states`, and `fields`.
  - **Fields**: Includes `partition`, `avail`, `timelimit`, `nodes`, `state`, `native_state`, `nodelist`, `node`, `cpus`, `alloc_cpus`, `idle_cpus`, `memory`, `alloc_memory`, `idle_memory`, `features`, `gres`, `runtime`, and `reason`. Node `state` is derived dynamically from drain/down status and allocated cores, so active nodes can report `MIXED` or `ALLOCATED`.

#### `GET /slurm/squeue`

  - **Description**: Gets queued/running jobs in an `squeue`-like shape.
  - **Query Parameters**: Supports `job_id`, `array_job_id`, `array_task_id`, `partition`/`cluster`, `state`/`states`, `user`/`user_id`, `name`/`job_name`, `account`, `qos`, and `fields`. `job_id` accepts normal submission IDs and Slurm-style array-task selectors such as `array_job_id_7` or `array_job_id_[1,3-5]`. Selectors such as `user`, `partition`, `account`, `qos`, `state`/`states`, and `job_name` accept comma, semicolon, or space separated lists.
  - **Fields**: Includes `job_id`, `array_job_id`, `array_task_id`, `partition`, `name`, `user_id`, `state`, `reason`, `nodelist`, `requested_nodelist`, `exclude_nodes`, `qos`, `account`, `cpus`, `ntasks`, `cpus_per_task`, `nodes`, `memory`, `tres`, `licenses`, `billing_units`, `priority`, `queue_position`, and `submit_time`.

#### `GET /slurm/sacct`

  - **Description**: Gets accounting records in a `sacct`-like shape.
  - **Query Parameters**: Supports `job_id`, `user`, `problem_id`, `job_name`, `partition`, `node`, `account`, `qos`, `array_job_id`, `array_task_id`, `event`, `state`/`states`, `native_state`, `start_time`/`starttime`, `end_time`/`endtime`, `page`, `limit`, and `fields`. `job_id` accepts comma, semicolon, or space separated IDs, plus Slurm-style array-task selectors such as `array_job_id_7` or `array_job_id_[1,3-5]`. Exact-match selectors such as `user`, `partition`, `account`, `qos`, `event`, and `native_state` accept comma, semicolon, or space separated lists; `job_name` uses comma or semicolon lists so names with spaces remain valid.
  - **Fields**: Includes `job_id`, `container_id`, `array_job_id`, `array_task_id`, `user_id`, `partition`, `node`, `account`, `qos`, `job_name`, `problem_id`, `event`, `state`, `native_state`, `reason`, `step_name`, `exit_code`, `alloc_cpus`, `alloc_mem`, `tres`, `billing_units`, `score`, `performance`, `message`, and `accounting_time`.

#### `GET /slurm/sreport`

  - **Description**: Gets accounting aggregates in an `sreport`-like shape. The JSON view defaults to grouping resource usage by account and can group by user, partition, QoS, or job.
  - **Query Parameters**: Supports `group_by`/`group`/`by` with `account`, `user`, `partition`, `qos`, or `job`, plus the same filters as `sacct`: `job_id`, `user`, `problem_id`, `job_name`, `partition`, `node`, `account`, `qos`, `array_job_id`, `array_task_id`, `event`, `state`/`states`, `native_state`, `start_time`/`starttime`, `end_time`/`endtime`, and `fields`. If `event` is omitted, only terminal usage events such as completed, failed, interrupted, preempted, allocation release, and completed/failed run steps are aggregated.
  - **Fields**: Includes `group_by`, `name`, `account`/`user_id`/`partition`/`qos`/`job_id`, `records`, `jobs`, `alloc_cpus`, `alloc_mem`, `billing_units`, `start_time`, and `end_time`.

#### `GET /slurm/seff/:id` and `GET /slurm/seff`

  - **Description**: Gets a `seff`-like efficiency summary for one job or interactive allocation from accounting records. It reports elapsed time and allocated CPU/memory for all jobs. CPU and memory efficiency are populated when matching `srun` step runtime samples are available; otherwise the response marks efficiency metrics as unavailable rather than fabricating usage.
  - **Query Parameters**: Path `:id` or `job_id`/`submission_id`/`allocation_id` selects the job. Optional `step_id`/`step`/`step_name`/`job_step_id` narrows the report to one step. Supports `fields`.
  - **Fields**: Includes `job_id`, `step_id`, `job_name`, `problem_id`, `user_id`, `partition`, `node`, `account`, `qos`, `state`, `native_state`, `event`, `reason`, `start_time`, `end_time`, `elapsed_seconds`, `alloc_cpus`, `alloc_mem`, `allocated_cpu_seconds`, `cpu_used_seconds`, `cpu_efficiency`, `cpu_efficiency_available`, `max_rss`, `max_rss_mb`, `memory_efficiency`, `memory_efficiency_available`, `billing_units`, `record_count`, `step_count`, `usage_source`, `efficiency_available`, and `message`.

#### `GET /slurm/sprio`

  - **Description**: Gets queued-job priority breakdowns in a `sprio`-like shape.
  - **Query Parameters**: Supports `job_id`, `array_job_id`, `array_task_id`, `user`/`user_id`, `partition`/`cluster`, `account`, `qos`, `state`/`states`, `native_status`, and `fields`. `job_id` accepts normal submission IDs and Slurm-style array-task selectors such as `array_job_id_7` or `array_job_id_[1,3-5]`. Selectors such as `user`, `partition`, `account`, `qos`, and `state`/`states` accept comma, semicolon, or space separated lists.
  - **Fields**: Includes `job_id`, `array_job_id`, `array_task_id`, `user_id`, `partition`, `account`, `qos`, `state`, `native_status`, `priority`, `manual_priority`, `partition_priority`, `qos_priority`, `fairshare_priority`, `fairshare_penalty`, `age_priority`, `job_size_priority`, and `nice_penalty`.

#### `GET /slurm/sshare`

  - **Description**: Gets configured account fairshare and decayed usage in an `sshare`-like shape.
  - **Query Parameters**: Supports `account` and `fields`.
  - **Fields**: Includes `account`, `parent_account`, `raw_shares`, `normalized_shares`, `raw_usage`, `effective_usage`, `usage_penalty`, `running_jobs`, and `submitted_jobs`.

#### `GET /slurm/sdiag`

  - **Description**: Gets scheduler diagnostics in an `sdiag`-like shape for queue health, resource pressure, license pools, allocations, and running steps.
  - **Query Parameters**: Supports `fields`.
  - **Fields**: Includes `generated_at`, `queue_size`, `backfill`, `queue_lengths`, `jobs_by_state`, `jobs_by_native_state`, `active_jobs`, `partitions`, `partition_count`, `nodes`, `total_cpus`, `allocated_cpus`, `idle_cpus`, `total_memory`, `allocated_memory`, `idle_memory`, `licenses`, `allocations_by_state`, `steps_by_state`, `priority_weights`, and `fairshare_decay`.

#### `GET|POST|DELETE /slurm/strigger` and `POST /slurm/strigger/evaluate`

  - **Description**: Manages persistent event triggers in a `strigger`-like shape. This HTTP view stores trigger definitions and evaluates them against current jobs, nodes, allocations, run steps, and accounting records; it does not execute arbitrary server-side programs.
  - **Create/Update Body**: `POST /slurm/strigger` supports `id`/`trigger_id`, `name`, `event`/`type`, `job_id`, `user`/`user_id`, `partition`/`cluster`, `node`, `state`, `action`, `program`, `flags`, and `active`. Supported events include `job_end`, `job_fail`, `job_cancel`, `job_time_limit`, `job_state`, `node_down`, `node_drain`, `node_up`, `allocation_release`, and `run_end`.
  - **List Query Parameters**: `GET /slurm/strigger` supports `trigger_id`/`id`, `name`, `event`/`type`, `active`, `evaluate=true`, and `fields`.
  - **Evaluate/Delete**: `POST /slurm/strigger/evaluate` evaluates active triggers, marks matched triggers as fired, and deactivates them unless `flags` contains `keep-active`. `DELETE /slurm/strigger/:id` or `DELETE /slurm/strigger?trigger_id=...` clears trigger definitions.
  - **Fields**: Includes `trigger_id`, `name`, `event`, `job_id`, `user_id`, `partition`, `node`, `state`, `action`, `program`, `flags`, `active`, `fired_at`, `matched`, `match_count`, `message`, `created_at`, and `updated_at`.

#### `GET|POST|PATCH|DELETE /slurm/scrontab` and `POST /slurm/scrontab/evaluate`

  - **Description**: Manages persistent scheduled batch submissions in a `scrontab`-like shape. Entries store a cron schedule plus an embedded `sbatch` request. This HTTP view does not run a background daemon by itself; an operator or controller should call `POST /slurm/scrontab/evaluate` periodically.
  - **Create/Update Body**: Supports `id`/`entry_id`, `name`, `schedule`, `enabled`, `next_run`/`next_run_at`, and `batch`. The nested `batch` object accepts the same fields as `/slurm/sbatch`, including `user`/`user_id`, `problem_id`, `partition`/`cluster`, resources, arrays, files, scripts, and `wrap`.
  - **Schedules**: Supports `@hourly`, `@daily`, `@weekly`, `@monthly`, `@every <duration>`, and five-field cron expressions with minute, hour, day-of-month, month, and day-of-week fields. Duration values accept Go-style values such as `10m` or Slurm-style time values such as `00:10:00`.
  - **List/Evaluate/Delete**: `GET /slurm/scrontab` supports `entry_id`/`id`, `user`/`user_id`, `problem_id`, `enabled`, and `fields`. `POST /slurm/scrontab/evaluate` submits due enabled entries through the same path as `/slurm/sbatch`, updates `last_run_at`, `last_job_id`, `run_count`, and `next_run_at`, and returns per-entry submission results. `DELETE /slurm/scrontab/:id` or `DELETE /slurm/scrontab?entry_id=...` removes entries.
  - **Fields**: Includes `entry_id`, `name`, `schedule`, `enabled`, `user_id`, `problem_id`, `next_run_at`, `last_run_at`, `last_job_id`, `run_count`, `message`, `submitted`, `job_id`, `error`, `created_at`, and `updated_at`.

#### `GET /slurm/sacctmgr/show/accounts`

  - **Description**: Shows configured accounts in a `sacctmgr show account`-like shape. Supports `account` and `fields`.

#### `GET /slurm/sacctmgr/ping`

  - **Description**: Gets a `sacctmgr ping`-like accounting database health record. In CSOJ this checks the configured database connection used for accounting records rather than a separate SlurmDBD process.
  - **Query Parameters**: Supports `fields`.
  - **Fields**: Includes `generated_at`, `daemon`, `service`, `role`, `status`, `responding`, `primary`, and `message`.

#### `POST /slurm/sacctmgr/account`, `PATCH /slurm/sacctmgr/account/:name`, and `DELETE /slurm/sacctmgr/account/:name`

  - **Description**: Adds, updates, or deletes runtime account definitions used by scheduler association, limit, and fairshare checks.
  - **Request Body**: Uses account fields such as `name`/`account`, `users`/`user`, `allow_qos`/`allowed_qos`/`qos`, `max_jobs`, `max_submit`, `max_billing_running`, `max_billing_submit`, `fairshare`, and `parent`/`parent_account`. User and QoS lists accept JSON arrays or Slurm-style comma/space strings.

#### `GET /slurm/sacctmgr/show/users`

  - **Description**: Shows configured scheduler users in a `sacctmgr show user`-like shape, aggregated from account/user/QoS associations. Supports `user`/`user_id`/`name`, `account`/`default_account`, `qos`, and `fields`.
  - **Fields**: Includes `user`, `user_id`, `username`, `principals`, `default_account`, `accounts`, `qos`, `allowed_qos`, `association_count`, and `associations`. `user_id` and `username` are resolved from CSOJ users when the association principal matches an existing user ID or username.

#### `GET /slurm/sacctmgr/show/clusters` and `GET /slurm/sacctmgr/show/cluster`

  - **Description**: Shows configured scheduler clusters in a `sacctmgr show cluster`-like shape. In CSOJ this maps to the scheduler cluster/partition objects used by `sinfo`, `squeue`, and `scontrol show partition`.
  - **Query Parameters**: Supports `cluster`/`name`/`partition` and `fields`.
  - **Fields**: Includes `cluster`, `name`, `partition`, `state`, `native_state`, `control_host`, `control_addr`, `rpc`, `node_count`, `nodes`, `node_names`, `down_nodes`, `drained_nodes`, `total_cpus`, `allocated_cpus`, `idle_cpus`, `total_memory`, `allocated_memory`, `idle_memory`, `tres`, `features`, `runtimes`, `queue_length`, `priority_tier`, `max_time`, `max_jobs`, `account_count`, `qos_count`, `license_count`, and `licenses`.

#### `GET /slurm/sacctmgr/show/config`

  - **Description**: Shows a `sacctmgr show config`-like accounting and scheduler configuration snapshot, including database health, clusters, accounts, QoS, reservations, TRES, and license pools.
  - **Query Parameters**: Supports `fields`.
  - **Fields**: Includes `generated_at`, `accounting_storage`, `database_status`, `database_message`, `queue_size`, `backfill`, `priority_weights`, `billing_weights`, `fairshare_decay`, `cluster_count`, `partition_count`, `clusters`, `account_count`, `accounts`, `qos_count`, `qos`, `reservation_count`, `reservations`, `tres`, and `licenses`.

#### `GET /slurm/sacctmgr/show/stats`

  - **Description**: Shows a `sacctmgr show stats`-like operational summary over jobs, accounting records, allocations, run steps, triggers, and scheduled entries.
  - **Query Parameters**: Supports `fields`.
  - **Fields**: Includes `generated_at`, `database_status`, `database_responding`, `database_message`, `jobs`, `accounting_records`, `allocations`, `steps`, `triggers`, `cron_entries`, `jobs_by_state`, `accounting_by_event`, `accounting_by_state`, `allocations_by_state`, `steps_by_state`, and `queue_lengths`.

#### `GET /slurm/sacctmgr/show/jobs` and `GET /slurm/sacctmgr/show/job`

  - **Description**: Shows job records in a `sacctmgr show job`-like shape by combining submission state with accounting summaries.
  - **Query Parameters**: Supports `job_id`, `array_job_id`, `array_task_id`, `problem`/`problem_id`, `user`/`user_id`, `partition`/`cluster`, `name`/`job_name`, `account`, `qos`, `status`/`native_status`, `state`/`states`, and `fields`. Job IDs accept Slurm-style array selectors.
  - **Fields**: Includes normal job fields plus `job`, `accounting_records`, `first_event`, `last_event`, `first_accounting_time`, `last_accounting_time`, `start_time`, `end_time`, `elapsed_seconds`, `terminal_accounting`, `alloc_cpus`, `alloc_mem`, and `accounting_billing_units`.

#### `GET /slurm/sacctmgr/show/problems` and `GET /slurm/sacctmgr/show/problem`

  - **Description**: Shows loaded CSOJ problems in a `sacctmgr show problem`-like shape, including their default scheduling metadata and submission/accounting counts.
  - **Query Parameters**: Supports `problem`/`problem_id`/`name`, `partition`/`cluster`, `account`, `qos`, `state`/`states`, and `fields`.
  - **Fields**: Includes `problem`, `problem_id`, `name`, `level`, `partition`, `cluster`, `state`, `start_time`, `end_time`, `max_submissions`, `cpu`/`cpus`, `memory`, `workflow_steps`, `score_mode`, `account`, `qos`, `priority`, `nice`, `hold`, `time_limit`, `dependencies`, `reservation`, `requested_nodelist`, `exclude_nodes`, `constraint`, `gres`, `tres`, `array`, `submissions`, and `accounting_records`.

#### `GET /slurm/sacctmgr/show/resources` and `GET /slurm/sacctmgr/show/resource`

  - **Description**: Shows configured resources in a `sacctmgr show resource`-like shape. This view is backed by the same TRES aggregation used by `show tres`.
  - **Query Parameters**: Supports `resource`/`name`/`tres`, `type`, and `fields`.
  - **Fields**: Includes `resource`, `name`, `tres`, `type`, `server`, `manager`, `count`, `billing_weight`, `source`, `sources`, and `state`.

#### `GET /slurm/sacctmgr/show/runawayjobs`

  - **Description**: Shows active job and allocation records that are candidates for `sacctmgr show runawayjobs`-style inspection. CSOJ reports active batch jobs without terminal accounting events and active allocations that have not been released; it does not mutate or repair records from this read-only endpoint.
  - **Query Parameters**: Supports `job_id`, `user`/`user_id`, `partition`/`cluster`, `state`/`states`, and `fields`.
  - **Fields**: Includes normal job/allocation fields plus `kind`, `started_at`, `elapsed_seconds`, `accounting_records`, `last_event`, `runaway_candidate`, and `candidate_reason`.

#### `GET /slurm/sacctmgr/show/transactions` and `GET /slurm/sacctmgr/show/transaction`

  - **Description**: Shows accounting-record changes in a `sacctmgr show transaction`-like shape.
  - **Query Parameters**: Supports `transaction_id`/`id`, `action`/`event`, `job_id`/`submission_id`, `user`/`user_id`, `problem_id`, `job_name`, `partition`/`cluster`, `node`, `account`, `qos`, `array_job_id`, `array_task_id`, `state`/`states`, `start_time`/`starttime`, `end_time`/`endtime`, `page`, `limit`, and `fields`.
  - **Fields**: Includes `transaction_id`, `id`, `timestamp`, `action`, `event`, `actor`, `user_id`, `object_type`, `object_id`, `job_id`, `container_id`, `partition`, `cluster`, `node`, `account`, `qos`, `state`, `native_state`, `reason`, `message`, `billing_units`, and `accounting_time`.

#### `GET /slurm/sacctmgr/show/events` and `GET /slurm/sacctmgr/show/event`

  - **Description**: Shows accounting events plus current down/drain node event snapshots in a `sacctmgr show event`-like shape.
  - **Query Parameters**: Supports `event`/`type`, `node`/`nodelist`, `partition`/`cluster`, `user`/`user_id`, `state`/`states`, `start_time`/`starttime`, `end_time`/`endtime`, `page`, `limit`, and `fields`.
  - **Fields**: Includes `event_id`, `id`, `source`, `event`, `type`, `timestamp`, `job_id`, `container_id`, `user_id`, `problem_id`, `partition`, `cluster`, `node`, `account`, `qos`, `state`, `native_state`, `reason`, `message`, and `accounting_time`.

#### `POST /slurm/sacctmgr/user`, `PATCH /slurm/sacctmgr/user/:name`, and `DELETE /slurm/sacctmgr/user/:name`

  - **Description**: Adds, updates, or deletes runtime user/account associations in a `sacctmgr add/modify/delete user`-like shape. Because CSOJ stores limits at the account level, limit fields on this endpoint update the target account association data.
  - **Request Body**: Upsert supports `name`/`user`/`user_id`, `account`/`default_account`, `qos`/`allow_qos`/`allowed_qos`, `fairshare`, `max_jobs`, `max_submit`, `max_billing_running`, and `max_billing_submit`.
  - **Delete Query Parameters**: Delete supports `account`/`default_account` and `qos`. If `account` is omitted, delete is allowed only when the user has exactly one matching account association.

#### `GET /slurm/sacctmgr/show/qos`

  - **Description**: Shows configured QoS records. Supports `qos` and `fields`.

#### `POST /slurm/sacctmgr/qos`, `PATCH /slurm/sacctmgr/qos/:name`, and `DELETE /slurm/sacctmgr/qos/:name`

  - **Description**: Adds, updates, or deletes runtime QoS definitions used by scheduler priority, limit, and preemption checks. Preempted jobs are failed with reason `Preempted` unless their job `requeue` flag is set, in which case they are reset and submitted back to the queue.
  - **Request Body**: Uses QoS fields such as `name`/`qos`, `priority`, `max_jobs_per_user`/`max_jobs`, `max_submit_jobs_per_user`/`max_submit`, `max_cpu_per_job`/`max_cpus_per_job`, `max_memory_per_job`/`max_mem_per_job`, `max_billing_per_job`, `max_billing_per_user_running`/`max_billing_running`, `max_billing_per_user_submit`/`max_billing_submit`, `max_time`/`time_limit`, and `preempt`/`preempt_qos`. Memory accepts integer MB or Slurm-style memory strings, time accepts integer seconds or Slurm-style time strings, and preempt lists accept JSON arrays or comma/space strings.

#### `GET /slurm/sacctmgr/show/assoc`

  - **Description**: Shows account/user/QoS associations expanded from account configuration. Supports `account`, `user`/`user_id`, `qos`, and `fields`.

#### `GET /slurm/sacctmgr/show/tres`

  - **Description**: Shows configured trackable resources in a `sacctmgr show tres`-like shape. The view aggregates CPU, memory, and node totals from configured scheduler nodes; GRES from node `gres`; license TRES from configured license pools; and any resource keys declared in scheduler `billing_weights`.
  - **Query Parameters**: Supports `tres`/`name`, `type`, and `fields`. Types include `cpu`, `mem`, `node`, `gres`, `license`, and `custom`.
  - **Fields**: Includes `id`, `tres`, `type`, `name`, `count`, `billing_weight`, `source`, `sources`, and `configured`.

#### `POST /slurm/sacctmgr/assoc`, `PATCH /slurm/sacctmgr/assoc/:account`, and `DELETE /slurm/sacctmgr/assoc/:account`

  - **Description**: Adds, updates, or deletes runtime account/user/QoS associations used by scheduler association and fairshare checks.
  - **Request Body**: Upsert supports `account`, `user`/`user_id`, `qos`, `parent_account`, `fairshare`, `max_jobs`, `max_submit`, `max_billing_running`, and `max_billing_submit`.
  - **Delete Query Parameters**: Delete supports `user`/`user_id` and/or `qos`. Because associations are represented as account-level user and QoS sets, deleting the last user or QoS on an account is rejected; delete the account or replace its full account definition instead.

#### `POST /slurm/salloc`

  - **Description**: Creates an interactive allocation without launching a judging workflow. It uses the same partition, account, QoS, reservation, node selection, feature, GRES, license, and resource-cap scheduling checks as batch jobs, then reserves scheduler CPU, memory, and any requested `license/...` TRES until released.
  - **Request Body**: Supports `user_id`/`user`, `partition`/`cluster`, `cpus`, `nodes`, `memory`/`mem`, `account`, `qos`, `tres`, `gres`, `time`/`time_limit`, `constraint`, `reservation`, `nodelist`/`node_list`/`nodeslist`, `exclude`/`exclude_nodes`, and `exclusive`. `nodes` reserves resources across distinct scheduler nodes, with at least one CPU per requested node. `exclusive` waits for idle nodes and reserves every core on each allocated node for the allocation. `memory`/`mem` accept integer MB or Slurm-style memory strings and are reserved per allocated node; `time`/`time_limit` accept integer seconds or the same Slurm-style time strings as `sbatch`.
  - **Response Fields**: Includes `allocation_id`, `job_id`, `state`, `partition`, `node`/`nodelist`, `requested_nodelist`, `exclude_nodes`, `nodes`, `cpus`, `memory`, `allocated_cores`, `allocated_node_cores`, `tres`, `gres`, `billing_units`, `exclusive`, and an `env` object with Slurm-like values such as `SLURM_JOB_ID`, `SLURM_JOB_PARTITION`, `SLURM_JOB_NODELIST`, `SLURM_JOB_NUM_NODES`, `SLURM_CPUS_ON_NODE`, and `SLURM_JOB_CPUS_PER_NODE`. `allocated_cores` describes the first allocated node; `allocated_node_cores` contains the full node-to-core map.
  - **Execution Boundary**: Multi-node `salloc` reserves and accounts resources across all allocated nodes. `/slurm/srun` currently launches its runtime container or Pod on the first allocated node, so a single step can use only the first node's allocated cores.

#### `GET /slurm/salloc`

  - **Description**: Lists interactive allocations. Supports `status` and `fields`.

#### `GET /slurm/salloc/:id`

  - **Description**: Shows one interactive allocation.

#### `POST /slurm/salloc/:id/release` and `DELETE /slurm/salloc/:id`

  - **Description**: Releases an active interactive allocation and returns its CPU, memory, and license resources to the scheduler.

#### `POST /slurm/sbcast`

  - **Description**: Stages files for an active interactive allocation in an `sbcast`-like shape. Staged files are copied into each later `srun` container before the command executes, using the same Docker/Kubernetes copy mechanism as batch input files. This HTTP view accepts file contents in the request body rather than reading arbitrary local server paths.
  - **Request Body**: Requires `allocation_id`/`job_id`. For one file, provide `path`/`destination`/`dest_path` plus `content` or `content_base64`. For multiple files, use `files` as a destination-to-content object and/or `files_base64` as a destination-to-base64 object. Absolute destinations are copied to that container path; relative destinations are copied under `/mnt/work`.
  - **Response Fields**: Includes `allocation_id`, `job_id`, `state`, `files`, `file_count`, `bytes`, and `staging_dir`. Staging data is removed when the allocation is released.

#### `POST /slurm/srun`

  - **Description**: Runs one command inside an active interactive allocation by creating a short-lived runtime container or Kubernetes Pod on the allocation's node. If `allocation_id`/`job_id` is omitted, CSOJ creates a temporary allocation, runs the step, and releases that allocation after the step finishes.
  - **Request Body**: Requires either `allocation_id`/`job_id` for an existing allocation or `user_id`/`user` for an implicit temporary allocation, plus either `command` as an argv array or `command_line` as a shell command. Supports `image`, `timeout`/`time`/`time_limit`, `cpus`, `memory`/`mem`, `root`, and `network`. For implicit allocations it also supports `partition`/`cluster`, `nodes`, `account`, `qos`, `tres`, `gres`, `constraint`, `reservation`, `nodelist`/`node_list`/`nodeslist`, `exclude`/`exclude_nodes`, and `exclusive`. `timeout`/`time`/`time_limit` accept integer seconds or Slurm-style time strings; memory fields accept integer MB or Slurm-style memory strings. Omitted `cpus`/`memory` default to the full allocation; implicit allocations default `cpus` to `1`. Requested step resources must fit within the active allocation and any currently running steps. Step timeout is capped to the allocation's remaining `time_limit`; an already expired allocation is released with reason `TimeLimit` and rejects new steps.
  - **Response Fields**: Includes `step_id`, `allocation_id`, `state`, `exit_code`, `stdout`, `stderr`, `image`, `runtime`, `node`, `cpus`, `memory`, `allocated_cores`, `alloc_tres`, `elapsed_seconds`, `started_at`, and `finished_at`. Implicit allocation runs also include `implicit_allocation` and `allocation_released`.

#### `GET /slurm/srun`

  - **Description**: Lists `srun` steps. Supports `allocation_id`/`job_id`, `status`/`native_status`, `state`/`states`, and `fields`.

#### `GET /slurm/srun/:id`

  - **Description**: Shows one `srun` step, including captured output and exit status.

#### `GET /slurm/sattach` and `GET /slurm/sattach/:id`

  - **Description**: Shows captured output for an `srun` step in an `sattach`-like shape. This HTTP view is non-streaming: it returns the step's currently recorded stdout/stderr and status rather than keeping an interactive terminal attached.
  - **Query Parameters**: `GET /slurm/sattach/:id` treats `:id` as a step ID. `GET /slurm/sattach` supports `step_id`/`id`/`job_step_id` for one step, or `allocation_id`/`job_id`, `status`/`native_status`, `state`/`states`, `user`/`user_id`, and `fields` for matching step records.
  - **Fields**: Includes the `srun` step fields plus `job_step_id`, `attached`, `stdin_supported`, `stdout_bytes`, and `stderr_bytes`.

#### `GET /slurm/sstat`

  - **Description**: Lists Slurm-like statistics for interactive `srun` steps.
  - **Query Parameters**: Supports `allocation_id`/`job_id`, `step_id`/`id`, `status`/`native_status`, `state`/`states`, `user`/`user_id`, and `fields`.
  - **Response Fields**: Includes `step_id`, `job_id`, `job_step_id`, `state`, `native_status`, `alloc_cpus`, `alloc_memory`, `allocated_cores`, `alloc_tres`, `elapsed_seconds`, `ave_cpu`, `ave_rss`, `max_rss`, `max_vmsize`, `exit_code`, `started_at`, and `finished_at`. Docker-backed `srun` steps sample Docker container stats while the command runs. Kubernetes-backed steps sample `kubectl top pod` when the cluster exposes Metrics API data; if metrics are unavailable the counters remain `0`. `ave_cpu` is CPU seconds and memory counters are bytes.

#### `GET /slurm/sstat/:id`

  - **Description**: Shows one `sstat` step statistics record.

#### `GET /slurm/scontrol/show/job/:id`

  - **Description**: Shows one job in an `scontrol show job`-like shape. `:id` may be a submission ID or a Slurm-style single array-task selector such as `array_job_id_7`. Supports `fields`.

#### `GET /slurm/scontrol/show/jobs`

  - **Description**: Lists jobs in an `scontrol show jobs`-like shape, including queued, running, suspended, and finished submissions.
  - **Query Parameters**: Supports `job_id`/`id`, `array_job_id`, `array_task_id`, `user`/`user_id`, `partition`/`cluster`, `name`/`job_name`, `account`, `qos`, `status`/`native_status`, `state`/`states`, and `fields`. `job_id` accepts normal submission IDs and Slurm-style array-task selectors such as `array_job_id_7` or `array_job_id_[1,3-5]`. Selectors such as `user`, `partition`, `account`, `qos`, `status`, `state`/`states`, and `job_name` accept comma, semicolon, or space separated lists.

#### `GET /slurm/scontrol/show/steps` and `GET /slurm/scontrol/show/step/:id`

  - **Description**: Shows interactive `srun` steps in an `scontrol show step`-like shape. `:id` may be a step ID or a Slurm-style `allocation_id.step_id` value.
  - **Query Parameters**: List supports `allocation_id`/`job_id`, `step_id`/`id`/`job_step_id`, `user`/`user_id`, `partition`/`cluster`, `node`/`nodelist`, `status`/`native_status`, `state`/`states`, and `fields`.
  - **Fields**: Includes `step_id`, `job_step_id`, `allocation_id`, `job_id`, `container_id`, `user_id`, `partition`, `node`, `state`, `native_status`, `reason`, `exit_code`, `stdout`, `stderr`, `timeout`, `cpus`, `memory`, `allocated_cores`, `alloc_tres`, `elapsed_seconds`, `started_at`, `finished_at`, and `created_at`.

#### `GET|POST /slurm/scontrol/show/hostnames` and `GET|POST /slurm/scontrol/show/hostlist`

  - **Description**: Expands and compacts Slurm-style node hostlists. `show/hostnames` expands bracket expressions such as `n[01-03],gpu-[1-3:2]` into individual hostnames. `show/hostlist` compacts hostname lists such as `n01,n02,n03` into bracket notation. If no input is supplied, both endpoints use currently configured scheduler nodes.
  - **Inputs**: Query parameters or JSON body support `hostlist`, `nodelist`/`node_list`/`nodeslist`, `nodes`, and `hostnames`. `nodes` and `hostnames` accept JSON arrays or comma/space strings.
  - **Fields**: Includes `hostlist`, `nodelist`, `hostnames`, `nodes`, and `count`.

#### `GET /slurm/scontrol/show/node/:clusterName/:nodeName`

  - **Description**: Shows one scheduler node in an `scontrol show node`-like shape. Supports `fields`. The node state reflects dynamic CPU allocation as `IDLE`, `MIXED`, or `ALLOCATED` unless drain/down state overrides it.

#### `GET /slurm/scontrol/show/nodes`

  - **Description**: Lists scheduler nodes in an `scontrol show nodes`-like shape.
  - **Query Parameters**: Supports `partition`/`cluster`, `node`/`nodelist`, `state`/`states`, and `fields`.

#### `GET /slurm/scontrol/show/daemons` and `GET /slurm/scontrol/show/daemon`

  - **Description**: Shows Slurm-daemon-shaped health records for CSOJ's API controller, scheduler controller, accounting database, and per-node runtime surface. `slurmctld`, `slurmdbd`, `slurmrestd`, and `slurmd` are compatibility roles; they are backed by CSOJ components rather than separate Slurm processes.
  - **Query Parameters**: Supports `daemon`/`name`/`type`, `cluster`/`partition`, `node`/`nodelist`, `status`/`state`/`states`, and `fields`.
  - **Fields**: Includes `daemon_id`, `daemon`, `service`, `role`, `cluster`, `partition`, `node`, `state`, `status`, `responding`, `should_run`, `runtime`, `listen`, `queue_lengths`, `message`, and `generated_at`.

#### `GET /slurm/scontrol/ping`

  - **Description**: Gets an `scontrol ping`-like controller health summary. The response checks the API/scheduler/database control plane and reports node-runtime records through the daemon count; it does not require real Slurm daemons.
  - **Query Parameters**: Supports `fields`.
  - **Fields**: Includes `generated_at`, `responding`, `status`, `mode`, `primary`, `controller_count`, `daemon_count`, `cluster_count`, `controllers`, and `down_daemons`.

#### `GET /slurm/scontrol/show/partition`

  - **Description**: Shows configured partitions in an `scontrol show partition`-like shape.
  - **Query Parameters**: Supports `partition`/`name` and `fields`.

#### `GET /slurm/scontrol/show/config`

  - **Description**: Shows an `scontrol show config`-like scheduler configuration snapshot, including runtime account/QoS/reservation changes.
  - **Query Parameters**: Supports `fields`.
  - **Fields**: Includes `queue_size`, `backfill`, `priority_weights`, `billing_weights`, `fairshare_decay`, `partitions`, `licenses`, `accounts`, `qos`, and `reservations`.

#### `GET /slurm/scontrol/show/licenses`

  - **Description**: Shows Slurm-like global license pool state.
  - **Query Parameters**: Supports `license` and `fields`.
  - **Fields**: Includes `license`, `total`, `used`, `available`, and `owners`.

#### `GET /slurm/scontrol/show/reservations`

  - **Description**: Shows configured reservations. Supports `reservation`/`name` and `fields`.

#### `POST /slurm/scontrol/create/reservation`, `POST|PATCH /slurm/scontrol/update/reservation/:name`, and `DELETE /slurm/scontrol/delete/reservation/:name`

  - **Description**: Adds, updates, or deletes runtime reservations used by scheduler reservation, node exclusion, and reservation resource cap checks.
  - **Request Body**: Uses reservation fields such as `name`/`reservation`, `cluster`/`partition`, `nodes`/`nodelist`/`node_list`/`nodeslist`, `users`, `accounts`, `starttime`/`start_time`/`start`, `endtime`/`end_time`/`end`, `duration`, `cpu`, `memory`/`mem`, `allow_overlap`, and `ignore_running`. Node, user, and account lists accept JSON arrays or Slurm-style comma/space strings. Positive `cpu` and `memory` values cap the total active CPU cores and memory MB allocated by jobs or interactive allocations using that reservation.

#### `POST|PATCH /slurm/scontrol/update/job/:id`

  - **Description**: Updates scheduling fields for one job. `:id` may be a submission ID or a Slurm-style single array-task selector such as `array_job_id_7`.
  - **Request Body**: Supports `name`/`job_name`, `work_dir`/`chdir`, `stdin_path`/`input`, `stdout_path`/`output`, `stderr_path`/`error`, `open_mode`, `comment`, `mail_type`, `mail_user`, `exclusive`, `requeue`, `export`, `environment`, `account`, `qos`, `priority`, `nice`, `hold`, `cpus`, `ntasks`, `cpus_per_task`, `nodes`, `memory`/`mem`, `begin`/`begin_time`/`start_time`, `deadline`, `time`/`time_limit`, `dependencies`, `reservation`, `nodelist`/`node_list`/`nodeslist`, `exclude`/`exclude_nodes`, `constraint`, `gres`, `tres`, `licenses`, and `reason`.
  - **Dependencies**: Supports Slurm-style multiple job IDs inside one dependency clause, for example `afterok:job-a:job-b`, OR alternatives with `?`, for example `afterok:job-a?afterok:job-b`, array task selectors such as `afterok:array_id_7` or `afterok:array_id_[1-3:2]`, and array-corresponding dependencies with `aftercorr:<array_job_id>`.

#### `POST|PATCH /slurm/scontrol/update/node/:clusterName/:nodeName`

  - **Description**: Updates scheduler node state and optional reason. Request body: `{"state": "idle|drain|down|inactive", "reason": "maintenance"}`. Slurm-style `resume` and `undrain` map to `idle`.

#### `POST|PATCH /slurm/scontrol/update/partition/:name`

  - **Description**: Updates runtime partition state and scheduling limits.
  - **Request Body**: Supports `state`, `priority_tier`/`priority`, `max_time`/`time_limit`, `max_jobs`, `allow_users`/`users`, `allow_accounts`/`accounts`, `allow_qos`/`allowed_qos`, and `deny_qos`. Lists accept JSON arrays or Slurm-style comma/space strings, `max_time` accepts integer seconds or Slurm-style time strings, and partition states such as `UP`, `DOWN`, `DRAIN`, and `INACTIVE` are normalized for the scheduler.

#### `POST /slurm/scontrol/hold/:id`

  - **Description**: Holds queued jobs. `:id` may be a submission ID, a bare Slurm array job ID to hold all tasks in that array, or a Slurm-style array-task selector such as `array_job_id_7` or `array_job_id_[1,3]`.

#### `POST /slurm/scontrol/release/:id`

  - **Description**: Releases held queued jobs. `:id` may be a submission ID, a bare Slurm array job ID to release all tasks in that array, or a Slurm-style array-task selector such as `array_job_id_7` or `array_job_id_[1,3]`.

#### `POST /slurm/scontrol/requeue/:id`

  - **Description**: Requeues finished jobs using the same submission IDs. `:id` may be a submission ID, a bare Slurm array job ID to requeue all tasks in that array, or a Slurm-style array-task selector such as `array_job_id_7` or `array_job_id_[1,3]`. Running jobs must be interrupted before requeueing.

#### `POST /slurm/scontrol/suspend/:id`

  - **Description**: Suspends a running job. `:id` may be a submission ID or a Slurm-style single array-task selector such as `array_job_id_7`. Docker-backed jobs use Docker pause and move to Slurm state `SUSPENDED`. Kubernetes-backed jobs send `STOP` to non-PID-1 processes inside the runner container.

#### `POST /slurm/scontrol/resume/:id`

  - **Description**: Resumes a suspended job. `:id` may be a submission ID or a Slurm-style single array-task selector such as `array_job_id_7`. Docker-backed jobs use Docker unpause and move back to Slurm state `RUNNING`. Kubernetes-backed jobs send `CONT` to non-PID-1 processes inside the runner container.

#### `POST /slurm/scontrol/signal/:id` and `POST /slurm/scancel/:id/signal`

  - **Description**: Sends a signal to a running or suspended job and records a `Signaled` accounting event. `:id` may be a submission ID or a Slurm-style single array-task selector such as `array_job_id_7`. Docker-backed jobs call Docker signal delivery. Kubernetes-backed jobs send the signal to non-PID-1 processes inside the runner container; `SIGKILL` also force-deletes the Pod afterward.
  - **Request Body / Query Parameters**: Accepts `{"signal": "USR1"}` or `?signal=USR1`. Empty signals default to `SIGTERM`; names are normalized to `SIG*` form.
  - **Response Fields**: Includes the normal job record plus `signal`.

#### `POST /slurm/scontrol/cancel/:id`, `POST /slurm/scancel/:id`, and `POST /slurm/scancel`

  - **Description**: Cancels queued, running, or suspended jobs. Path `:id` may be a submission ID, a bare Slurm array job ID to cancel all tasks in that array, or a Slurm-style array-task selector such as `array_job_id_7` or `array_job_id_[1,3]`. The native status becomes `Failed`, and the Slurm state view becomes `CANCELLED`.
  - **Bulk Request Body**: `POST /slurm/scancel` supports `job_id`, `job_ids`, `array_job_id`, `array_task_id`, `user`/`user_id`, `partition`/`cluster`, `name`/`job_name`, `state`, `account`, `qos`, and `signal`. State accepts the same Slurm short aliases as query filters. `job_id`/`job_ids` accept normal submission IDs and Slurm-style array-task selectors such as `array_job_id_7` or `array_job_id_[1,3-5]`.
  - **Bulk Query Parameters**: Supports the same selectors plus `signal`/`s`. Selector values such as `user`, `partition`, `account`, `qos`, `state`/`states`, and `job_name` accept comma, semicolon, or space separated lists. At least one selector is required to avoid accidentally cancelling or signaling every active job.
  - **Signal Mode**: When `signal` is set, `POST /slurm/scancel` sends that signal to matched running or suspended jobs and records `Signaled` accounting events instead of cancelling them. Queued matches fail per item because they have no runtime process to signal.
  - **Bulk Response Fields**: Includes `items`, `matched`, `cancelled`, `signaled`, and `failed`; each item includes `job_id`, `array_job_id`, `array_task_id`, `name`, `job_name`, `problem_id`, `user_id`, `partition`, `state`, `native_status`, `reason`, `cancelled`, `signaled`, and optional `signal`, `message`, or `error`.

-----

### Cluster & Container Management

#### `GET /clusters/status`

  - **Description**: Gets the current resource usage, queue lengths, and global license pool status for all configured clusters and nodes.

#### `GET /clusters/queue`

  - **Description**: Gets a scheduler queue snapshot similar to `squeue`, including queued/running submissions, job name, `slurm_state`, `slurm_reason`, cluster, node, QoS, account, TRES, billing units, computed priority, queue position, pending reason, and job-array metadata.

#### `GET /clusters/:clusterName/nodes/:nodeName`

  - **Description**: Gets detailed status for a specific node.

#### `POST /clusters/:clusterName/nodes/:nodeName/pause`

  - **Description**: Pauses a node, preventing it from accepting new judging tasks.

#### `POST /clusters/:clusterName/nodes/:nodeName/resume`

  - **Description**: Resumes a paused node.

#### `POST /clusters/:clusterName/nodes/:nodeName/drain`

  - **Description**: Marks a node as drained so it no longer receives new submissions.

#### `POST /clusters/:clusterName/nodes/:nodeName/down`

  - **Description**: Marks a node as down so it no longer receives new submissions.

#### `POST /clusters/:clusterName/nodes/:nodeName/undrain`

  - **Description**: Returns a drained/down node to `idle`.

#### `GET /containers`

  - **Description**: Gets a paginated list of all containers. Supports filtering by `submission_id`, `status`, and `user_query`.

#### `GET /containers/:id`

  - **Description**: Gets details for a single container.

-----

### WebSocket

#### `GET /ws/submissions/:id/containers/:conID/logs`

  - **Description**: Establishes a WebSocket connection to stream the complete log for any container. For finished containers, it streams the saved log file. For running containers, it first sends all historical logs from the cache and then continues to stream new logs in real-time. This is available regardless of the `show` flag.
  - **Authentication**: None.
