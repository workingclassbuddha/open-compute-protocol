# OCP v0.2 Durable Execution Alpha

Status: implementation note  
Date: 2026-04-17

## Purpose

This milestone adds the first real execution plane to OCP without breaking the current v0.1 control-plane behavior.

## Scope

In scope:

- queued job dispatch mode
- worker registration
- worker heartbeats
- job attempts
- lease-backed claims
- shell executor
- Python executor
- artifactized results

Out of scope:

- duplex peer sessions
- Docker executor
- Wasm executor
- distributed scheduler
- signed attestations

## Backwards Compatibility

Existing v0.1 bounded jobs remain valid:

- inline jobs still work
- handshakes, artifacts, leases, and sync remain unchanged
- queued execution is additive

## New Runtime Concepts

### Worker

Registered local or remote executor with:

- worker id
- status
- capabilities
- resources
- labels
- concurrency

### Job Attempt

Per-claim execution record with:

- attempt id
- attempt number
- worker id
- status
- lease id
- heartbeat
- error or result

### Queued Job

Any job with `dispatch_mode=queued` or a generic runtime such as `shell.command` or `python.inline`.

## Status Model

Job statuses added or emphasized:

- `queued`
- `running`
- `completed`
- `failed`

Attempt statuses:

- `claimed`
- `running`
- `completed`
- `failed`

## Retry Model

- retries are controlled by `metadata.retry_policy.max_attempts`
- failed attempts release their lease
- retryable failures requeue the job until `max_attempts` is reached

## Executors

### Shell

`kind = shell.command`

Payload:

- `command`: argv list or shell string
- `cwd`: optional, must stay inside `workspace_root`
- `env`: optional
- `timeout_seconds`: optional

Output bundle:

- `stdout`
- `stderr`
- `exit_code`
- `cwd`
- `argv`

### Python

`kind = python.inline`

Payload:

- `code`
- `args`
- `cwd`
- `env`
- `timeout_seconds`

Implementation note:

- current alpha runs this through `sys.executable -c`

## Persistence

New tables:

- `mesh_workers`
- `mesh_job_attempts`

Job retries and dispatch mode are stored in job metadata for now.

## Immediate Follow-Up

After this alpha lands:

1. add worker HTTP endpoints
2. add scheduler decision records
3. add cancellation and expiry tests for queued jobs
4. add Docker executor behind capability checks
