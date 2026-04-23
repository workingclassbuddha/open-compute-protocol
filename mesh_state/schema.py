from __future__ import annotations


BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS mesh_peers (
    peer_id TEXT PRIMARY KEY,
    display_name TEXT,
    public_key TEXT NOT NULL,
    signature_scheme TEXT DEFAULT '',
    endpoint_url TEXT DEFAULT '',
    stream_url TEXT DEFAULT '',
    trust_tier TEXT DEFAULT 'trusted',
    reachability TEXT DEFAULT 'direct',
    status TEXT DEFAULT 'known',
    mesh_session_id TEXT DEFAULT '',
    protocol_version TEXT DEFAULT '',
    capability_cards TEXT DEFAULT '[]',
    card TEXT DEFAULT '{}',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_handshake_at TEXT
);
CREATE TABLE IF NOT EXISTS mesh_seen_nonces (
    peer_id TEXT NOT NULL,
    nonce TEXT NOT NULL,
    route TEXT NOT NULL,
    request_id TEXT DEFAULT '',
    seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (peer_id, nonce)
);
CREATE TABLE IF NOT EXISTS mesh_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT UNIQUE,
    event_type TEXT NOT NULL,
    peer_id TEXT,
    request_id TEXT,
    payload TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_remote_events (
    peer_id TEXT NOT NULL,
    remote_seq INTEGER NOT NULL,
    event_id TEXT DEFAULT '',
    event_type TEXT NOT NULL,
    request_id TEXT DEFAULT '',
    payload TEXT DEFAULT '{}',
    remote_created_at TEXT DEFAULT '',
    synced_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (peer_id, remote_seq)
);
CREATE TABLE IF NOT EXISTS mesh_leases (
    id TEXT PRIMARY KEY,
    resource TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    agent_id TEXT DEFAULT '',
    job_id TEXT DEFAULT '',
    status TEXT DEFAULT 'active',
    ttl_seconds INTEGER DEFAULT 300,
    lock_token TEXT DEFAULT '',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    heartbeat_at TEXT DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT,
    released_at TEXT
);
CREATE TABLE IF NOT EXISTS mesh_artifacts (
    id TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    media_type TEXT DEFAULT 'application/octet-stream',
    size_bytes INTEGER DEFAULT 0,
    owner_peer_id TEXT NOT NULL,
    policy TEXT DEFAULT '{}',
    path TEXT NOT NULL,
    metadata TEXT DEFAULT '{}',
    retention_class TEXT DEFAULT 'durable',
    retention_deadline_at TEXT DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_secrets (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    name TEXT NOT NULL,
    value TEXT DEFAULT '',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scope, name)
);
CREATE TABLE IF NOT EXISTS mesh_jobs (
    id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    kind TEXT NOT NULL,
    origin_peer_id TEXT NOT NULL,
    target_peer_id TEXT NOT NULL,
    requirements TEXT DEFAULT '{}',
    policy TEXT DEFAULT '{}',
    payload_ref TEXT DEFAULT '{}',
    payload_inline TEXT DEFAULT '{}',
    artifact_inputs TEXT DEFAULT '[]',
    status TEXT DEFAULT 'accepted',
    result_ref TEXT DEFAULT '{}',
    lease_id TEXT DEFAULT '',
    executor TEXT DEFAULT '',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_handoffs (
    id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    from_peer_id TEXT NOT NULL,
    to_peer_id TEXT NOT NULL,
    from_agent TEXT DEFAULT '',
    to_agent TEXT DEFAULT '',
    summary TEXT NOT NULL,
    intent TEXT DEFAULT '',
    constraints TEXT DEFAULT '{}',
    artifact_refs TEXT DEFAULT '[]',
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_notifications (
    id TEXT PRIMARY KEY,
    notification_type TEXT DEFAULT 'info',
    priority TEXT DEFAULT 'normal',
    title TEXT NOT NULL,
    body TEXT DEFAULT '',
    compact_title TEXT DEFAULT '',
    compact_body TEXT DEFAULT '',
    status TEXT DEFAULT 'unread',
    target_peer_id TEXT DEFAULT '',
    target_agent_id TEXT DEFAULT '',
    target_device_classes TEXT DEFAULT '[]',
    related_job_id TEXT DEFAULT '',
    related_approval_id TEXT DEFAULT '',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    acked_at TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS mesh_approvals (
    id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    action_type TEXT DEFAULT 'operator_action',
    severity TEXT DEFAULT 'normal',
    title TEXT NOT NULL,
    summary TEXT DEFAULT '',
    compact_summary TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    requested_by_peer_id TEXT DEFAULT '',
    requested_by_agent_id TEXT DEFAULT '',
    target_peer_id TEXT DEFAULT '',
    target_agent_id TEXT DEFAULT '',
    target_device_classes TEXT DEFAULT '[]',
    related_job_id TEXT DEFAULT '',
    notification_id TEXT DEFAULT '',
    resolution TEXT DEFAULT '{}',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT DEFAULT '',
    resolved_at TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS mesh_treaties (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    summary TEXT DEFAULT '',
    treaty_type TEXT DEFAULT 'continuity',
    status TEXT DEFAULT 'draft',
    parties TEXT DEFAULT '[]',
    document TEXT DEFAULT '{}',
    metadata TEXT DEFAULT '{}',
    created_by_peer_id TEXT DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT DEFAULT '',
    ratified_at TEXT DEFAULT '',
    suspended_at TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS mesh_workers (
    id TEXT PRIMARY KEY,
    peer_id TEXT NOT NULL,
    agent_id TEXT DEFAULT '',
    status TEXT DEFAULT 'active',
    capabilities TEXT DEFAULT '[]',
    resources TEXT DEFAULT '{}',
    labels TEXT DEFAULT '[]',
    max_concurrent_jobs INTEGER DEFAULT 1,
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_discovery_candidates (
    base_url TEXT PRIMARY KEY,
    peer_id TEXT DEFAULT '',
    display_name TEXT DEFAULT '',
    endpoint_url TEXT DEFAULT '',
    status TEXT DEFAULT 'discovered',
    trust_tier TEXT DEFAULT 'trusted',
    device_profile TEXT DEFAULT '{}',
    manifest TEXT DEFAULT '{}',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_error TEXT DEFAULT '',
    last_error_at TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS mesh_cooperative_tasks (
    id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    name TEXT DEFAULT '',
    strategy TEXT DEFAULT 'spread',
    base_job TEXT DEFAULT '{}',
    shard_count INTEGER DEFAULT 0,
    shard_jobs TEXT DEFAULT '[]',
    target_peers TEXT DEFAULT '[]',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_missions (
    id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    title TEXT DEFAULT '',
    intent TEXT DEFAULT '',
    status TEXT DEFAULT 'planned',
    priority TEXT DEFAULT 'normal',
    workload_class TEXT DEFAULT 'default',
    origin_peer_id TEXT NOT NULL,
    target_strategy TEXT DEFAULT 'local',
    policy TEXT DEFAULT '{}',
    continuity TEXT DEFAULT '{}',
    metadata TEXT DEFAULT '{}',
    child_job_ids TEXT DEFAULT '[]',
    cooperative_task_ids TEXT DEFAULT '[]',
    latest_checkpoint_ref TEXT DEFAULT '{}',
    result_ref TEXT DEFAULT '{}',
    result_bundle_ref TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_job_attempts (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    status TEXT DEFAULT 'running',
    lease_id TEXT DEFAULT '',
    executor TEXT DEFAULT '',
    result_ref TEXT DEFAULT '{}',
    error TEXT DEFAULT '',
    metadata TEXT DEFAULT '{}',
    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
    heartbeat_at TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT DEFAULT '',
    UNIQUE(job_id, attempt_number)
);
CREATE TABLE IF NOT EXISTS mesh_queue_messages (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE,
    queue_name TEXT DEFAULT 'default',
    status TEXT DEFAULT 'queued',
    dedupe_key TEXT DEFAULT '',
    ack_deadline_seconds INTEGER DEFAULT 300,
    dead_letter_queue TEXT DEFAULT '',
    delivery_attempts INTEGER DEFAULT 0,
    visibility_timeout_at TEXT DEFAULT '',
    available_at TEXT DEFAULT CURRENT_TIMESTAMP,
    claimed_at TEXT DEFAULT '',
    acked_at TEXT DEFAULT '',
    replay_deadline_at TEXT DEFAULT '',
    retention_deadline_at TEXT DEFAULT '',
    lease_id TEXT DEFAULT '',
    worker_id TEXT DEFAULT '',
    current_attempt_id TEXT DEFAULT '',
    last_error TEXT DEFAULT '',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_scheduler_decisions (
    id TEXT PRIMARY KEY,
    request_id TEXT DEFAULT '',
    job_id TEXT DEFAULT '',
    job_kind TEXT DEFAULT '',
    status TEXT DEFAULT 'placed',
    strategy TEXT DEFAULT '',
    target_type TEXT DEFAULT '',
    peer_id TEXT DEFAULT '',
    score INTEGER DEFAULT 0,
    placement TEXT DEFAULT '{}',
    selected TEXT DEFAULT '{}',
    candidates TEXT DEFAULT '[]',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mesh_offload_preferences (
    peer_id TEXT NOT NULL,
    workload_class TEXT NOT NULL,
    preference TEXT DEFAULT 'allow',
    source TEXT DEFAULT 'operator',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (peer_id, workload_class)
);
CREATE TABLE IF NOT EXISTS mesh_autonomy_runs (
    id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    mode TEXT DEFAULT 'assisted',
    status TEXT DEFAULT 'planned',
    summary TEXT DEFAULT '',
    actions TEXT DEFAULT '[]',
    result TEXT DEFAULT '{}',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_mesh_events_created ON mesh_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_remote_events_peer_created ON mesh_remote_events(peer_id, remote_seq DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_leases_peer_status ON mesh_leases(peer_id, status);
CREATE INDEX IF NOT EXISTS idx_mesh_jobs_status ON mesh_jobs(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_artifacts_digest ON mesh_artifacts(digest);
CREATE INDEX IF NOT EXISTS idx_mesh_secrets_scope_name ON mesh_secrets(scope, name);
CREATE INDEX IF NOT EXISTS idx_mesh_notifications_target_status ON mesh_notifications(target_peer_id, target_agent_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_approvals_target_status ON mesh_approvals(target_peer_id, target_agent_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_workers_status ON mesh_workers(status, last_heartbeat_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_discovery_candidates_status ON mesh_discovery_candidates(status, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_cooperative_tasks_created ON mesh_cooperative_tasks(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_missions_updated ON mesh_missions(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_job_attempts_job ON mesh_job_attempts(job_id, attempt_number DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_job_attempts_worker_status ON mesh_job_attempts(worker_id, status);
CREATE INDEX IF NOT EXISTS idx_mesh_queue_messages_status ON mesh_queue_messages(status, available_at ASC, updated_at ASC);
CREATE INDEX IF NOT EXISTS idx_mesh_queue_messages_dedupe ON mesh_queue_messages(dedupe_key, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_scheduler_decisions_created ON mesh_scheduler_decisions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_offload_preferences_updated ON mesh_offload_preferences(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_mesh_autonomy_runs_created ON mesh_autonomy_runs(created_at DESC);
"""


def initialize_mesh_schema(conn) -> None:
    conn.executescript(BASE_SCHEMA)

    queue_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(mesh_queue_messages)").fetchall()
    }
    queue_column_defs = {
        "ack_deadline_seconds": "INTEGER DEFAULT 300",
        "dead_letter_queue": "TEXT DEFAULT ''",
        "replay_deadline_at": "TEXT DEFAULT ''",
        "retention_deadline_at": "TEXT DEFAULT ''",
    }
    for column_name, column_def in queue_column_defs.items():
        if column_name not in queue_columns:
            conn.execute(f"ALTER TABLE mesh_queue_messages ADD COLUMN {column_name} {column_def}")

    artifact_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(mesh_artifacts)").fetchall()
    }
    artifact_column_defs = {
        "retention_class": "TEXT DEFAULT 'durable'",
        "retention_deadline_at": "TEXT DEFAULT ''",
    }
    for column_name, column_def in artifact_column_defs.items():
        if column_name not in artifact_columns:
            conn.execute(f"ALTER TABLE mesh_artifacts ADD COLUMN {column_name} {column_def}")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mesh_artifacts_retention "
        "ON mesh_artifacts(retention_deadline_at, created_at DESC)"
    )
    conn.commit()
