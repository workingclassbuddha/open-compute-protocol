"""
Standalone local runtime primitives for the OCP reference project.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _utcnow_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def _utcnow() -> str:
    return _utcnow_dt().isoformat().replace("+00:00", "Z")


def _loads_json(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


class _ManagedConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc, tb):
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


class OCPStore:
    """Small sqlite-backed local substrate for standalone Sovereign Mesh."""

    def __init__(self, *, db_path: str):
        self.db_path = str(Path(db_path).expanduser())
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._event_log: list[dict[str, Any]] = []
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level="DEFERRED",
            factory=_ManagedConnection,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS personal_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT UNIQUE,
                    event_type TEXT NOT NULL,
                    text TEXT DEFAULT '',
                    source TEXT DEFAULT 'ocp',
                    payload TEXT DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS agent_registrations (
                    agent_id TEXT PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    agent_type TEXT DEFAULT 'ai',
                    runtime TEXT DEFAULT '',
                    model_version TEXT DEFAULT 'unknown-model',
                    role TEXT DEFAULT '',
                    scope TEXT DEFAULT '',
                    interface TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    capabilities TEXT DEFAULT '[]',
                    handoff_formats TEXT DEFAULT '[]',
                    permissions TEXT DEFAULT '[]',
                    memory_boundary TEXT DEFAULT '',
                    verification_responsibilities TEXT DEFAULT '[]',
                    approval_scope TEXT DEFAULT 'yellow',
                    context_mode TEXT DEFAULT '',
                    metadata TEXT DEFAULT '{}',
                    status TEXT DEFAULT 'joined',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS agent_sessions (
                    id TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL,
                    runtime TEXT DEFAULT '',
                    status TEXT DEFAULT 'active',
                    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_heartbeat_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    ended_at TEXT,
                    current_task TEXT DEFAULT '',
                    current_project TEXT DEFAULT '',
                    summary TEXT DEFAULT '',
                    metadata TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS ocp_beacons (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    domain TEXT DEFAULT 'swarm',
                    agent_id TEXT DEFAULT '',
                    agent_name TEXT DEFAULT '',
                    energy REAL DEFAULT 0.96,
                    metadata TEXT DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_agent_registrations_seen
                    ON agent_registrations(last_seen_at DESC, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_agent_sessions_active
                    ON agent_sessions(status, last_heartbeat_at DESC);
                CREATE INDEX IF NOT EXISTS idx_ocp_beacons_created
                    ON ocp_beacons(created_at DESC);

                CREATE TABLE IF NOT EXISTS handoff_packets (
                    id TEXT PRIMARY KEY,
                    from_agent TEXT DEFAULT '',
                    to_agent TEXT DEFAULT '',
                    project_id TEXT DEFAULT '',
                    objective TEXT DEFAULT '',
                    context TEXT DEFAULT '',
                    resource_refs TEXT DEFAULT '[]',
                    approval_state TEXT DEFAULT 'clear',
                    status TEXT DEFAULT 'pending',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            conn.commit()

    def log_event(self, event_type: str, message: str, **kwargs) -> None:
        entry = {"type": event_type, "message": message, "ts": _utcnow(), **kwargs}
        self._event_log.append(entry)
        if len(self._event_log) > 200:
            self._event_log = self._event_log[-100:]
        try:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO personal_events (id, event_type, text, source, payload, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        event_type,
                        str(message or "")[:500],
                        str(kwargs.get("source") or "ocp"),
                        json.dumps({k: v for k, v in kwargs.items() if k != "source"}),
                        _utcnow(),
                    ),
                )
                conn.commit()
        except Exception:
            logger.debug("ocp store failed to persist event", exc_info=True)

    def register_agent(
        self,
        agent_id: str,
        agent_name: str = "",
        capabilities: Optional[list] = None,
        description: str = "",
        agent_type: str = "ai",
        model_version: str = "unknown-model",
        metadata: Optional[dict] = None,
    ) -> dict:
        agent_id = (agent_id or "").strip()
        if not agent_id:
            raise ValueError("agent_id is required")
        capabilities = [str(item).strip() for item in (capabilities or []) if str(item).strip()]
        metadata = dict(metadata or {})
        now = _utcnow()
        agent_name = (agent_name or metadata.get("agent_name") or agent_id).strip() or agent_id
        row = {
            "agent_id": agent_id,
            "agent_name": agent_name,
            "agent_type": str(agent_type or "ai").strip() or "ai",
            "runtime": str(metadata.get("runtime") or "").strip(),
            "model_version": str(metadata.get("model_version") or model_version or "unknown-model").strip() or "unknown-model",
            "role": str(metadata.get("role") or "").strip(),
            "scope": str(metadata.get("scope") or "").strip(),
            "interface": str(metadata.get("interface") or "").strip(),
            "description": str(description or "").strip(),
            "capabilities": capabilities,
            "handoff_formats": [str(item).strip() for item in (metadata.get("handoff_formats") or []) if str(item).strip()],
            "permissions": [str(item).strip() for item in (metadata.get("permissions") or []) if str(item).strip()],
            "memory_boundary": str(metadata.get("memory_boundary") or "").strip(),
            "verification_responsibilities": [
                str(item).strip()
                for item in (metadata.get("verification_responsibilities") or [])
                if str(item).strip()
            ],
            "approval_scope": str(metadata.get("approval_scope") or "yellow").strip() or "yellow",
            "context_mode": str(metadata.get("context_mode") or "").strip(),
            "metadata": metadata,
            "status": str(metadata.get("status") or "joined").strip() or "joined",
        }
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO agent_registrations
                (agent_id, agent_name, agent_type, runtime, model_version, role, scope, interface, description,
                 capabilities, handoff_formats, permissions, memory_boundary, verification_responsibilities,
                 approval_scope, context_mode, metadata, status, created_at, updated_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(agent_id) DO UPDATE SET
                    agent_name=excluded.agent_name,
                    agent_type=excluded.agent_type,
                    runtime=excluded.runtime,
                    model_version=excluded.model_version,
                    role=excluded.role,
                    scope=excluded.scope,
                    interface=excluded.interface,
                    description=excluded.description,
                    capabilities=excluded.capabilities,
                    handoff_formats=excluded.handoff_formats,
                    permissions=excluded.permissions,
                    memory_boundary=excluded.memory_boundary,
                    verification_responsibilities=excluded.verification_responsibilities,
                    approval_scope=excluded.approval_scope,
                    context_mode=excluded.context_mode,
                    metadata=excluded.metadata,
                    status=excluded.status,
                    updated_at=excluded.updated_at,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    row["agent_id"],
                    row["agent_name"],
                    row["agent_type"],
                    row["runtime"],
                    row["model_version"],
                    row["role"],
                    row["scope"],
                    row["interface"],
                    row["description"],
                    json.dumps(row["capabilities"]),
                    json.dumps(row["handoff_formats"]),
                    json.dumps(row["permissions"]),
                    row["memory_boundary"],
                    json.dumps(row["verification_responsibilities"]),
                    row["approval_scope"],
                    row["context_mode"],
                    json.dumps(row["metadata"]),
                    row["status"],
                    now,
                    now,
                    now,
                ),
            )
            conn.commit()
        return self.get_agent_registration(agent_id) or row

    def get_agent_registration(self, agent_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM agent_registrations WHERE agent_id=?",
                ((agent_id or "").strip(),),
            ).fetchone()
        return self._row_to_agent(row) if row else {}

    def heartbeat_agent_session(
        self,
        session_id: str,
        *,
        agent_id: str,
        runtime: str = "",
        current_task: str = "",
        current_project: str = "",
        metadata: Optional[dict] = None,
        status: str = "active",
        summary: str = "",
    ) -> dict:
        session_id = (session_id or "").strip()
        agent_id = (agent_id or "").strip()
        if not session_id or not agent_id:
            raise ValueError("session_id and agent_id are required")
        now = _utcnow()
        payload = dict(metadata or {})
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO agent_sessions
                (id, agent_id, runtime, status, started_at, last_heartbeat_at, ended_at, current_task, current_project, summary, metadata)
                VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    agent_id=excluded.agent_id,
                    runtime=excluded.runtime,
                    status=excluded.status,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    ended_at=CASE WHEN excluded.status='active' THEN NULL ELSE agent_sessions.ended_at END,
                    current_task=excluded.current_task,
                    current_project=excluded.current_project,
                    summary=CASE WHEN excluded.summary != '' THEN excluded.summary ELSE agent_sessions.summary END,
                    metadata=excluded.metadata
                """,
                (
                    session_id,
                    agent_id,
                    runtime,
                    status,
                    now,
                    now,
                    current_task,
                    current_project,
                    summary,
                    json.dumps(payload),
                ),
            )
            conn.execute(
                """
                UPDATE agent_registrations
                SET runtime=?, status=?, updated_at=?, last_seen_at=?
                WHERE agent_id=?
                """,
                (runtime, "active" if status == "active" else status, now, now, agent_id),
            )
            conn.commit()
        return self.get_agent_session(session_id)

    def get_agent_session(self, session_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM agent_sessions WHERE id=?",
                ((session_id or "").strip(),),
            ).fetchone()
        return self._row_to_session(row) if row else {}

    def prune_stale_agent_sessions(self) -> None:
        return None

    def list_agent_registrations(self, *, limit: int = 100, include_sessions: bool = True) -> list[dict]:
        self.prune_stale_agent_sessions()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM agent_registrations
                ORDER BY last_seen_at DESC, updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            session_rows = []
            if include_sessions:
                session_rows = conn.execute(
                    """
                    SELECT *
                    FROM agent_sessions
                    WHERE status='active'
                    ORDER BY last_heartbeat_at DESC, started_at DESC
                    """
                ).fetchall()
        active_sessions: dict[str, dict[str, Any]] = {}
        for row in session_rows:
            if row["agent_id"] in active_sessions:
                continue
            active_sessions[row["agent_id"]] = self._row_to_session(row)
        agents = []
        for row in rows:
            agent = self._row_to_agent(row)
            agent["active_session"] = active_sessions.get(agent["agent_id"])
            agent["contract_ready"] = bool(agent["role"] and (agent["capabilities"] or agent["permissions"]))
            agents.append(agent)
        return agents

    def store_beacon(
        self,
        text: str,
        *,
        agent_id: str = "",
        agent_name: str = "",
        domain: str = "swarm",
        energy: float = 0.96,
        metadata: Optional[dict] = None,
    ) -> dict:
        beacon_id = str(uuid.uuid4())
        payload = dict(metadata or {})
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO ocp_beacons (id, text, domain, agent_id, agent_name, energy, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    beacon_id,
                    str(text or "").strip(),
                    str(domain or "swarm").strip() or "swarm",
                    str(agent_id or "").strip(),
                    str(agent_name or "").strip(),
                    max(0.0, min(1.0, float(energy))),
                    json.dumps(payload),
                    _utcnow(),
                ),
            )
            row = conn.execute("SELECT * FROM ocp_beacons WHERE id=?", (beacon_id,)).fetchone()
            conn.commit()
        return self._row_to_beacon(row)

    def get_beacons(self, *, limit: int = 10) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM ocp_beacons
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [self._row_to_beacon(row) for row in rows]

    def _row_to_agent(self, row) -> dict:
        if not row:
            return {}
        metadata = _loads_json(row["metadata"], {})
        return {
            "agent_id": row["agent_id"],
            "agent_name": row["agent_name"],
            "agent_type": row["agent_type"] or "ai",
            "runtime": row["runtime"] or "",
            "model_version": row["model_version"] or "unknown-model",
            "role": row["role"] or metadata.get("role") or "",
            "scope": row["scope"] or metadata.get("scope") or "",
            "permissions": _loads_json(row["permissions"], []),
            "memory_boundary": row["memory_boundary"] or metadata.get("memory_boundary") or "",
            "verification_responsibilities": _loads_json(row["verification_responsibilities"], []),
            "approval_scope": row["approval_scope"] or "yellow",
            "context_mode": row["context_mode"] or "",
            "interface": row["interface"] or "",
            "description": row["description"] or "",
            "capabilities": _loads_json(row["capabilities"], []),
            "handoff_formats": _loads_json(row["handoff_formats"], []),
            "metadata": metadata,
            "status": row["status"] or "joined",
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_seen_at": row["last_seen_at"],
        }

    def _row_to_session(self, row) -> dict:
        if not row:
            return {}
        return {
            "id": row["id"],
            "agent_id": row["agent_id"],
            "runtime": row["runtime"] or "",
            "status": row["status"] or "active",
            "started_at": row["started_at"],
            "last_heartbeat_at": row["last_heartbeat_at"],
            "ended_at": row["ended_at"],
            "current_task": row["current_task"] or "",
            "current_project": row["current_project"] or "",
            "summary": row["summary"] or "",
            "metadata": _loads_json(row["metadata"], {}),
        }

    def _row_to_beacon(self, row) -> dict:
        if not row:
            return {}
        return {
            "id": row["id"],
            "text": row["text"],
            "domain": row["domain"] or "swarm",
            "agent_id": row["agent_id"] or "",
            "agent_name": row["agent_name"] or "",
            "energy": float(row["energy"] or 0.0),
            "metadata": _loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
        }


class OCPRegistry:
    """Minimal lock, ledger, and beacon layer for standalone OCP."""

    def __init__(self, lattice: OCPStore):
        self.lattice = lattice
        self._init_db()

    def _init_db(self) -> None:
        with self.lattice._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS ocp_registry_locks (
                    resource TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL,
                    agent_name TEXT DEFAULT '',
                    session_id TEXT DEFAULT '',
                    lock_token TEXT DEFAULT '',
                    lock_type TEXT DEFAULT 'task',
                    reason TEXT DEFAULT '',
                    metadata TEXT DEFAULT '{}',
                    locked_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    heartbeat_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    expires_at TEXT
                );

                CREATE TABLE IF NOT EXISTS ocp_registry_ledger (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT UNIQUE,
                    agent_id TEXT NOT NULL,
                    agent_name TEXT DEFAULT '',
                    action TEXT NOT NULL,
                    resource TEXT DEFAULT '',
                    status TEXT DEFAULT 'ok',
                    details TEXT DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_ocp_registry_locks_agent
                    ON ocp_registry_locks(agent_id);
                CREATE INDEX IF NOT EXISTS idx_ocp_registry_locks_expiry
                    ON ocp_registry_locks(expires_at);
                CREATE INDEX IF NOT EXISTS idx_ocp_registry_ledger_created
                    ON ocp_registry_ledger(created_at DESC);
                """
            )
            conn.commit()

    def acquire_lock(
        self,
        resource: str,
        *,
        agent_id: str,
        agent_name: Optional[str] = None,
        session_id: Optional[str] = None,
        reason: str = "",
        ttl_seconds: int = 900,
        lock_type: str = "task",
        metadata: Optional[dict] = None,
    ) -> dict:
        resource = (resource or "").strip()
        agent_id = (agent_id or "").strip()
        if not resource or not agent_id:
            raise ValueError("resource and agent_id are required")
        metadata = dict(metadata or {})
        lock_token = str(metadata.get("lock_token") or uuid.uuid4())
        now = _utcnow()
        expires_at = (_utcnow_dt() + dt.timedelta(seconds=max(60, int(ttl_seconds)))).isoformat().replace("+00:00", "Z")
        with self.lattice._conn() as conn:
            self._prune_expired_locks(conn, now)
            existing = conn.execute(
                "SELECT * FROM ocp_registry_locks WHERE resource=?",
                (resource,),
            ).fetchone()
            if existing and existing["agent_id"] != agent_id:
                lock = self._row_to_lock(existing)
                self.log_action(
                    "lock.conflict",
                    agent_id=agent_id,
                    agent_name=agent_name,
                    resource=resource,
                    status="conflict",
                    details={"held_by": lock},
                )
                return {"status": "conflict", "lock": lock}
            if existing:
                conn.execute(
                    """
                    UPDATE ocp_registry_locks
                    SET agent_name=?, session_id=?, lock_token=?, lock_type=?, reason=?, metadata=?, heartbeat_at=?, expires_at=?
                    WHERE resource=?
                    """,
                    (
                        agent_name or "",
                        session_id or "",
                        lock_token,
                        lock_type,
                        reason,
                        json.dumps(metadata),
                        now,
                        expires_at,
                        resource,
                    ),
                )
                action = "lock.renewed"
            else:
                conn.execute(
                    """
                    INSERT INTO ocp_registry_locks
                    (resource, agent_id, agent_name, session_id, lock_token, lock_type, reason, metadata, locked_at, heartbeat_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        resource,
                        agent_id,
                        agent_name or "",
                        session_id or "",
                        lock_token,
                        lock_type,
                        reason,
                        json.dumps(metadata),
                        now,
                        now,
                        expires_at,
                    ),
                )
                action = "lock.acquired"
            row = conn.execute(
                "SELECT * FROM ocp_registry_locks WHERE resource=?",
                (resource,),
            ).fetchone()
            conn.commit()
        self.log_action(
            action,
            agent_id=agent_id,
            agent_name=agent_name,
            resource=resource,
            details={"reason": reason, "lock_type": lock_type, "ttl_seconds": ttl_seconds, "session_id": session_id, "metadata": metadata},
        )
        return {"status": "ok", "lock": self._row_to_lock(row)}

    def heartbeat_lock(
        self,
        resource: str,
        *,
        agent_id: Optional[str] = None,
        session_id: Optional[str] = None,
        lock_token: Optional[str] = None,
        ttl_seconds: int = 900,
        metadata: Optional[dict] = None,
    ) -> dict:
        resource = (resource or "").strip()
        if not resource:
            raise ValueError("resource is required")
        now = _utcnow()
        expires_at = (_utcnow_dt() + dt.timedelta(seconds=max(60, int(ttl_seconds)))).isoformat().replace("+00:00", "Z")
        with self.lattice._conn() as conn:
            self._prune_expired_locks(conn, now)
            row = conn.execute(
                "SELECT * FROM ocp_registry_locks WHERE resource=?",
                (resource,),
            ).fetchone()
            if not row:
                return {"status": "not_found", "resource": resource}
            if not self._lock_owned_by(row, agent_id=agent_id, session_id=session_id, lock_token=lock_token):
                return {"status": "forbidden", "resource": resource, "lock": self._row_to_lock(row)}
            merged_metadata = _loads_json(row["metadata"], {})
            merged_metadata.update(dict(metadata or {}))
            conn.execute(
                """
                UPDATE ocp_registry_locks
                SET heartbeat_at=?, expires_at=?, metadata=?
                WHERE resource=?
                """,
                (now, expires_at, json.dumps(merged_metadata), resource),
            )
            conn.commit()
            fresh = conn.execute(
                "SELECT * FROM ocp_registry_locks WHERE resource=?",
                (resource,),
            ).fetchone()
        self.log_action(
            "lock.heartbeat",
            agent_id=agent_id or row["agent_id"],
            agent_name=row["agent_name"],
            resource=resource,
            details={"session_id": session_id or row["session_id"], "ttl_seconds": ttl_seconds},
        )
        return {"status": "ok", "lock": self._row_to_lock(fresh)}

    def release_lock(
        self,
        resource: str,
        *,
        agent_id: Optional[str] = None,
        session_id: Optional[str] = None,
        lock_token: Optional[str] = None,
        force: bool = False,
    ) -> dict:
        resource = (resource or "").strip()
        if not resource:
            raise ValueError("resource is required")
        with self.lattice._conn() as conn:
            row = conn.execute(
                "SELECT * FROM ocp_registry_locks WHERE resource=?",
                (resource,),
            ).fetchone()
            if not row:
                return {"status": "not_found", "resource": resource}
            if not force and not self._lock_owned_by(row, agent_id=agent_id, session_id=session_id, lock_token=lock_token):
                return {"status": "forbidden", "resource": resource, "lock": self._row_to_lock(row)}
            conn.execute("DELETE FROM ocp_registry_locks WHERE resource=?", (resource,))
            conn.commit()
        self.log_action(
            "lock.released",
            agent_id=agent_id or row["agent_id"],
            agent_name=row["agent_name"],
            resource=resource,
            details={"force": force},
        )
        return {"status": "released", "lock": self._row_to_lock(row)}

    def emit_beacon(
        self,
        text: str,
        *,
        agent_id: str,
        agent_name: Optional[str] = None,
        domain: str = "swarm",
        energy: float = 0.96,
        metadata: Optional[dict] = None,
    ) -> dict:
        node = self.lattice.store_beacon(
            text,
            agent_id=agent_id,
            agent_name=agent_name or "",
            domain=domain,
            energy=energy,
            metadata=metadata,
        )
        self.log_action(
            "beacon.emitted",
            agent_id=agent_id,
            agent_name=agent_name,
            resource=node["id"],
            details={"text": node["text"], "domain": domain, "metadata": metadata or {}},
        )
        return {"status": "ok", "node": node}

    def get_beacons(self, limit: int = 10) -> list[dict]:
        return self.lattice.get_beacons(limit=limit)

    def log_action(
        self,
        action: str,
        *,
        agent_id: str,
        agent_name: Optional[str] = None,
        resource: Optional[str] = None,
        status: str = "ok",
        details: Optional[dict] = None,
    ) -> dict:
        entry_id = str(uuid.uuid4())
        payload = dict(details or {})
        with self.lattice._conn() as conn:
            conn.execute(
                """
                INSERT INTO ocp_registry_ledger (id, agent_id, agent_name, action, resource, status, details)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (entry_id, agent_id, agent_name or "", action, resource or "", status, json.dumps(payload)),
            )
            row = conn.execute(
                "SELECT * FROM ocp_registry_ledger WHERE id=?",
                (entry_id,),
            ).fetchone()
            conn.commit()
        self.lattice.log_event(
            "registry",
            f"{action} · {resource or agent_id}",
            source=agent_name or agent_id,
            payload={"agent_id": agent_id, "action": action, "resource": resource, "status": status, "details": payload},
        )
        return self._row_to_ledger(row)

    def _prune_expired_locks(self, conn, now: str) -> None:
        conn.execute(
            """
            DELETE FROM ocp_registry_locks
            WHERE expires_at IS NOT NULL
              AND expires_at != ''
              AND expires_at <= ?
            """,
            (now,),
        )

    def _lock_owned_by(
        self,
        row,
        *,
        agent_id: Optional[str] = None,
        session_id: Optional[str] = None,
        lock_token: Optional[str] = None,
    ) -> bool:
        if lock_token and str(row["lock_token"] or "").strip() == str(lock_token).strip():
            return True
        if session_id and str(row["session_id"] or "").strip() == str(session_id).strip():
            return True
        if agent_id and str(row["agent_id"] or "").strip() == str(agent_id).strip():
            return True
        return False

    def _row_to_lock(self, row) -> dict:
        if not row:
            return {}
        return {
            "resource": row["resource"],
            "agent_id": row["agent_id"],
            "agent_name": row["agent_name"] or "",
            "session_id": row["session_id"] or "",
            "lock_token": row["lock_token"] or "",
            "lock_type": row["lock_type"] or "task",
            "reason": row["reason"] or "",
            "metadata": _loads_json(row["metadata"], {}),
            "locked_at": row["locked_at"],
            "heartbeat_at": row["heartbeat_at"],
            "expires_at": row["expires_at"],
        }

    def _row_to_ledger(self, row) -> dict:
        if not row:
            return {}
        return {
            "seq": int(row["seq"]),
            "id": row["id"],
            "agent_id": row["agent_id"],
            "agent_name": row["agent_name"] or "",
            "action": row["action"],
            "resource": row["resource"] or "",
            "status": row["status"] or "ok",
            "details": _loads_json(row["details"], {}),
            "created_at": row["created_at"],
        }


PersonalLattice = OCPStore
HiveRegistry = OCPRegistry

__all__ = [
    "HiveRegistry",
    "OCPRegistry",
    "OCPStore",
    "PersonalLattice",
]
