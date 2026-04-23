"""
server.py — Shared Personal Golem HTTP server (RE-INJECTED).
"""

from __future__ import annotations

import argparse
import base64
import errno
import hashlib
import ipaddress
import json
import logging
import mimetypes
import os
import subprocess
import time
import urllib.request as _ur
import uuid as _uuid
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
PM_DIR = Path(__file__).parent
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse
import importlib

from obsidian_runtime import load_obsidian_snapshot

# Load .env for API keys.
_env_path = Path(__file__).parent / '.env'
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

logger = logging.getLogger(__name__)
_HERE = Path(__file__).parent
_PROCESS_STARTED_AT = time.time()
RUNTIME_CONTRACT_VERSION = "pm-runtime-contract/v1"
RUNTIME_BUILD_VERSION = (
    os.environ.get("PERSONAL_MIRROR_BUILD_VERSION")
    or os.environ.get("PM_BUILD_VERSION")
    or "local-dev"
).strip()
RUNTIME_BUILD_STAMP = (
    os.environ.get("PERSONAL_MIRROR_BUILD_STAMP")
    or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(_HERE.stat().st_mtime))
).strip()

API_ROUTE_MANIFEST = (
    {
        "method": "GET",
        "path": "/status",
        "description": "Organism state, verification health, mention health, memory tiers, metabolism health",
        "handler": "_send_status",
    },
    {
        "method": "GET",
        "path": "/runtime/contract",
        "description": "Runtime contract, build stamp, route manifest, auth mode, and subsystem truth surface",
        "handler": "_handle_runtime_contract",
    },
    {
        "method": "GET",
        "path": "/autonomy/status",
        "description": "Autonomy pipeline health across lattice, dream, metabolism, vessel, and oscillators",
        "handler": "_handle_autonomy_status",
    },
    {
        "method": "GET",
        "path": "/memory/auto-inject",
        "description": "Shared context for agents, including resolved thought handles when a query is present",
        "handler": "_handle_auto_inject",
    },
    {
        "method": "GET",
        "path": "/self-model",
        "description": "Inspectable self-model nodes such as parts, values, commitments, and absences",
        "handler": "_handle_self_model",
    },
    {
        "method": "GET",
        "path": "/world-model",
        "description": "Inspectable world-state snapshot with current focus, context shifts, and promise pressure",
        "handler": "_handle_world_model",
    },
    {
        "method": "GET",
        "path": "/promises",
        "description": "Inspect tracked promises and which ones currently look at risk",
        "handler": "_handle_promises",
    },
    {
        "method": "GET",
        "path": "/experiments",
        "description": "Inspect active experiments, review timing, and recorded outcomes",
        "handler": "_handle_experiments",
    },
    {
        "method": "POST",
        "path": "/self-model/store",
        "description": "Store a structured self-model node in the personal lattice",
        "handler": "_handle_self_model_store",
    },
    {
        "method": "POST",
        "path": "/promises/store",
        "description": "Store a tracked promise in the personal lattice",
        "handler": "_handle_promises_store",
    },
    {
        "method": "POST",
        "path": "/experiments/start",
        "description": "Start a bounded experiment with hypothesis, trigger, smallest test, and review date",
        "handler": "_handle_experiments_start",
    },
    {
        "method": "POST",
        "path": "/experiments/close",
        "description": "Close an experiment with an outcome and captured learning",
        "handler": "_handle_experiments_close",
    },
    {
        "method": "GET",
        "path": "/coherence/diagnosis",
        "description": "Compositional coherence diagnosis with drift, contradiction, loop, and absence factors",
        "handler": "_handle_coherence_diagnosis",
    },
    {
        "method": "GET",
        "path": "/continuity",
        "description": "Evidence-backed continuity brief with active continuations, tensions, last intent, and next step",
        "handler": "_handle_continuity",
    },
    {
        "method": "POST",
        "path": "/continuity/feedback",
        "description": "Record whether continuity guidance was accepted, deferred, rejected, reframed, or later followed through",
        "handler": "_handle_continuity_feedback",
    },
    {
        "method": "GET",
        "path": "/noticing",
        "description": "Inspect recent silent noticing and intervention nodes",
        "handler": "_handle_noticing",
    },
    {
        "method": "GET",
        "path": "/trajectory",
        "description": "Generate an evidence-backed future-self forecast for the current project",
        "handler": "_handle_trajectory",
    },
    {
        "method": "GET",
        "path": "/rituals/suggest",
        "description": "Suggest the most relevant developmental ritual for the current state",
        "handler": "_handle_ritual_suggest",
    },
    {
        "method": "POST",
        "path": "/rituals/run",
        "description": "Run a developmental ritual such as the weekly truth audit, morning wake, or future-self council",
        "handler": "_handle_run_ritual",
    },
    {
        "method": "POST",
        "path": "/noticing/scan",
        "description": "Run a silent noticing scan for drifting commitments",
        "handler": "_handle_noticing_scan",
    },
    {
        "method": "POST",
        "path": "/trajectory/refresh",
        "description": "Generate and persist a fresh trajectory snapshot",
        "handler": "_handle_trajectory_refresh",
    },
    {
        "method": "POST",
        "path": "/chat/ask",
        "description": "Grounded personal chat",
        "handler": "_handle_chat",
    },
    {
        "method": "POST",
        "path": "/mentions/resolve",
        "description": "Deterministic local mention resolution",
        "handler": "_handle_mentions_resolve",
    },
    {
        "method": "GET",
        "path": "/thread",
        "description": "Chronological high-energy project thread",
        "handler": "_handle_thread",
    },
    {
        "method": "POST",
        "path": "/dream/trigger",
        "description": "Trigger one dream cycle",
        "handler": "_handle_dream_trigger",
    },
    {
        "method": "GET",
        "path": "/dream/stream",
        "description": "Dream event stream",
        "handler": "_handle_dream_stream",
    },
    {
        "method": "GET",
        "path": "/metabolism/status",
        "description": "Metabolism daemon state and queue preview",
        "handler": "_handle_metabolism_status",
    },
    {
        "method": "GET",
        "path": "/metabolism/jobs",
        "description": "Inspect metabolism jobs",
        "handler": "_handle_metabolism_jobs",
    },
    {
        "method": "POST",
        "path": "/metabolism/trigger",
        "description": "Queue a metabolism job manually",
        "handler": "_handle_metabolism_trigger",
    },
    {
        "method": "POST",
        "path": "/actions/dispatch",
        "description": "Submit a structured action intent to the action bus",
        "handler": "_handle_action_dispatch",
    },
    {
        "method": "GET",
        "path": "/actions/capabilities",
        "description": "Inspect the currently supported bounded action adapters",
        "handler": "_handle_actions_capabilities",
    },
    {
        "method": "GET",
        "path": "/actions/history",
        "description": "Review queued and executed action_dispatch jobs",
        "handler": "_handle_actions_history",
    },
    {
        "method": "GET",
        "path": "/ops/overview",
        "description": "Inspect active sessions, locks, approvals, and recent action lifecycle state",
        "handler": "_handle_ops_overview",
    },
    {
        "method": "GET",
        "path": "/registry/status",
        "description": "Inspect locks, beacons, ledger activity, agent contracts, and coordination health",
        "handler": "_handle_registry_status",
    },
    {
        "method": "GET",
        "path": "/agents/status",
        "description": "Inspect registered agents, active sessions, and contract readiness",
        "handler": "_handle_agents_status",
    },
    {
        "method": "GET",
        "path": "/approvals/inbox",
        "description": "Inspect blocked approval jobs awaiting judgment",
        "handler": "_handle_approvals_inbox",
    },
    {
        "method": "POST",
        "path": "/approvals/resolve",
        "description": "Approve, reject, or defer a queued action job",
        "handler": "_handle_approvals_resolve",
    },
    {
        "method": "POST",
        "path": "/registry/lock",
        "description": "Acquire a swarm lock",
        "handler": "_handle_registry_lock",
    },
    {
        "method": "POST",
        "path": "/registry/unlock",
        "description": "Release a swarm lock",
        "handler": "_handle_registry_unlock",
    },
    {
        "method": "POST",
        "path": "/registry/beacon",
        "description": "Emit an operational beacon",
        "handler": "_handle_registry_beacon",
    },
    {
        "method": "GET",
        "path": "/mesh/manifest",
        "description": "Advertise organism identity, transports, capabilities, and exported presence",
        "handler": "_handle_mesh_manifest",
    },
    {
        "method": "POST",
        "path": "/mesh/handshake",
        "description": "Accept a signed mesh federation handshake and register a peer organism",
        "handler": "_handle_mesh_handshake",
    },
    {
        "method": "GET",
        "path": "/mesh/peers",
        "description": "Inspect known mesh peers and federation health",
        "handler": "_handle_mesh_peers",
    },
    {
        "method": "POST",
        "path": "/mesh/peers/sync",
        "description": "Synchronize one or more connected peers using stored stream cursors and heartbeat snapshots",
        "handler": "_handle_mesh_peers_sync",
    },
    {
        "method": "GET",
        "path": "/mesh/stream",
        "description": "Return a mesh event stream snapshot and websocket bootstrap frame",
        "handler": "_handle_mesh_stream",
    },
    {
        "method": "POST",
        "path": "/mesh/lease/acquire",
        "description": "Acquire an advisory cross-organism mesh lease",
        "handler": "_handle_mesh_lease_acquire",
    },
    {
        "method": "POST",
        "path": "/mesh/lease/heartbeat",
        "description": "Heartbeat an advisory cross-organism mesh lease",
        "handler": "_handle_mesh_lease_heartbeat",
    },
    {
        "method": "POST",
        "path": "/mesh/lease/release",
        "description": "Release an advisory cross-organism mesh lease",
        "handler": "_handle_mesh_lease_release",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/submit",
        "description": "Submit a signed mesh job for bounded remote execution",
        "handler": "_handle_mesh_job_submit",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/schedule",
        "description": "Schedule a mesh job onto the best local or remote target according to trust, capability, and load",
        "handler": "_handle_mesh_job_schedule",
    },
    {
        "method": "GET",
        "path": "/mesh/jobs/{job_id}",
        "description": "Inspect the current state of a mesh job",
        "handler": "_handle_mesh_job_get",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/{job_id}/cancel",
        "description": "Cancel an in-flight mesh job",
        "handler": "_handle_mesh_job_cancel",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/{job_id}/resume",
        "description": "Resume a resumable mesh job from its latest checkpoint",
        "handler": "_handle_mesh_job_resume",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/{job_id}/resume-from-checkpoint",
        "description": "Resume a resumable mesh job from a specified checkpoint artifact",
        "handler": "_handle_mesh_job_resume_from_checkpoint",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/{job_id}/restart",
        "description": "Restart a mesh job cleanly without checkpoint state",
        "handler": "_handle_mesh_job_restart",
    },
    {
        "method": "GET",
        "path": "/mesh/workers",
        "description": "Inspect registered mesh workers and their current concurrency state",
        "handler": "_handle_mesh_workers",
    },
    {
        "method": "GET",
        "path": "/mesh/queue",
        "description": "Inspect durable mesh queue delivery state and redelivery status",
        "handler": "_handle_mesh_queue",
    },
    {
        "method": "GET",
        "path": "/mesh/queue/events",
        "description": "Read queue lifecycle events with replay cursors and optional job filters",
        "handler": "_handle_mesh_queue_events",
    },
    {
        "method": "GET",
        "path": "/mesh/queue/metrics",
        "description": "Inspect queue pressure, worker slot capacity, and dead-letter backlog",
        "handler": "_handle_mesh_queue_metrics",
    },
    {
        "method": "POST",
        "path": "/mesh/queue/replay",
        "description": "Replay a dead-lettered or cancelled queue message back into the runnable queue",
        "handler": "_handle_mesh_queue_replay",
    },
    {
        "method": "POST",
        "path": "/mesh/queue/ack-deadline",
        "description": "Adjust the active ack deadline for an inflight queue delivery",
        "handler": "_handle_mesh_queue_ack_deadline",
    },
    {
        "method": "GET",
        "path": "/mesh/scheduler/decisions",
        "description": "Inspect recent durable scheduler placement decisions and unplaced jobs",
        "handler": "_handle_mesh_scheduler_decisions",
    },
    {
        "method": "POST",
        "path": "/mesh/workers/register",
        "description": "Register or update a mesh worker runtime on the local organism",
        "handler": "_handle_mesh_worker_register",
    },
    {
        "method": "POST",
        "path": "/mesh/workers/{worker_id}/heartbeat",
        "description": "Heartbeat a mesh worker and refresh local worker metadata",
        "handler": "_handle_mesh_worker_heartbeat",
    },
    {
        "method": "POST",
        "path": "/mesh/workers/{worker_id}/poll",
        "description": "Poll for queued mesh jobs matching the worker capabilities",
        "handler": "_handle_mesh_worker_poll",
    },
    {
        "method": "POST",
        "path": "/mesh/workers/{worker_id}/claim",
        "description": "Claim the next matching queued mesh job for a worker",
        "handler": "_handle_mesh_worker_claim",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/attempts/{attempt_id}/heartbeat",
        "description": "Heartbeat a claimed mesh job attempt lease",
        "handler": "_handle_mesh_attempt_heartbeat",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/attempts/{attempt_id}/complete",
        "description": "Mark a mesh job attempt completed and publish its result bundle",
        "handler": "_handle_mesh_attempt_complete",
    },
    {
        "method": "POST",
        "path": "/mesh/jobs/attempts/{attempt_id}/fail",
        "description": "Mark a mesh job attempt failed and optionally requeue it",
        "handler": "_handle_mesh_attempt_fail",
    },
    {
        "method": "POST",
        "path": "/mesh/artifacts/publish",
        "description": "Publish a signed mesh artifact and receive an artifact ref",
        "handler": "_handle_mesh_artifact_publish",
    },
    {
        "method": "GET",
        "path": "/mesh/artifacts",
        "description": "List mesh artifacts with retention and metadata filters",
        "handler": "_handle_mesh_artifact_list",
    },
    {
        "method": "GET",
        "path": "/mesh/artifacts/{artifact_id}",
        "description": "Fetch a mesh artifact by id",
        "handler": "_handle_mesh_artifact_get",
    },
    {
        "method": "POST",
        "path": "/mesh/artifacts/purge",
        "description": "Purge expired mesh artifacts by retention deadline",
        "handler": "_handle_mesh_artifact_purge",
    },
    {
        "method": "POST",
        "path": "/mesh/agents/handoff",
        "description": "Submit a signed cross-organism handoff packet",
        "handler": "_handle_mesh_handoff",
    },
    {
        "method": "GET",
        "path": "/obsidian/status",
        "description": "Obsidian synapse runtime/export status",
        "handler": "_handle_obsidian_status",
    },
    {
        "method": "GET",
        "path": "/wake",
        "description": "Bootstrap continuity context for an agent, including active session and next-step reasoning",
        "handler": "_handle_wake",
    },
)


def _utc_iso(ts: float | None = None) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts or time.time()))


def _route_manifest_hash() -> str:
    payload = json.dumps(API_ROUTE_MANIFEST, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _discover_git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(_HERE), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=1.5,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    sha = (result.stdout or "").strip()
    return sha or None


def _runtime_auth_mode() -> str:
    raw = (
        os.environ.get("PERSONAL_MIRROR_AUTH_MODE")
        or os.environ.get("PM_AUTH_MODE")
        or ""
    ).strip().lower()
    if raw in {"development", "dev", "local"}:
        return "development"
    if raw in {"secured", "secure", "token"}:
        return "secured"
    return "secured" if _configured_agent_token() else "development"


def _is_loopback_client(client_address: str | None) -> bool:
    if not client_address:
        return False
    try:
        return ipaddress.ip_address(client_address).is_loopback
    except ValueError:
        return client_address in {"localhost"}


def _enabled_subsystems() -> dict[str, bool]:
    return {
        "engine": server_context.get("engine") is not None,
        "lattice": server_context.get("lattice") is not None,
        "dream": server_context.get("dream") is not None,
        "registry": server_context.get("registry") is not None,
        "swarm": server_context.get("swarm") is not None,
        "vessel": server_context.get("vessel") is not None,
        "metabolism": server_context.get("metabolism") is not None,
        "action_bus": server_context.get("action_bus") is not None,
        "mesh": server_context.get("mesh") is not None,
        "organism_ready": bool(server_context.get("is_ready")),
    }


def _runtime_contract_payload() -> dict:
    auth_mode = _runtime_auth_mode()
    git_sha = _discover_git_sha()
    expected = (
        os.environ.get("PERSONAL_MIRROR_EXPECTED_RUNTIME_CONTRACT_VERSION")
        or RUNTIME_CONTRACT_VERSION
    ).strip()
    configured_token = _configured_agent_token()
    return {
        "runtime_contract_version": RUNTIME_CONTRACT_VERSION,
        "expected_runtime_contract_version": expected,
        "runtime_contract_matches_expected": expected == RUNTIME_CONTRACT_VERSION,
        "process_started_at": _utc_iso(_PROCESS_STARTED_AT),
        "uptime_seconds": round(max(time.time() - _PROCESS_STARTED_AT, 0.0), 3),
        "build": {
            "version": RUNTIME_BUILD_VERSION,
            "stamp": RUNTIME_BUILD_STAMP,
            "git_sha": git_sha,
            "git_sha_short": git_sha[:12] if git_sha else None,
        },
        "routes": {
            "manifest_count": len(API_ROUTE_MANIFEST),
            "manifest_hash": _route_manifest_hash(),
        },
        "auth": {
            "mode": auth_mode,
            "token_configured": bool(configured_token),
            "protected_get_routes": sorted(_PROTECTED_GET_PATHS),
            "protected_post_routes": sorted(_PROTECTED_POST_PATHS),
            "development_policy": "loopback_only" if auth_mode == "development" else None,
            "secured_policy": "token_required" if auth_mode == "secured" else None,
        },
        "enabled_subsystems": _enabled_subsystems(),
    }


def _obsidian_dependency_payload(vault_path: str | None) -> dict:
    resolved = ""
    try:
        if vault_path:
            resolved = str(Path(vault_path).expanduser().resolve(strict=False))
    except Exception:
        resolved = str(vault_path or "")
    workspace = str(_HERE.resolve(strict=False))
    dependency_mode = "unconfigured"
    warning = ""
    boundary_clear = True
    if resolved:
        if resolved.startswith(workspace):
            dependency_mode = "workspace_local"
        else:
            dependency_mode = "external_vault"
            boundary_clear = False
            warning = (
                "Personal Mirror is reading an external Obsidian vault dependency. "
                "This boundary is real and should not be mistaken for a self-contained Personal Mirror vault."
            )
    return {
        "mode": dependency_mode,
        "vault_path": resolved or None,
        "workspace_root": workspace,
        "boundary_clear": boundary_clear,
        "warning": warning or None,
    }


def render_dynamic_page(path: str) -> str | None:
    try:
        import pages as _pages

        _pages = importlib.reload(_pages)
        route_map = {
            "/": lambda: _pages.html_chat_page(),
            "/shell": lambda: _pages.html_shell_page(),
            "/shell.html": lambda: _pages.html_shell_page(),
            "/dashbourn": lambda: _pages.html_chat_page(),
            "/dashboard": lambda: _pages.html_chat_page(),
            "/cockpit": lambda: _pages.html_chat_page(),
            "/wallet": lambda: _pages.html_wallet_page(),
            "/onboarding": lambda: _pages.html_onboarding_page(),
            "/briefing": lambda: _pages.html_briefing_page(
                server_context.get("lattice"),
                server_context.get("history"),
            ),
            "/mind": lambda: _pages.html_mind_page(),
            "/lattice": lambda: _pages.html_lattice_page(),
            "/dream": lambda: _pages.html_dream_page(server_context.get("lattice")),
            "/ops": lambda: _pages.html_ops_page(),
            "/operator": lambda: _pages.html_ops_page(),
            "/approvals": lambda: _pages.html_approvals_page(),
            "/axioms": lambda: _pages.html_axioms_page(server_context.get("lattice")),
            "/tensions": lambda: _pages.html_tensions_page(server_context.get("lattice")),
            "/history": lambda: _pages.html_history_page(server_context.get("history")),
            "/terminal": lambda: _pages.html_terminal_page(),
            "/chat": lambda: _pages.html_chat_page(),
            "/rules": lambda: _pages.html_rules_page(server_context.get("lattice")),
            "/coherence": lambda: _pages.html_coherence_page(server_context.get("lattice")),
            "/future": lambda: _pages.html_trajectory_page(server_context.get("lattice")),
            "/trajectory-lab": lambda: _pages.html_trajectory_page(server_context.get("lattice")),
            "/auto-inject": lambda: _pages.html_auto_inject_page(server_context.get("lattice")),
            "/memories": lambda: _pages.html_memories_page(),
            "/patterns": lambda: _pages.html_patterns_page(server_context.get("lattice")),
            "/swarm": lambda: _pages.html_swarm_page(server_context.get("lattice")),
        }
        renderer = route_map.get(path)
        return renderer() if renderer else None
    except Exception as exc:
        logger.warning(f"Dynamic page routing failed for {path}: {exc}")
        return None

# --- DYNAMIC IMPORT SYSTEM (The "Nervous System" attempt) ---
def safe_import(module_name):
    """Import a module from the personal_mirror package using absolute imports.
    Tries package-qualified name first, then top-level fallback.
    """
    # Modules in personal_mirror package use absolute imports (no leading dot)
    _pkg = "personal_mirror"
    try:
        # Try absolute import within package context
        m = importlib.import_module(f"{_pkg}.{module_name}")
        logger.info(f"Loaded {_pkg}.{module_name}")
        return m
    except ImportError as e1:
        try:
            # Fallback: top-level import (for mirror_engine, etc. at package root)
            m = __import__(module_name)
            logger.info(f"Loaded {module_name} (top-level)")
            return m
        except ImportError as e2:
            logger.error(f"CRITICAL: Could not find module {module_name}. The 'Soul' is disconnected. ({e2})")
            return None

# Attempt to load the intelligence layers
mirror_engine_mod = safe_import("mirror_engine")
personal_lattice_mod = safe_import("personal_lattice")
personal_mind_mod = safe_import("personal_mind")
personal_pedagogue_mod = safe_import("pedagogue")
personal_dream_mod = safe_import("personal_dream")
hive_registry_mod = safe_import("hive_registry")
swarm_gateway_mod = safe_import("swarm_gateway")
interaction_history_mod = safe_import("history")
pm_vessel_mod = safe_import("pm_vessel")
pm_oscillators_mod = safe_import("pm_oscillators")
thought_grammar_mod = safe_import("thought_grammar")
metabolism_engine_mod = safe_import("metabolism_engine")
action_bus_mod = safe_import("action_bus")
mesh_mod = safe_import("mesh")

# Global context container
server_context = {
    'engine': None,
    'lattice': None,
    'mind': None,
    'pedagogue': None,
    'dream': None,
    'registry': None,
    'swarm': None,
    'history': None,
    'vessel': None,
    'organism': None,
    'metabolism': None,
    'action_bus': None,
    'mesh': None,
    'oscillator_thread': None,
    'is_ready': False
}

# --- GLOBALS ---
_session_id = str(_uuid.uuid4())


def _structured_log(event: str, **fields):
    try:
        logger.info("mirror.telemetry %s", json.dumps({"event": event, **fields}, sort_keys=True, default=str))
    except Exception:
        logger.info("mirror.telemetry event=%s fields=%s", event, fields)


_CLIENT_DISCONNECT_ERRNOS = {
    errno.EPIPE,
    errno.ECONNRESET,
    getattr(errno, "ECONNABORTED", 103),
}

_PROTECTED_GET_PATHS = {
    "/approvals/inbox",
    "/actions/history",
    "/ops/overview",
    "/registry",
    "/registry/status",
    "/registry/locks",
    "/registry/beacons",
    "/registry/ledger",
}

_PROTECTED_POST_PATHS = {
    "/memory/store",
    "/self-model/store",
    "/promises/store",
    "/experiments/start",
    "/experiments/close",
    "/continuity/feedback",
    "/rituals/run",
    "/noticing/scan",
    "/trajectory/refresh",
    "/swarm/submit",
    "/registry/lock",
    "/registry/heartbeat",
    "/registry/unlock",
    "/registry/beacon",
    "/ice/verify",
    "/memory/resolve-tension",
    "/dream/trigger",
    "/omp/join",
    "/session/begin",
    "/session/end",
    "/memory/continuation",
    "/memory/insight",
    "/memory/question",
    "/continuation/resolve",
    "/autonomy/trigger",
    "/metabolism/trigger",
    "/actions/dispatch",
    "/approvals/resolve",
    "/mesh/peers/sync",
}

_SIGNED_MESH_POST_PATHS = {
    "/mesh/handshake",
    "/mesh/jobs/submit",
    "/mesh/artifacts/publish",
    "/mesh/agents/handoff",
}

_PROTECTED_MESH_ARTIFACT_CONTENT_PATH = "/mesh/artifacts/content"


def _is_client_disconnect(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        return exc.errno in _CLIENT_DISCONNECT_ERRNOS or str(exc).lower().find("broken pipe") >= 0
    return False


def _configured_agent_token() -> str:
    return (
        os.environ.get("PERSONAL_MIRROR_AGENT_TOKEN")
        or os.environ.get("PM_AGENT_TOKEN")
        or ""
    ).strip()


def _extract_bearer_token(value: str) -> str:
    sample = (value or "").strip()
    if not sample:
        return ""
    if sample.lower().startswith("bearer "):
        return sample[7:].strip()
    return sample


def _request_agent_token(headers) -> str:
    if headers is None:
        return ""
    for key in ("X-PM-Agent-Token", "X-Agent-Key", "X-API-Key", "Authorization"):
        value = headers.get(key)
        token = _extract_bearer_token(value)
        if token:
            return token
    return ""


def _route_requires_agent_auth(method: str, path: str) -> bool:
    route_method = (method or "").strip().upper()
    route_path = (path or "").strip()
    if route_method == "GET":
        return route_path in _PROTECTED_GET_PATHS or route_path == _PROTECTED_MESH_ARTIFACT_CONTENT_PATH
    if route_method == "POST":
        if route_path.startswith("/mesh/") and route_path not in _SIGNED_MESH_POST_PATHS:
            return True
        return route_path in _PROTECTED_POST_PATHS
    return False


def _authorization_failure_payload(method: str, path: str, client_address: str | None = None) -> dict:
    mode = _runtime_auth_mode()
    if mode == "secured":
        detail = "protected routes require X-PM-Agent-Token or Authorization Bearer token in secured mode"
    else:
        detail = "protected routes are limited to loopback clients in development mode"
    return {
        "error": "agent authorization required",
        "auth_mode": mode,
        "path": path,
        "method": (method or "").upper(),
        "client_address": client_address,
        "detail": detail,
    }


def _is_authorized_agent_request(method: str, path: str, headers, client_address: str | None = None) -> bool:
    if not _route_requires_agent_auth(method, path):
        return True
    mode = _runtime_auth_mode()
    if mode == "development":
        if _is_loopback_client(client_address):
            return True
        configured = _configured_agent_token()
        return bool(configured and _request_agent_token(headers) == configured)
    configured = _configured_agent_token()
    if not configured:
        return False
    return _request_agent_token(headers) == configured


def _normalize_model_version(value) -> str:
    resolved = (value or "").strip()
    return resolved or "unknown-model"


def _build_autogenerated_identity_text(*, agent_id: str, agent_name: str, model_version: str, ctx: dict) -> str:
    session_num = (ctx.get("sessions_count") or 0) + 1
    return (
        f"Session {session_num}. Agent {agent_id} operating as {agent_name}. "
        f"Model version: {model_version}. Shared memory nodes: {ctx.get('lattice_nodes', 0)}. "
        f"Mirror coherence: {ctx.get('lattice_mirror_m', 0.0):.4f}."
    )


def begin_session_payload(lattice, data: dict) -> dict:
    agent_id = (data.get("agent_id") or "unknown-agent").strip() or "unknown-agent"
    session_id = data.get("session_id", str(_uuid.uuid4()))
    text = (data.get("text") or "").strip()
    metadata = dict(data.get("metadata") or {})
    registration = lattice.get_agent_registration(agent_id) if hasattr(lattice, "get_agent_registration") else {}
    agent_name = (
        data.get("agent_name")
        or data.get("human_name")
        or metadata.get("agent_name")
        or registration.get("agent_name")
        or agent_id
    )
    model_version = _normalize_model_version(data.get("model_version") or metadata.get("model_version") or registration.get("model_version"))
    if hasattr(lattice, "canonicalize_identity_nodes"):
        lattice.canonicalize_identity_nodes(
            agent_id=agent_id,
            agent_name=agent_name,
            model_version=model_version,
            session_id=session_id,
            source="session_begin",
        )
    ctx = lattice.get_wake_context(agent_id, limit_each=3)
    identity_source = "provided_text"
    if not text:
        identity_source = "autogenerated"
        text = _build_autogenerated_identity_text(
            agent_id=agent_id,
            agent_name=agent_name,
            model_version=model_version,
            ctx=ctx,
        )
    metadata.update(
        {
            "agent_name": agent_name,
            "identity_source": identity_source,
            "registration_metadata": registration,
        }
    )
    node = lattice.store_identity_node(
        agent_id=agent_id,
        text=text,
        session_id=session_id,
        model_version=model_version,
        metadata=metadata,
    )
    runtime = (
        metadata.get("runtime")
        or registration.get("runtime")
        or ""
    )
    if hasattr(lattice, "heartbeat_agent_session"):
        lattice.heartbeat_agent_session(
            session_id,
            agent_id=agent_id,
            runtime=str(runtime or ""),
            current_project=(data.get("project_id") or data.get("project") or "").strip(),
            current_task=(data.get("current_task") or data.get("query") or text[:160]).strip(),
            metadata=metadata,
            status="active",
        )
    wake_ctx = lattice.get_wake_context(agent_id, limit_each=5)
    _structured_log(
        "session_begin_identity_source",
        agent_id=agent_id,
        agent_name=agent_name,
        model_version=model_version,
        identity_source=identity_source,
        registration_node_id=registration.get("node_id"),
        session_id=session_id,
    )
    return {
        "identity_node": node,
        "session_id": session_id,
        "wake_context": wake_ctx,
        "status": "session_begun",
    }


def resolve_mentions_payload(lattice, data: dict) -> dict:
    query = (data.get("query") or data.get("q") or "").strip()
    if not query:
        return {
            "query": "",
            "query_without_mentions": "",
            "expanded_query": "",
            "mentions": [],
            "warnings": [],
            "resolved_node_ids": [],
            "context_blocks": [],
            "latency_ms": 0.0,
        }
    if "@" not in query or not thought_grammar_mod:
        return {
            "query": query,
            "query_without_mentions": query,
            "expanded_query": query,
            "mentions": [],
            "warnings": [],
            "resolved_node_ids": [],
            "context_blocks": [],
            "latency_ms": 0.0,
        }

    resolver = thought_grammar_mod.ThoughtGrammarResolver(lattice)
    return resolver.resolve_query(
        query=query,
        agent_id=(data.get("agent_id") or data.get("agent") or "claude-code").strip() or "claude-code",
        session_id=(data.get("session_id") or "").strip() or None,
        project_id=(data.get("project_id") or data.get("project") or "").strip() or None,
        limit=int(data.get("limit") or 5),
    )


def _mentions_to_context_nodes(mentions: List[dict], limit: int = 4) -> List[dict]:
    context = []
    for mention in mentions:
        for item in mention.get("items", []):
            context.append(
                {
                    "id": item.get("id"),
                    "short_ref": item.get("short_ref") or item.get("ref"),
                    "category": item.get("category"),
                    "domain": item.get("domain"),
                    "value": item.get("text", ""),
                    "verification_status": item.get("verification_status"),
                    "energy": item.get("energy"),
                }
            )
            if len(context) >= limit:
                return context
    return context


def _search_context(lattice, query: str, limit: int = 4) -> List[dict]:
    sample = (query or "").strip()
    if not lattice or not sample:
        return []
    try:
        return lattice.recall_semantic(sample, max_results=limit)
    except Exception:
        return []


def _grounded_fallback_answer(resolved_mentions: List[dict]) -> str:
    snippets = []
    for mention in resolved_mentions[:2]:
        for item in mention.get("items", [])[:2]:
            text = (item.get("text") or "").strip()
            if text and text not in snippets:
                snippets.append(text)
    if not snippets:
        return "Based on grounded thought references: No resolved context was available. [Grounded via thought grammar]"
    if len(snippets) == 1:
        return f"Based on grounded thought references: {snippets[0]} [Grounded via thought grammar]"
    return (
        f"Based on grounded thought references: {snippets[0]} "
        f"Also relevant: {snippets[1]} [Grounded via thought grammar]"
    )


def _continuity_project_id(lattice, data: dict) -> Optional[str]:
    explicit = (data.get("project_id") or data.get("project") or "").strip()
    if explicit:
        return explicit
    session_id = (data.get("session_id") or "").strip()
    agent_id = (data.get("agent_id") or "").strip()
    if lattice and session_id and hasattr(lattice, "get_agent_session"):
        try:
            session = lattice.get_agent_session(session_id)
        except Exception:
            session = None
        project = ((session or {}).get("current_project") or "").strip()
        if project:
            return project
    if lattice and hasattr(lattice, "get_active_context_window"):
        try:
            active = lattice.get_active_context_window(focus_type="workspace")
        except Exception:
            active = None
        project = ((active or {}).get("project_id") or "").strip()
        if project:
            return project
    if session_id and session_id != "unknown":
        return session_id
    if agent_id:
        return None
    return None


def _continuity_context_block(brief: Optional[dict], query: str = "") -> str:
    if not isinstance(brief, dict):
        return ""
    lines: list[str] = []
    last_intent = (brief.get("inferred_last_intent") or {}).get("summary")
    next_step = (brief.get("best_next_step") or {}).get("text")
    continuations = [
        item.get("text")
        for item in (brief.get("active_continuations") or [])[:2]
        if isinstance(item, dict) and item.get("text")
    ]
    tensions = [
        item.get("text")
        for item in (brief.get("top_tensions") or [])[:2]
        if isinstance(item, dict) and item.get("text")
    ]
    if not (last_intent or next_step or continuations or tensions):
        return ""
    posture = _continuity_response_posture(brief, query=query)
    fit = posture.get("intervention_fit") or {}
    posture_fit = posture.get("posture_fit") or {}
    if fit.get("label"):
        lines.append(f"INTERVENTION_FIT: {fit['label']}")
    if posture_fit.get("steering") and posture_fit.get("steering") != "neutral":
        lines.append(f"POSTURE_FIT: {posture_fit['steering']}")
    if posture.get("instruction"):
        lines.append(f"RESPONSE_POSTURE: {posture['instruction']}")
    if last_intent:
        lines.append(f"LAST_INTENT: {last_intent}")
    for item in continuations:
        lines.append(f"ACTIVE_CONTINUATION: {item}")
    for item in tensions:
        lines.append(f"TOP_TENSION: {item}")
    if next_step:
        lines.append(f"BEST_NEXT_STEP: {next_step}")
    if not lines:
        return ""
    return "[Continuity Context]\n" + "\n".join(lines)


def _continuity_query_signal(query: str, continuity: Optional[dict] = None) -> dict:
    lowered = " ".join(str(query or "").strip().lower().split())
    if not lowered:
        return {
            "kind": "neutral",
            "label": "Neutral",
            "summary": "",
        }
    directional_terms = (
        "what should i do",
        "what do i do",
        "what now",
        "next step",
        "what next",
        "where should i focus",
        "where do i focus",
        "help me decide",
        "decide",
        "choose",
        "pick one",
        "priority",
        "prioritize",
        "plan",
        "recommend",
        "which should",
        "should i",
    )
    overloaded_terms = (
        "overloaded",
        "overwhelmed",
        "too much",
        "swamped",
        "stretched",
        "scattered",
        "chaotic",
        "behind",
        "spinning",
        "can't keep up",
        "cannot keep up",
    )
    emotional_terms = (
        "feel",
        "afraid",
        "scared",
        "anxious",
        "sad",
        "guilty",
        "ashamed",
        "hurt",
        "upset",
        "torn",
        "conflicted",
        "ambivalent",
        "burned out",
        "burnt out",
        "stuck",
        "confused",
    )
    reflective_terms = (
        "why am i",
        "why do i",
        "what am i missing",
        "help me understand",
        "what matters here",
        "what is this pattern",
        "make sense",
    )
    signals = (continuity.get("signals") or {}) if isinstance(continuity, dict) else {}
    intervention_fit = signals.get("intervention_fit") or {}
    fit_label = str(intervention_fit.get("label") or "neutral").strip().lower() or "neutral"
    active_count = len((continuity.get("active_continuations") or [])) if isinstance(continuity, dict) else 0
    tension_count = len((continuity.get("top_tensions") or [])) if isinstance(continuity, dict) else 0
    directional = any(term in lowered for term in directional_terms) or lowered.endswith("?")
    overloaded = any(term in lowered for term in overloaded_terms) or (fit_label == "hesitant" and (active_count + tension_count) >= 3)
    emotional = any(term in lowered for term in emotional_terms)
    reflective = any(term in lowered for term in reflective_terms)
    if overloaded:
        return {
            "kind": "overloaded",
            "label": "Overloaded",
            "summary": "The query reads as overloaded or timing-constrained, so the response should reduce complexity and keep the next move small.",
        }
    if emotional:
        return {
            "kind": "emotional",
            "label": "Emotional",
            "summary": "The query sounds emotionally charged or conflicted, so the response should lower pressure and avoid sounding pushy.",
        }
    if directional:
        return {
            "kind": "directional",
            "label": "Directional",
            "summary": "The query is asking for a concrete steer, so the response can be more direct about the next move.",
        }
    if reflective:
        return {
            "kind": "reflective",
            "label": "Reflective",
            "summary": "The query is exploratory, so the response should clarify the strongest pattern before steering action.",
        }
    return {
        "kind": "neutral",
        "label": "Neutral",
        "summary": "",
    }


def _continuity_response_posture(continuity: Optional[dict], query: str = "") -> dict:
    continuity = continuity if isinstance(continuity, dict) else {}
    query_signal = _continuity_query_signal(query, continuity)
    if not continuity:
        continuity = {}
    fit = ((continuity.get("signals") or {}).get("intervention_fit") or {})
    posture_fit = ((continuity.get("signals") or {}).get("posture_fit") or {})
    fit_label = str(fit.get("label") or "neutral").strip().lower() or "neutral"
    posture_steering = str(posture_fit.get("steering") or "neutral").strip().lower() or "neutral"
    postures = {
        "trust-building": {
            "tone": "directive",
            "label": "Direct",
            "instruction": "Be direct and concrete. Name the strongest thread, recommend one clear next move, and do not hedge unnecessarily.",
        },
        "hesitant": {
            "tone": "scaffolded",
            "label": "Scaffolded",
            "instruction": "Use supportive structure. Offer one small next move, acknowledge timing pressure, and keep the recommendation easy to pick up.",
        },
        "resistant": {
            "tone": "gentle",
            "label": "Gentle",
            "instruction": "Lower the pressure. Reframe the recommendation as a smaller or safer move, and avoid sounding forceful or overly certain.",
        },
        "neutral": {
            "tone": "reflective",
            "label": "Reflective",
            "instruction": "Use a calm reflective tone, surface the strongest thread, and offer one concrete next step without overcommitting.",
        },
    }
    posture = dict(postures.get(fit_label, postures["neutral"]))
    if posture_steering == "gentler":
        posture = dict(postures["resistant"])
    elif posture_steering == "firmer":
        posture = dict(postures["trust-building"])
    elif posture_steering == "stable" and fit_label == "neutral":
        posture = dict(postures["hesitant"])
    query_kind = query_signal.get("kind") or "neutral"
    if query_kind == "overloaded":
        posture = dict(postures["hesitant"] if posture_steering != "gentler" else postures["resistant"])
    elif query_kind == "emotional":
        posture = dict(postures["resistant"])
    elif query_kind == "directional" and posture_steering != "gentler":
        if fit_label == "neutral":
            posture = dict(postures["trust-building"])
        elif fit_label == "resistant":
            posture = dict(postures["hesitant"])
    posture["intervention_fit"] = {
        "label": fit_label,
        "summary": (fit.get("summary") or "").strip(),
        "accepted": fit.get("accepted"),
        "not_now": fit.get("not_now"),
        "rejected": fit.get("rejected"),
        "net_bias": fit.get("net_bias"),
    }
    posture["posture_fit"] = {
        "label": str(posture_fit.get("label") or "neutral").strip(),
        "summary": (posture_fit.get("summary") or "").strip(),
        "steering": posture_steering,
        "good": posture_fit.get("good"),
        "too_forceful": posture_fit.get("too_forceful"),
        "too_soft": posture_fit.get("too_soft"),
    }
    posture["query_signal"] = query_signal
    reasons = [
        summary
        for summary in (
            query_signal.get("summary"),
            posture["posture_fit"].get("summary"),
            posture["intervention_fit"].get("summary"),
        )
        if summary
    ]
    posture["selection_basis"] = "feedback+query" if query_kind != "neutral" else "feedback"
    posture["reason"] = reasons[0] if reasons else posture.get("instruction", "")
    return posture


def _should_use_continuity_fallback(clean_query: str, continuity: Optional[dict]) -> bool:
    if not isinstance(continuity, dict):
        return False
    if not ((continuity.get("inferred_last_intent") or {}).get("summary") or (continuity.get("best_next_step") or {}).get("text")):
        return False
    lowered = (clean_query or "").strip().lower()
    cues = (
        "what now",
        "next",
        "best next",
        "where am i",
        "what matters",
        "continue",
        "pick up",
        "resume",
        "focus",
        "tension",
    )
    return any(cue in lowered for cue in cues)


def _continuity_fallback_answer(continuity: dict, query: str = "") -> str:
    posture = _continuity_response_posture(continuity, query=query)
    tone = posture.get("tone")
    last_intent = ((continuity.get("inferred_last_intent") or {}).get("summary") or "").strip()
    next_step = ((continuity.get("best_next_step") or {}).get("text") or "").strip()
    tensions = [
        (item.get("text") or "").strip()
        for item in (continuity.get("top_tensions") or [])[:2]
        if isinstance(item, dict) and (item.get("text") or "").strip()
    ]
    if tone == "directive":
        lead = "Based on continuity evidence: Stay direct here."
    elif tone == "scaffolded":
        lead = "Based on continuity evidence: Keep the next move small and easy to resume."
    elif tone == "gentle":
        lead = "Based on continuity evidence: Use a lower-pressure move here."
    else:
        lead = "Based on continuity evidence:"
    if last_intent and next_step:
        answer = f"{lead} You were last moving toward {last_intent}. Best next step: {next_step}"
    elif next_step:
        answer = f"{lead} Best next step: {next_step}"
    elif last_intent:
        answer = f"{lead} The strongest live direction is {last_intent}"
    else:
        answer = f"{lead} No strong continuity direction is currently resolved."
    if tensions:
        answer += f" Main tension: {tensions[0]}"
    return answer + " [Grounded via continuity brief]"


def _summarize_support_item(item: dict) -> dict:
    text = (item.get("text") or item.get("value") or "").strip()
    return {
        "id": item.get("id") or "",
        "short_ref": item.get("short_ref") or item.get("ref") or "",
        "category": item.get("category") or item.get("kind") or "",
        "text": text[:220],
        "why_now": (item.get("why_now") or "").strip(),
        "confidence": item.get("confidence"),
        "salience": item.get("salience"),
    }


def _continuity_answer_support(continuity: Optional[dict], query: str = "") -> dict:
    if not isinstance(continuity, dict):
        return {
            "last_intent": None,
            "best_next_step": None,
            "drift_signal": None,
            "evidence": [],
            "response_posture": _continuity_response_posture({}, query=query),
        }
    evidence: list[dict] = []
    seen: set[tuple[str, str]] = set()

    def add(item: Optional[dict]) -> None:
        if not isinstance(item, dict):
            return
        summarized = _summarize_support_item(item)
        key = (summarized.get("id") or "", summarized.get("text") or "")
        if not (key[0] or key[1]) or key in seen:
            return
        seen.add(key)
        evidence.append(summarized)

    for section in ("active_continuations", "top_tensions"):
        for item in (continuity.get(section) or [])[:3]:
            add(item)
            for ev in (item.get("evidence") or [])[:2]:
                add(ev)
    inferred = continuity.get("inferred_last_intent") or {}
    next_step = continuity.get("best_next_step") or {}
    drift_signal = ((continuity.get("signals") or {}).get("drift_signal") or {})
    posture = _continuity_response_posture(continuity, query=query)
    for ev in (inferred.get("evidence") or [])[:2]:
        add(ev)
    for ev in (next_step.get("evidence") or [])[:2]:
        add(ev)
    for ev in (drift_signal.get("evidence") or [])[:2]:
        add(ev)
    return {
        "last_intent": (inferred.get("summary") or "").strip() or None,
        "best_next_step": (next_step.get("text") or "").strip() or None,
        "drift_signal": {
            "label": str(drift_signal.get("label") or "").strip() or None,
            "mode": str(drift_signal.get("mode") or "").strip() or None,
            "summary": (drift_signal.get("summary") or "").strip() or None,
            "pressure": drift_signal.get("pressure"),
        },
        "project_id": (continuity.get("project_id") or "").strip() or None,
        "response_posture": posture,
        "evidence": evidence[:6],
    }


def _continuity_feedback_delta(previous: Optional[dict], current: Optional[dict]) -> dict:
    before = previous if isinstance(previous, dict) else {}
    after = current if isinstance(current, dict) else {}
    changes: list[str] = []

    before_step = ((before.get("best_next_step") or {}).get("text") or "").strip()
    after_step = ((after.get("best_next_step") or {}).get("text") or "").strip()
    if after_step and after_step != before_step:
        changes.append("next_step_changed")

    before_intent = ((before.get("inferred_last_intent") or {}).get("summary") or "").strip()
    after_intent = ((after.get("inferred_last_intent") or {}).get("summary") or "").strip()
    if after_intent and after_intent != before_intent:
        changes.append("last_intent_shifted")

    before_fit = str((((before.get("signals") or {}).get("intervention_fit") or {}).get("label")) or "").strip()
    after_fit = str((((after.get("signals") or {}).get("intervention_fit") or {}).get("label")) or "").strip()
    if after_fit and after_fit != before_fit:
        changes.append("intervention_fit_changed")

    before_posture = str((((before.get("signals") or {}).get("posture_fit") or {}).get("steering")) or "").strip()
    after_posture = str((((after.get("signals") or {}).get("posture_fit") or {}).get("steering")) or "").strip()
    if after_posture and after_posture != before_posture:
        if after_posture == "gentler":
            changes.append("tone_softened")
        elif after_posture == "firmer":
            changes.append("tone_firmed")
        else:
            changes.append("tone_rebalanced")

    before_acted = int((((before.get("signals") or {}).get("intervention_fit") or {}).get("acted_on")) or 0)
    after_acted = int((((after.get("signals") or {}).get("intervention_fit") or {}).get("acted_on")) or 0)
    if after_acted > before_acted:
        changes.append("follow_through_recorded")

    labels = {
        "tone_softened": "Tone softened.",
        "tone_firmed": "Tone became more direct.",
        "tone_rebalanced": "Tone guidance was updated.",
        "next_step_changed": "Best next step changed.",
        "last_intent_shifted": "Last intent shifted.",
        "intervention_fit_changed": "Intervention fit changed.",
        "follow_through_recorded": "Follow-through recorded.",
    }
    summary = " ".join(labels[item] for item in changes[:3]).strip()
    if not summary:
        summary = "Continuity updated."
    return {
        "summary": summary,
        "changes": changes,
        "before": {
            "best_next_step": before_step or None,
            "last_intent": before_intent or None,
            "intervention_fit": before_fit or None,
            "posture_fit": before_posture or None,
            "acted_on": before_acted,
        },
        "after": {
            "best_next_step": after_step or None,
            "last_intent": after_intent or None,
            "intervention_fit": after_fit or None,
            "posture_fit": after_posture or None,
            "acted_on": after_acted,
        },
    }


def _query_mind(
    mind,
    lattice,
    expanded_query: str,
    clean_query: str,
    resolved_mentions: List[dict],
    continuity: Optional[dict] = None,
) -> str:
    answer = None
    if mind and getattr(mind, "lattice", None) is None and lattice is not None:
        try:
            mind.lattice = lattice
        except Exception:
            pass
    posture_query = clean_query or expanded_query
    continuity_block = _continuity_context_block(continuity, query=posture_query)
    prompt_query = expanded_query
    if continuity_block:
        prompt_query = continuity_block + "\n\n" + expanded_query
    if mind and hasattr(mind, "query"):
        answer = mind.query(prompt_query)
    elif mind and hasattr(mind, "answer"):
        answer = mind.answer(clean_query or prompt_query)
    elif mind and hasattr(mind, "verify_belief"):
        verdict = mind.verify_belief(clean_query or expanded_query)
        if isinstance(verdict, dict):
            rationale = (verdict.get("rationale") or "").strip()
            if rationale:
                answer = rationale
            elif verdict.get("verified") is True:
                answer = "Based on my verified knowledge: This claim is supported."
            elif verdict.get("verified") is False:
                answer = "Based on my verified knowledge: This claim is not yet verified."

    if answer:
        return answer
    if resolved_mentions:
        return _grounded_fallback_answer(resolved_mentions)
    if _should_use_continuity_fallback(clean_query or expanded_query, continuity):
        return _continuity_fallback_answer(continuity or {}, query=posture_query)
    context = _search_context(lattice, clean_query or expanded_query, limit=1)
    if context:
        top = context[0]
        return (
            "Based on my verified knowledge: "
            + (top.get("text") or top.get("value") or "The available facts do not address this question.")
            + " [Rendered from verified nodes]"
        )
    return "Based on my verified knowledge: The available facts do not address this question. [Rendered from verified nodes]"


def build_chat_payload(lattice, mind, data: dict) -> dict:
    query = (data.get("query") or "").strip()
    if not query:
        raise ValueError("query is required")

    resolution = resolve_mentions_payload(lattice, data)
    resolved_mentions = [item for item in resolution.get("mentions", []) if item.get("resolved")]
    clean_query = resolution.get("query_without_mentions") or query
    expanded_query = resolution.get("expanded_query") or query
    agent_id = (data.get("agent_id") or "").strip() or None
    project_id = _continuity_project_id(lattice, data)
    continuity = (
        lattice.continuity_brief(project_id=project_id, agent_id=agent_id, limit=3)
        if lattice and hasattr(lattice, "continuity_brief")
        else {}
    )
    posture_query = clean_query or expanded_query
    continuity_block = _continuity_context_block(continuity, query=posture_query)
    answer = _query_mind(mind, lattice, expanded_query, clean_query, resolved_mentions, continuity=continuity)
    context = _mentions_to_context_nodes(resolved_mentions)
    if not context:
        context = _search_context(lattice, clean_query or query, limit=4)
    context_blocks = list(resolution.get("context_blocks", []))
    if continuity_block:
        context_blocks = [continuity_block] + context_blocks
    return {
        "answer": answer,
        "query": query,
        "expanded_query": expanded_query,
        "resolved_mentions": resolved_mentions,
        "warnings": resolution.get("warnings", []),
        "resolved_node_ids": resolution.get("resolved_node_ids", []),
        "context_blocks": context_blocks,
        "context": context,
        "continuity": continuity,
        "answer_support": {
            "mode": (
            "mentions" if resolved_mentions else
                "continuity" if _should_use_continuity_fallback(clean_query or expanded_query, continuity) else
                "semantic"
            ),
            "continuity": _continuity_answer_support(continuity, query=posture_query),
            "context": [_summarize_support_item(item) for item in context[:4]],
        },
    }


def initialize_organism():
    print("🚀 [Personal Mirror] Initiating Soul Implantation...")
    
    try:
        # 1. Lattice (The Truth)
        if personal_lattice_mod:
            server_context['lattice'] = personal_lattice_mod.PersonalLattice(db_path="personal_lattice.db")
            print("✅ Personal Lattice Connected.")
        else:
            print("❌ Personal Lattice Missing!")

        # 1b. Registry (The Hive Coordination Layer)
        if hive_registry_mod and server_context['lattice']:
            server_context['registry'] = hive_registry_mod.HiveRegistry(server_context['lattice'])
            print("✅ Hive Registry Online.")

        # 2. Engine (The Metabolism)
        if mirror_engine_mod:
            server_context['engine'] = mirror_engine_mod.MirrorEngine()
            print("✅ Mirror Engine Online.")
        else:
            print("❌ Mirror Engine Missing!")

        # 3. Mind (The Cognition)
        if personal_mind_mod and server_context['engine'] and server_context['lattice']:
            server_context['mind'] = personal_mind_mod.PersonalMind(lattice=server_context['lattice'])
            print("✅ Personal Mind Online.")
        
        # 4. Pedagogue (The Curriculum)
        if personal_pedagogue_mod and server_context['mind']:
            server_context['pedagogue'] = personal_pedagogue_mod.PersonalPedagogue(
                server_context['engine'], server_context['lattice'], server_context['mind']
            )
            print("✅ Pedagogue Online.")

        # 5. Dream (The Subconscious)
        if personal_dream_mod and server_context.get('lattice') and server_context.get('mind'):
            server_context['dream'] = personal_dream_mod.PersonalDream(
                server_context['lattice'], server_context['mind']
            )
            server_context['dream'].start()
            print("✅ Dream Cycle Online.")

        # 5b. Metabolism (The Background Life)
        if metabolism_engine_mod and server_context.get('lattice'):
            if action_bus_mod:
                server_context['action_bus'] = action_bus_mod.ActionBus(
                    server_context['lattice'],
                    registry=server_context.get('registry'),
                    workspace_root=str(_HERE),
                )
                print("✅ Action Bus Online.")
            server_context['metabolism'] = metabolism_engine_mod.MetabolismEngine(
                server_context['lattice'],
                dream=server_context.get('dream'),
                mind=server_context.get('mind'),
                registry=server_context.get('registry'),
                action_bus=server_context.get('action_bus'),
            )
            server_context['metabolism'].start()
            print("✅ Metabolism Engine Online.")

        # 6. Swarm (The Connectivity)
        if swarm_gateway_mod and server_context['lattice']:
            server_context['swarm'] = swarm_gateway_mod.SwarmGateway(server_context['lattice'])
            print("✅ Swarm Gateway Online.")

        # 6b. Sovereign Mesh (federated organism substrate)
        if mesh_mod and server_context.get('lattice'):
            server_context['mesh'] = mesh_mod.SovereignMesh(
                server_context['lattice'],
                registry=server_context.get('registry'),
                metabolism=server_context.get('metabolism'),
                swarm=server_context.get('swarm'),
                workspace_root=str(_HERE),
                base_url=os.environ.get("PERSONAL_MIRROR_BASE_URL") or "http://localhost:8421",
                display_name=os.environ.get("PERSONAL_MIRROR_MESH_NAME") or "Personal Mirror",
                node_id=os.environ.get("PERSONAL_MIRROR_MESH_NODE_ID"),
                golem_enabled=os.environ.get("PERSONAL_MIRROR_GOLEM_PROVIDER", "").strip().lower() in {"1", "true", "yes", "on"},
            )
            print("✅ Sovereign Mesh Online.")

        # 7. History (The Memory)
        if interaction_history_mod:
            from pathlib import Path as _Path
            _hist_db = _Path(__file__).parent / "personal_lattice.db"
            server_context['history'] = interaction_history_mod.InteractionHistory(db_path=_hist_db)
            print("✅ Interaction History Online.")

        # ── NEW: 8. Vessel (The Metabolic Body) ──
        if pm_vessel_mod:
            server_context['vessel'] = pm_vessel_mod.PmVessel(
                save_path=PM_DIR / "vessel_state.json"
            )
            server_context['vessel'].start()
            # Sync vessel counters from live DB — corrects stale node_count,
            # stage, and gamma that may have drifted since last restart.
            if server_context.get('lattice'):
                server_context['vessel'].sync_from_lattice(server_context['lattice'], force=True)
                server_context['vessel'].save()
            print("✅ Vessel Online — metabolic engine heartbeat active.")
        else:
            print("⚠️ Vessel module missing — using fallback status.")

        # ── NEW: 9. Oscillators (The Heartbeat) — bridges vessel + dream + lattice ──
        if pm_oscillators_mod and server_context['vessel'] and server_context.get('lattice'):
            organism = pm_oscillators_mod.PersonalMirrorOrganism(
                vessel=server_context['vessel'],
                lattice=server_context['lattice'],
                dream=server_context.get('dream'),
                mind=server_context.get('mind'),
                pedagogue=server_context.get('pedagogue'),
                engine=server_context.get('engine'),
                registry=server_context.get('registry'),
                history=server_context.get('history'),
            )
            server_context['organism'] = organism
            osc_thread = organism.run_async()
            server_context['oscillator_thread'] = osc_thread
            print("✅ Oscillators Online — 5-oscillator heartbeat started (brainstem, cortex, subconscious, pedagogue, appetite).")
        else:
            print("⚠️ Oscillators not started — vessel or lattice missing.")

        # Final Check
        if server_context['engine'] and server_context['lattice']:
            server_context['is_ready'] = True
            print("✨ [Personal Mirror] IMPLANTATION SUCCESSFUL. The organism is alive.")
        else:
            print("⚠️ [Personal Mirror] Implantation partial. Intelligence is limited.")
            
    except Exception as e:
        print(f"💥 [Personal Mirror] CRITICAL FAILURE DURING STARTUP: {e}")
        traceback.print_exc()

class MirrorHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logger.info(f"mirror.server: {fmt % args}")

    def do_OPTIONS(self):
        try:
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, X-PM-Agent-Token, X-Agent-Key, X-API-Key, Authorization",
            )
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.end_headers()
        except Exception as exc:
            if _is_client_disconnect(exc):
                logger.debug("Client disconnected during OPTIONS %s", getattr(self, "path", "?"))
                return
            raise

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        client_host = self.client_address[0] if getattr(self, "client_address", None) else None
        if not _is_authorized_agent_request("GET", path, self.headers, client_host):
            self._send_json(_authorization_failure_payload("GET", path, client_host), 401)
            return
        
        # Standardize path
        if path.startswith("/shell/"):
            path = "/" + path[7:]

        # 1. Static HTML Routing (Legacy Support)
        static_routes = {
            "/": "_HERE/shell.html",
            "/shell": "_HERE/shell.html",
            "/briefing": "_HERE/pages/morning_briefing.py", # Note: This is actually a script, but we'll route it
            "/onboarding": "_HERE/onboarding.html"
        }
        
        # (Simplified for this injection)
        if path == "/status":
            self._send_status()
            return
        if path == "/runtime/contract":
            self._handle_runtime_contract()
            return
        if path == "/daily":
            path = "/briefing"
        if path == "/obsidian/status":
            self._handle_obsidian_status()
            return
        if path == "/obsidian/notes":
            self._handle_obsidian_notes(params)
            return
        if path == "/vessel/calibration":
            self._handle_calibration()
            return
        if path == "/ask":
            self._handle_ask(params)
            return
        if path == "/verify":
            self._handle_verify(params)
            return
        if path == "/ice/verify":
            self._handle_ice_verify(params)
            return
        if path == "/dream/stream":
            self._handle_dream_stream()
            return
        if path == "/memory/search":
            self._handle_search(params)
            return
        if path == "/memory/auto-inject":
            self._handle_auto_inject(params)
            return
        if path == "/self-model":
            self._handle_self_model(params)
            return
        if path == "/world-model":
            self._handle_world_model(params)
            return
        if path == "/promises":
            self._handle_promises(params)
            return
        if path == "/experiments":
            self._handle_experiments(params)
            return
        if path == "/coherence/diagnosis":
            self._handle_coherence_diagnosis(params)
            return
        if path == "/continuity":
            self._handle_continuity(params)
            return
        if path == "/noticing":
            self._handle_noticing(params)
            return
        if path == "/trajectory":
            self._handle_trajectory(params)
            return
        if path == "/rituals/suggest":
            self._handle_ritual_suggest(params)
            return
        if path == "/tension":
            self._handle_tension()
            return
        # OMP v1 Agent Memory endpoints
        if path == "/agent/memory/v1/list":
            self._handle_agent_memory_list(params)
            return
        if path == "/agent/memory/v1/tensions":
            self._handle_agent_memory_tensions()
            return
        if path == "/agent/memory/v1/stats":
            self._handle_agent_memory_stats()
            return
        if path == "/agent/memory/v1/recall":
            self._handle_agent_memory_recall(params)
            return
        if path == "/memory/all":
            self._handle_all_memories()
            return
        if path == "/memory/action-rules":
            self._handle_action_rules()
            return
        if path in ("/nodes", "/api/nodes"):
            self._handle_nodes(params)
            return
        if path == "/thread":
            self._handle_thread(params)
            return
        if path in {"/registry", "/registry/status"}:
            self._handle_registry_status()
            return
        if path == "/registry/locks":
            self._handle_registry_locks(params)
            return
        if path == "/registry/beacons":
            self._handle_registry_beacons(params)
            return
        if path == "/registry/ledger":
            self._handle_registry_ledger(params)
            return
        if path == "/manifold" and params:
            # Only handle /manifold as API if there are query params
            # Otherwise let it fall through to serve manifold.html
            self._handle_manifold(params)
            return
        if path == "/curriculum":
            self._handle_curriculum()
            return
        if path == "/agents/status":
            self._handle_agents_status()
            return
        if path == "/api/activity":
            self._handle_activity()
            return
        if path == "/api/activity/sse":
            self._handle_activity_sse()
            return
        # OMPv2 geometric endpoints
        if path == "/omp/neighbors":
            self._handle_omp_neighbors(params)
            return
        if path == "/omp/trajectory":
            self._handle_omp_trajectory(params)
            return
        if path == "/omp/centroid":
            self._handle_omp_centroid(params)
            return
        if path == "/omp/adjacent":
            self._handle_omp_adjacent(params)
            return
        # Substrate page
        # ── OMP v2 Universal Protocol ────────────────────────────────────────────
        if path == "/omp/manifest":
            self._handle_omp_manifest()
            return
        if path == "/convergence/data":
            self._handle_convergence_data(params)
            return
        if path == "/convergence/stats":
            self._handle_convergence_stats()
            return
        # ── Wake Protocol ────────────────────────────────────────────────────────
        if path == "/wake":
            self._handle_wake(params)
            return
        if path == "/identity/chain":
            self._handle_identity_chain(params)
            return
        if path == "/continuations":
            self._handle_continuations(params)
            return
        if path in ("/substrate", "/substrate.html"):
            file_path = _HERE / "pages" / "substrate.html"
            if file_path.exists():
                self._serve_file(file_path)
            else:
                self._send_404()
            return
        # Mirror River page
        if path in ("/mirror-river", "/mirror_river", "/mirror_river.html"):
            file_path = _HERE / "mirror_river.html"
            if file_path.exists():
                self._serve_file(file_path)
            else:
                self._send_404()
            return
        # Convergence page
        if path in ("/convergence", "/convergence.html"):
            file_path = _HERE / "pages" / "convergence.html"
            if file_path.exists():
                self._serve_file(file_path)
            else:
                self._send_404()
            return
        # OMP Guide page
        if path in ("/omp-guide", "/omp_guide", "/omp_guide.html"):
            file_path = _HERE / "pages" / "omp_guide.html"
            if file_path.exists():
                self._serve_file(file_path)
            else:
                self._send_404()
            return
        # Autonomy endpoints
        if path == "/autonomy/status":
            self._handle_autonomy_status()
            return
        if path == "/metabolism/status":
            self._handle_metabolism_status()
            return
        if path == "/metabolism/jobs":
            self._handle_metabolism_jobs(params)
            return
        if path == "/approvals/inbox":
            self._handle_approvals_inbox(params)
            return
        if path == "/actions/capabilities":
            self._handle_actions_capabilities()
            return
        if path == "/actions/history":
            self._handle_actions_history(params)
            return
        if path == "/ops/overview":
            self._handle_ops_overview(params)
            return
        if path == "/mesh/manifest":
            self._handle_mesh_manifest()
            return
        if path == "/mesh/peers":
            self._handle_mesh_peers(params)
            return
        if path == "/mesh/stream":
            self._handle_mesh_stream(params)
            return
        if path == "/mesh/workers":
            self._handle_mesh_workers(params)
            return
        if path == "/mesh/queue":
            self._handle_mesh_queue(params)
            return
        if path == "/mesh/queue/events":
            self._handle_mesh_queue_events(params)
            return
        if path == "/mesh/queue/metrics":
            self._handle_mesh_queue_metrics()
            return
        if path == "/mesh/scheduler/decisions":
            self._handle_mesh_scheduler_decisions(params)
            return
        if path == "/mesh/artifacts":
            self._handle_mesh_artifact_list(params)
            return
        if path.startswith("/mesh/jobs/"):
            self._handle_mesh_job_get(path)
            return
        if path.startswith("/mesh/artifacts/"):
            self._handle_mesh_artifact_get(path, params)
            return

        # ── Discovery Proxy ─────────────────────────────────────────────────────
        # Forward browser's same-origin requests to Main Golem (port 8420)
        # This avoids CORS/sandbox issues when browser JS can't reach localhost
        _PROXY_PATHS = {
            "/discover/anomalies":   "/discover/anomalies",
            "/discover/analogies":   "/discover/analogies",
            "/discover/hypotheses":  "/discover/hypotheses",
            "/discover/bridges":     "/discover/bridges",
            "/discover/voids":       "/discover/voids",
            "/discover/invariants":  "/discover/invariants",
            "/discover/fractal":     "/discover/fractal",
            "/agents":               "/agents",
            "/agents/register":       "/agents/register",
        }
        _MAIN_GOLEM = "http://localhost:8420"
        for _pp in _PROXY_PATHS:
            if path.startswith(_pp):
                import urllib.request as _ur
                _qs = ("?" + self.path.split("?", 1)[1]) if "?" in self.path else ""
                _target = f"{_MAIN_GOLEM}{_PROXY_PATHS[_pp]}{_qs}"
                # Forward auth headers from incoming request
                _hdrs = {"Accept": "application/json", "Content-Type": "application/json"}
                _inh = self.headers
                for _hk in ("X-Agent-Key", "Authorization", "X-API-Key"):
                    _hv = _inh.get(_hk)
                    if _hv:
                        _hdrs[_hk] = _hv
                try:
                    _req = _ur.Request(_target, headers=_hdrs)
                    with _ur.urlopen(_req, timeout=20) as _r:
                        _data = _r.read()
                        self._write_response(_data, code=200, content_type="application/json")
                except Exception as _ex:
                    if _is_client_disconnect(_ex):
                        logger.debug("Client disconnected during discovery proxy %s", self.path)
                        return
                    self._send_json({"error": str(_ex)}, 502)
                return

        # Feed endpoint (returns recent activity)
        if path == "/feed":
            self._send_json({"feed": [], "message": "Feed aggregation not yet implemented"})
            return

        # Wallet state — serve PM's own 88-node lattice instead of proxying to Main Golem
        if path.startswith("/wallet_state"):
            _engine = server_context.get("engine")
            _lattice = server_context.get("lattice")
            if not _lattice:
                self._send_json({"error": "PM lattice unavailable"}, 502)
                return

            # Get status directly from engine/lattice (no HTTP recursion)
            _engine_status = _engine.get_status() if _engine and hasattr(_engine, "get_status") else {}
            _lattice_status = _lattice.get_status() if _lattice and hasattr(_lattice, "get_status") else {}
            _raw = {**_engine_status, **_lattice_status}

            # Get nodes directly from lattice
            try:
                _pm_nodes = _lattice.list_nodes(limit=500)
            except Exception:
                _pm_nodes = []

            _org = {
                "stage": _raw.get("stage", "adult"),
                "mirror_M": _raw.get("mirror_M", _raw.get("mirror", 0.33)),
                "node_count": _raw.get("total_nodes", len(_pm_nodes)),
                "lambda": _raw.get("lambda", 0.0),
                "gamma": _raw.get("gamma", 0.0),
                "theta": _raw.get("theta", 0.0),
                "breathing_state": _raw.get("breathing_state", "BREATHING"),
                "energy": int(_raw.get("avg_energy", 0.5) * 100),
            }
            _tension_count = _raw.get("tension_nodes", 0)
            _total_nodes = _raw.get("total_nodes", len(_pm_nodes))

            # Build map_nodes in the format the 3D lattice map expects
            _map_nodes = []
            for _n in _pm_nodes:
                _prov = _n.get("provenance", {})
                if isinstance(_prov, dict):
                    _prov_chain = [_prov.get("source", "")]
                elif isinstance(_prov, list):
                    _prov_chain = _prov
                else:
                    _prov_chain = [str(_prov)] if _prov else []

                _cat = _n.get("category", "note")
                _is_immutable = _cat in ("axiom", "definition", "constants")
                _is_tension = _cat == "tension"

                _map_nodes.append({
                    "id": _n.get("id", ""),
                    "text": (_n.get("text") or "").strip(),
                    "domain": _n.get("domain", "hermes"),
                    "weight": float(_n.get("weight", 0.5)),
                    "resonance": float(_n.get("energy", 0.5)),
                    "immutable": _is_immutable,
                    "in_tension": _is_tension,
                    "type": _cat,
                    "provenance": _n.get("source", "hermes"),
                    "verification_chain": [c for c in _prov_chain if c],
                    "access_count": _n.get("access_count", 0),
                })

            # Build organism block
            _wallet_org = {
                "stage": _org.get("stage", "seed"),
                "mirror_M": _org.get("mirror_M", 0.0),
                "nodes": _org.get("node_count", _total_nodes),
                "node_count": _org.get("node_count", _total_nodes),
                "lambda": _org.get("lambda", 0.0),
                "gamma": _org.get("gamma", 0.0),
                "theta": _org.get("theta", 0.0),
                "silence_rate": 0.0,  # PM doesn't compute silence rate
                "tensions_active": _tension_count,
                "breathing_state": _org.get("breathing_state", "FROZEN"),
                "age_hours": 0.0,  # PM doesn't track organism age
                "energy": _org.get("energy", 0.0),
            }

            _wallet = {
                "organism": _wallet_org,
                "overnight_crystallizations": [],
                "overnight_resolved": 0,
                "bridges": [],
                "tensions": [],
                "tensions_active": _tension_count,
                "map_nodes": _map_nodes,
                "map_links": [],   # PM has no proximity/link data between nodes
                "generated_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "note": f"Personal Mirror wallet_state — {_total_nodes} nodes, {len(_map_nodes)} in lattice map",
                "generated_at": time.time(),
            }

            self._send_json(_wallet)
            return

        # Network dashboard (serve from pages/)
        if path == "/network":
            file_path = _HERE / "pages" / "network_dashboard.html"
            if file_path.exists():
                self._serve_file(file_path)
            else:
                self._send_json({"error": "Network dashboard not available"}, 404)
            return

        # ── Dynamic page routes via pages.py ──────────────────────────────────
        dynamic_html = render_dynamic_page(path)
        if dynamic_html is not None:
            self._send_html(dynamic_html)
            return

        # Static routes: serve from _HERE root (not pages/)
        if path in ("/", "/shell", "/shell.html", "/onboarding"):
            filename = path.lstrip("/") or "shell.html"
            file_path = _HERE / filename
            if file_path.exists() and file_path.is_file():
                self._serve_file(file_path)
            elif (_HERE / (filename + ".html")).exists():
                self._serve_file(_HERE / (filename + ".html"))
            else:
                self._send_404()
            return

        # Default: serve from pages/
        file_to_serve = path.lstrip('/')

        # Route aliases: clean URL → actual file
        _ALIASES = {
            "map":                 "lattice_map.html",
            "lattice-graph":       "lattice_graph.html",
            "lattice_graph":       "lattice_graph.html",
            "lattice_map":         "lattice_map.html",
            "glass-mind":          "glass_mind.html",
            "glass_mind":          "glass_mind.html",
            "oracle-chat":         "oracle_chat.html",
            "oracle_chat":         "oracle_chat.html",
            "wallet-canvas":       "golem_wallet_live.html",
            "wallet_canvas":       "golem_wallet_live.html",
            "manifold":            "manifold.html",
            "discover/dashboard":   "discovery_dashboard.html",
            "discover/triage":     "discovery_dashboard.html",
            "hypotheses-view":     "hypotheses_view.html",
            "hypotheses_view":     "hypotheses_view.html",
            "dream-theatre":       "dream_theatre.html",
            "dream_theatre":       "dream_theatre.html",
            "wake":                "wake.html",
            "temporal-self":       "temporal_self.html",
            "temporal_self":       "temporal_self.html",
            "convergence":         "convergence.html",
            "omp-guide":           "omp_guide.html",
            "omp_guide":           "omp_guide.html",
            "anomalies-view":      "anomalies_view.html",
            "anomalies_view":      "anomalies_view.html",
            "analogies-view":      "analogies_view.html",
            "analogies_view":      "analogies_view.html",
            "mind-map":            "mind_map.html",
            "mind_map":            "mind_map.html",
            "soul":                "soul.html",
            "pulse":               "pulse.html",
        }
        if file_to_serve in _ALIASES:
            file_to_serve = _ALIASES[file_to_serve]

        # Serve omniscience.html from _HERE root (not pages/)
        if file_to_serve in ("omniscience", "omniscience.html"):
            _f = _HERE / "omniscience.html"
            if _f.exists():
                self._serve_file(_f)
                return
            self._send_404()
            return

        file_path = _HERE / "pages" / file_to_serve
        if file_path.exists() and file_path.is_file():
            self._serve_file(file_path)
        elif (file_to_serve + ".html") and (_HERE / "pages" / (file_to_serve + ".html")).exists():
            self._serve_file(_HERE / "pages" / (file_to_serve + ".html"))
        else:
            self._send_404()

    def do_POST(self):
        if not server_context['is_ready']:
            self._send_json({"error": "Organism is not yet fully initialized"}, 503)
            return
        path = urlparse(self.path).path
        client_host = self.client_address[0] if getattr(self, "client_address", None) else None
        if not _is_authorized_agent_request("POST", path, self.headers, client_host):
            self._send_json(_authorization_failure_payload("POST", path, client_host), 401)
            return

        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            body_str = body.decode('utf-8') if isinstance(body, bytes) else body
            data = json.loads(body_str) if str(body_str).strip() else {}
            
            if path == "/memory/store":
                self._handle_store(data)
            elif path == "/self-model/store":
                self._handle_self_model_store(data)
            elif path == "/promises/store":
                self._handle_promises_store(data)
            elif path == "/experiments/start":
                self._handle_experiments_start(data)
            elif path == "/experiments/close":
                self._handle_experiments_close(data)
            elif path == "/rituals/run":
                self._handle_run_ritual(data)
            elif path == "/noticing/scan":
                self._handle_noticing_scan(data)
            elif path == "/trajectory/refresh":
                self._handle_trajectory_refresh(data)
            elif path == "/continuity/feedback":
                self._handle_continuity_feedback(data)
            elif path == "/chat/ask":
                self._handle_chat(data)
            elif path == "/mentions/resolve":
                self._handle_mentions_resolve(data)
            elif path == "/swarm/submit":
                self._handle_swarm_submit(data)
            elif path == "/registry/lock":
                self._handle_registry_lock(data)
            elif path == "/registry/heartbeat":
                self._handle_registry_heartbeat(data)
            elif path == "/registry/unlock":
                self._handle_registry_unlock(data)
            elif path == "/registry/beacon":
                self._handle_registry_beacon(data)
            elif path == "/ice/verify":
                self._handle_ice_verify(data)
            elif path == "/memory/resolve-tension":
                self._handle_resolve_tension(data)
            elif path == "/dream/trigger":
                self._handle_dream_trigger(data)
            elif path == "/omp/join":
                self._handle_omp_join(data)
            elif path == "/session/begin":
                self._handle_session_begin(data)
            elif path == "/session/end":
                self._handle_session_end(data)
            elif path == "/memory/continuation":
                self._handle_store_continuation(data)
            elif path == "/memory/insight":
                self._handle_store_insight(data)
            elif path == "/memory/question":
                self._handle_store_question(data)
            elif path == "/continuation/resolve":
                self._handle_resolve_continuation(data)
            elif path == "/autonomy/trigger":
                self._handle_autonomy_trigger(data)
            elif path == "/metabolism/trigger":
                self._handle_metabolism_trigger(data)
            elif path == "/actions/dispatch":
                self._handle_action_dispatch(data)
            elif path == "/approvals/resolve":
                self._handle_approvals_resolve(data)
            elif path == "/mesh/handshake":
                self._handle_mesh_handshake(data)
            elif path == "/mesh/peers/sync":
                self._handle_mesh_peers_sync(data)
            elif path == "/mesh/lease/acquire":
                self._handle_mesh_lease_acquire(data)
            elif path == "/mesh/lease/heartbeat":
                self._handle_mesh_lease_heartbeat(data)
            elif path == "/mesh/lease/release":
                self._handle_mesh_lease_release(data)
            elif path == "/mesh/jobs/submit":
                self._handle_mesh_job_submit(data)
            elif path == "/mesh/jobs/schedule":
                self._handle_mesh_job_schedule(data)
            elif path.startswith("/mesh/jobs/") and path.endswith("/resume-from-checkpoint"):
                self._handle_mesh_job_resume_from_checkpoint(path, data)
            elif path.startswith("/mesh/jobs/") and path.endswith("/resume"):
                self._handle_mesh_job_resume(path, data)
            elif path.startswith("/mesh/jobs/") and path.endswith("/restart"):
                self._handle_mesh_job_restart(path, data)
            elif path.startswith("/mesh/jobs/") and path.endswith("/cancel"):
                self._handle_mesh_job_cancel(path, data)
            elif path == "/mesh/workers/register":
                self._handle_mesh_worker_register(data)
            elif path.startswith("/mesh/workers/") and path.endswith("/heartbeat"):
                self._handle_mesh_worker_heartbeat(path, data)
            elif path.startswith("/mesh/workers/") and path.endswith("/poll"):
                self._handle_mesh_worker_poll(path, data)
            elif path.startswith("/mesh/workers/") and path.endswith("/claim"):
                self._handle_mesh_worker_claim(path, data)
            elif path == "/mesh/queue/replay":
                self._handle_mesh_queue_replay(data)
            elif path == "/mesh/queue/ack-deadline":
                self._handle_mesh_queue_ack_deadline(data)
            elif path.startswith("/mesh/jobs/attempts/") and path.endswith("/heartbeat"):
                self._handle_mesh_attempt_heartbeat(path, data)
            elif path.startswith("/mesh/jobs/attempts/") and path.endswith("/complete"):
                self._handle_mesh_attempt_complete(path, data)
            elif path.startswith("/mesh/jobs/attempts/") and path.endswith("/fail"):
                self._handle_mesh_attempt_fail(path, data)
            elif path == "/mesh/artifacts/publish":
                self._handle_mesh_artifact_publish(data)
            elif path == "/mesh/artifacts/purge":
                self._handle_mesh_artifact_purge(data)
            elif path == "/mesh/agents/handoff":
                self._handle_mesh_handoff(data)
            else:
                self._send_json({"error": "Unknown POST endpoint"}, 404)
        except Exception as e:
            if _is_client_disconnect(e):
                logger.info("Client disconnected during POST %s", self.path)
                return
            logger.error(f"POST error: {e}")
            self._send_json({"error": str(e)}, 500)

    # --- Internal Handlers ---

    def _handle_calibration(self):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            self._send_json(registry.get_vessel_state())
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _send_status(self):
        if not server_context['is_ready']:
            self._send_json({"status": "initializing", "ready": False}, 503)
            return

        from routes.reporting_routes import status_response

        payload = status_response(
            lattice=server_context.get('lattice'),
            metabolism=server_context.get('metabolism'),
            vessel=server_context.get('vessel'),
            engine=server_context.get('engine'),
            registry=server_context.get('registry'),
            dream=server_context.get('dream'),
            runtime_contract=_runtime_contract_payload(),
        )
        self._send_json(payload)

    def _handle_runtime_contract(self):
        payload = _runtime_contract_payload()
        payload["ready"] = bool(server_context.get("is_ready"))
        self._send_json(payload)

    def _handle_obsidian_status(self):
        """GET /obsidian/status — Return Obsidian Synapse sync status."""
        try:
            import sqlite3
            db_path = _HERE / "personal_lattice.db"
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            snapshot = load_obsidian_snapshot(conn)
            conn.close()

            status = {
                "connected": snapshot["connected"],
                "running": snapshot["running"],
                "status": snapshot["status"],
                "stale": snapshot["stale"],
                "last_event_seq": snapshot["last_event_seq"],
                "last_sync": snapshot["last_sync_at"],
                "last_activity": snapshot["last_activity_at"],
                "last_file": snapshot["last_file"],
                "vault_path": snapshot["vault_path"],
                "total_exports": snapshot["exports"],
                "last_export": snapshot["last_export_at"],
            }
            status["dependency"] = _obsidian_dependency_payload(snapshot.get("vault_path"))
            self._send_json(status)
        except Exception as e:
            self._send_json({"error": str(e), "connected": False}, 500)

    def _handle_obsidian_notes(self, params):
        """GET /obsidian/notes — Return recent synced Obsidian notes with compact provenance."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable", "notes": [], "count": 0}, 503)
            return

        limit = int((params.get("limit", ["6"])[0] or "6"))
        limit = min(max(limit, 1), 24)
        scan = int((params.get("scan", ["240"])[0] or "240"))
        scan = min(max(scan, limit), 1200)

        def _compact_excerpt(text: str, limit_chars: int = 220) -> str:
            sample = " ".join((text or "").strip().split())
            if len(sample) <= limit_chars:
                return sample
            return sample[: limit_chars - 3] + "..."

        def _title_from(note: dict) -> str:
            prov = note.get("provenance") or {}
            title = (prov.get("title") or "").strip()
            if title:
                return title
            text = (note.get("text") or "").strip()
            for line in text.splitlines():
                candidate = line.strip().lstrip("#").strip()
                if candidate:
                    return candidate[:80]
            return (prov.get("filename") or note.get("domain") or note.get("short_ref") or "Vault Note").strip()

        try:
            nodes = lattice.list_nodes(limit=scan)
            obsidian_nodes = [
                node for node in nodes
                if str(node.get("source", "")).startswith("obsidian_synapse")
                and not bool(node.get("archived"))
            ]
            notes = []
            for node in obsidian_nodes[:limit]:
                prov = node.get("provenance") or {}
                notes.append(
                    {
                        "id": node.get("id"),
                        "short_ref": node.get("short_ref"),
                        "ref": node.get("short_ref") or node.get("id"),
                        "title": _title_from(node),
                        "excerpt": _compact_excerpt(node.get("text") or ""),
                        "category": node.get("category", "belief"),
                        "domain": node.get("domain", "obsidian"),
                        "created_at": node.get("created_at"),
                        "weight": float(node.get("weight", 0.5)),
                        "energy": float(node.get("energy", 0.5)),
                        "verification_status": node.get("verification_status"),
                        "filename": prov.get("filename"),
                        "file_path": prov.get("file_path") or prov.get("path"),
                        "vault_path": prov.get("vault_path"),
                        "tags": list(prov.get("obsidian_tags") or []),
                        "linked_axioms": list(prov.get("linked_axioms") or []),
                        "linked_axiom_details": list(prov.get("linked_axiom_details") or []),
                    }
                )
            self._send_json({"notes": notes, "count": len(notes), "scanned": len(obsidian_nodes)})
        except Exception as e:
            logger.error(f"Obsidian notes error: {e}")
            self._send_json({"error": str(e), "notes": [], "count": 0}, 500)

    def _handle_ask(self, params):
        query = params.get("q", [""])[0]
        if not query:
            self._send_json({"error": "Missing query"}, 400)
            return

        # Try PersonalMind verify_belief first, fall back to lattice search
        if server_context['mind']:
            try:
                result = server_context['mind'].verify_belief(query)
                self._send_json({"answer": result})
                return
            except Exception:
                pass

        # Fallback: semantic search on lattice
        if server_context['lattice']:
            try:
                results = server_context['lattice'].recall_semantic(query, max_results=5)
                self._send_json({"results": results, "count": len(results)})
                return
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
        else:
            self._send_json({"error": "Lattice unavailable"}, 503)

    def _handle_verify(self, params):
        """Verify a claim against personal knowledge — returns verdict + evidence."""
        claim = params.get("claim", params.get("text", [""]))[0]
        if not claim:
            self._send_json({"error": "no claim provided"}, 400)
            return

        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return

        try:
            # Semantic search for supporting and contradicting nodes
            results = lattice.recall_semantic(claim, max_results=20)
            q_words = set(claim.lower().split())

            support_nodes = []
            contradict_nodes = []

            for r in results:
                text = r.get("text", "")
                w = r.get("weight", 0.5) or 0.5
                energy = r.get("energy", 0.5) or 0.5
                r_words = set(text.lower().split())
                overlap = len(q_words & r_words) / max(len(q_words), 1)
                score = overlap * w * energy

                if score >= 0.10:
                    entry = {"text": text[:200], "category": r.get("category", "unknown"), "score": round(score, 3)}
                    # Simple contradiction detection: negate key words
                    if any(f"not {w}" in text.lower() or f"never {w}" in text.lower() for w in list(q_words)[:3]):
                        contradict_nodes.append(entry)
                    else:
                        support_nodes.append(entry)

            support_nodes.sort(key=lambda x: x["score"], reverse=True)
            contradict_nodes.sort(key=lambda x: x["score"], reverse=True)

            best_sup = support_nodes[0]["score"] if support_nodes else 0
            best_con = contradict_nodes[0]["score"] if contradict_nodes else 0

            if best_sup > 0.3 and best_con < 0.15:
                verdict = "supported"
                confidence = best_sup
            elif best_con > 0.3 and best_sup < 0.15:
                verdict = "contradicted"
                confidence = best_con
            elif best_sup > 0.2 and best_con > 0.2:
                verdict = "contested"
                confidence = max(best_sup, best_con)
            else:
                verdict = "unknown"
                confidence = 0.0

            self._send_json({
                "verdict": verdict,
                "confidence": round(confidence, 3),
                "claim": claim,
                "supporting": support_nodes[:5],
                "contradicting": contradict_nodes[:3],
                "total_nodes": len(results),
            })
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_search(self, params):
        query = params.get("q", [""])[0]
        if not server_context['lattice']:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            include_embeddings = params.get("include_embeddings", ["0"])[0].lower() in ("1", "true", "yes")
            max_results = int(params.get("limit", ["10"])[0])
            results = server_context['lattice'].recall_semantic(query, max_results=max_results, include_embeddings=include_embeddings)
            self._send_json({"results": results, "count": len(results)})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_auto_inject(self, params):
        lattice = server_context.get("lattice")
        registry = server_context.get("registry")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            query = params.get("q", params.get("query", [""]))[0]
            limit = int(params.get("limit", [20])[0])
            agent_id = params.get("agent", params.get("agent_id", ["claude-code"]))[0]
            continuity_project = _continuity_project_id(
                lattice,
                {
                    "agent_id": agent_id,
                    "session_id": params.get("session_id", [""])[0],
                    "project_id": params.get("project_id", params.get("project", [""]))[0],
                },
            )
            resolution = resolve_mentions_payload(
                lattice,
                {
                    "query": query,
                    "agent_id": agent_id,
                    "session_id": params.get("session_id", [""])[0],
                    "project_id": continuity_project,
                    "limit": limit,
                },
            )
            search_query = resolution.get("query_without_mentions") or query
            payload = lattice.build_auto_inject_context(
                search_query,
                limit=max(1, min(limit, 30)),
                project_id=continuity_project,
                agent_id=agent_id,
            )
            payload["project_scope"] = {
                "resolved_project_id": continuity_project or "",
                "agent_id": agent_id,
                "session_id": params.get("session_id", [""])[0],
            }
            if resolution.get("mentions"):
                payload["resolved_mentions"] = [item for item in resolution.get("mentions", []) if item.get("resolved")]
                payload["warnings"] = resolution.get("warnings", [])
                payload["expanded_query"] = resolution.get("expanded_query", query)
                payload["context_blocks"] = resolution.get("context_blocks", [])
                payload["resolved_node_ids"] = resolution.get("resolved_node_ids", [])
            if registry:
                payload["vessel"] = registry.get_vessel_state()
                payload["locks"] = registry.list_locks(limit=12)
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_thread(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        project_id = params.get("project_id", params.get("project", [""]))[0]
        if not project_id:
            self._send_json({"error": "project_id is required"}, 400)
            return
        try:
            limit = int(params.get("limit", [40])[0])
            min_energy = float(params.get("min_energy", [0.55])[0])
            thread = lattice.recall_thread(project_id, limit=max(1, min(limit, 100)), min_energy=min_energy)
            self._send_json({"project_id": project_id, "count": len(thread), "thread": thread})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_self_model(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            limit = int(params.get("limit", [50])[0])
            kinds = params.get("kind") or params.get("category") or params.get("kinds")
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            include_archived = str(params.get("include_archived", ["false"])[0]).lower() in {"1", "true", "yes", "on"}
            payload = lattice.get_self_model(
                kinds=kinds,
                project_id=project_id,
                limit=max(1, min(limit, 500)),
                include_archived=include_archived,
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_world_model(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            limit = int(params.get("limit", [10])[0])
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            payload = lattice.get_world_model_snapshot(
                project_id=project_id,
                limit=max(1, min(limit, 50)),
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_promises(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            limit = int(params.get("limit", [50])[0])
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            include_archived = str(params.get("include_archived", ["false"])[0]).lower() in {"1", "true", "yes", "on"}
            payload = lattice.get_promises(
                project_id=project_id,
                limit=max(1, min(limit, 200)),
                include_archived=include_archived,
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_experiments(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            limit = int(params.get("limit", [50])[0])
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            include_archived = str(params.get("include_archived", ["false"])[0]).lower() in {"1", "true", "yes", "on"}
            payload = lattice.get_experiments(
                project_id=project_id,
                limit=max(1, min(limit, 200)),
                include_archived=include_archived,
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_coherence_diagnosis(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            limit = int(params.get("limit", [120])[0])
            payload = lattice.compute_coherence_diagnosis(
                project_id=project_id,
                limit=max(10, min(limit, 300)),
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_continuity(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            agent_id = (params.get("agent_id", params.get("agent", [""]))[0] or "").strip() or None
            limit = int(params.get("limit", ["3"])[0])
            from routes.reporting_routes import continuity_response

            payload = continuity_response(
                lattice,
                project_id=project_id,
                agent_id=agent_id,
                limit=max(1, min(limit, 6)),
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_continuity_feedback(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            project_id = ((data.get("project_id") or data.get("project") or "").strip() or None)
            agent_id = ((data.get("agent_id") or data.get("agent") or "").strip() or None)
            query = (data.get("query") or "").strip()
            previous = lattice.continuity_brief(
                project_id=project_id,
                agent_id=agent_id,
                limit=max(1, min(int(data.get("limit") or 3), 6)),
            )
            payload = lattice.record_continuity_feedback(
                decision=(data.get("decision") or "").strip(),
                suggested_step=(data.get("suggested_step") or data.get("best_next_step") or "").strip(),
                last_intent=(data.get("last_intent") or "").strip(),
                project_id=project_id,
                agent_id=agent_id,
                query=query,
                evidence_ids=list(data.get("evidence_ids") or []),
                response_posture=data.get("response_posture") if isinstance(data.get("response_posture"), dict) else None,
                posture_feedback=(data.get("posture_feedback") or "").strip(),
                outcome=(data.get("outcome") or "").strip(),
                source=(data.get("source") or "api:continuity_feedback").strip() or "api:continuity_feedback",
            )
            continuity = lattice.continuity_brief(
                project_id=project_id,
                agent_id=agent_id,
                limit=max(1, min(int(data.get("limit") or 3), 6)),
            )
            payload["continuity"] = continuity
            payload["delta"] = _continuity_feedback_delta(previous, continuity)
            payload["answer_support"] = {
                "continuity": _continuity_answer_support(continuity, query=query),
            }
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_noticing(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            limit = int(params.get("limit", [20])[0])
            silent_only = str(params.get("silent_only", ["true"])[0]).lower() in {"1", "true", "yes", "on"}
            nodes = lattice.list_noticing(
                project_id=project_id,
                limit=max(1, min(limit, 100)),
                silent_only=silent_only,
            )
            self._send_json({"nodes": nodes, "count": len(nodes), "project_id": project_id or ""})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_trajectory(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            horizon_days = int(params.get("horizon_days", params.get("days", ["30"]))[0])
            persist = str(params.get("persist", ["false"])[0]).lower() in {"1", "true", "yes", "on"}
            from routes.reporting_routes import trajectory_response

            payload = trajectory_response(
                lattice,
                project_id=project_id,
                horizon_days=horizon_days,
                persist=persist,
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_ritual_suggest(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            project_id = (params.get("project_id", params.get("project", [""]))[0] or "").strip() or None
            payload = lattice.suggest_ritual(project_id=project_id)
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_ice_verify(self, payload):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            if isinstance(payload, dict):
                data = payload
            else:
                data = {key: values[0] for key, values in payload.items() if values}
            node_id = (data.get("node_id") or "").strip()
            text = (data.get("text") or data.get("claim") or "").strip()
            verifier = (data.get("verifier") or data.get("agent_id") or data.get("source") or "api:ice").strip()
            persist = str(data.get("persist", "false")).lower() in {"1", "true", "yes", "on"}
            if node_id:
                result = lattice.run_ice_protocol(node_id, verifier=verifier)
            elif persist and text:
                node = lattice.store_node(
                    text,
                    domain=(data.get("domain") or "personal").strip() or "personal",
                    category=(data.get("category") or "belief").strip() or "belief",
                    weight=float(data.get("weight", 0.62)),
                    energy=float(data.get("energy", 0.72)),
                    source=(data.get("source") or verifier or "api:ice").strip() or "api:ice",
                    provenance={
                        "project_id": (data.get("project_id") or data.get("domain") or "personal").strip() or "personal",
                        "api_ingest": True,
                    },
                    verified_by=verifier,
                    verification_status="pending",
                    auto_ice=False,
                )
                result = lattice.run_ice_protocol(node["id"], verifier=verifier)
            elif text:
                result = lattice.preview_ice(
                    text,
                    domain=(data.get("domain") or "personal").strip() or "personal",
                    category=(data.get("category") or "belief").strip() or "belief",
                    source=(data.get("source") or verifier or "api:ice").strip() or "api:ice",
                    verifier=verifier,
                    energy=float(data.get("energy", 0.72)),
                    weight=float(data.get("weight", 0.62)),
                )
            else:
                self._send_json({"error": "text or node_id is required"}, 400)
                return
            self._send_json(result)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_registry_status(self):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            self._send_json(registry.get_registry_snapshot(limit=20))
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_registry_locks(self, params):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            agent_id = params.get("agent_id", [""])[0] or None
            limit = int(params.get("limit", [50])[0])
            locks = registry.list_locks(agent_id=agent_id, limit=max(1, min(limit, 200)))
            self._send_json({"locks": locks, "count": len(locks)})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_registry_beacons(self, params):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            limit = int(params.get("limit", [20])[0])
            beacons = registry.get_beacons(limit=max(1, min(limit, 100)))
            self._send_json({"beacons": beacons, "count": len(beacons)})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_registry_ledger(self, params):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            agent_id = params.get("agent_id", [""])[0] or None
            limit = int(params.get("limit", [50])[0])
            ledger = registry.get_ledger(agent_id=agent_id, limit=max(1, min(limit, 200)))
            self._send_json({"ledger": ledger, "count": len(ledger)})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_registry_lock(self, data):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            payload = registry.acquire_lock(
                data.get("resource") or data.get("file") or data.get("task") or "",
                agent_id=data.get("agent_id") or data.get("source") or "",
                agent_name=data.get("agent_name"),
                session_id=data.get("session_id"),
                reason=data.get("reason", ""),
                ttl_seconds=int(data.get("ttl_seconds", data.get("ttl", 900))),
                lock_type=data.get("lock_type", "task"),
                metadata=data.get("metadata") if isinstance(data.get("metadata"), dict) else {},
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_registry_heartbeat(self, data):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            payload = registry.heartbeat_lock(
                data.get("resource") or data.get("file") or data.get("task") or "",
                agent_id=data.get("agent_id"),
                session_id=data.get("session_id"),
                lock_token=data.get("lock_token"),
                ttl_seconds=int(data.get("ttl_seconds", data.get("ttl", 900))),
                metadata=data.get("metadata") if isinstance(data.get("metadata"), dict) else {},
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_registry_unlock(self, data):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            force = str(data.get("force", "false")).lower() in {"1", "true", "yes", "on"}
            payload = registry.release_lock(
                data.get("resource") or data.get("file") or data.get("task") or "",
                agent_id=data.get("agent_id"),
                session_id=data.get("session_id"),
                lock_token=data.get("lock_token"),
                force=force,
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_registry_beacon(self, data):
        registry = server_context.get("registry")
        if not registry:
            self._send_json({"error": "Registry unavailable"}, 503)
            return
        try:
            payload = registry.emit_beacon(
                data.get("text") or data.get("beacon") or data.get("signal") or "",
                agent_id=data.get("agent_id") or data.get("source") or "unknown-agent",
                agent_name=data.get("agent_name"),
                domain=data.get("domain", "swarm"),
                energy=float(data.get("energy", 0.96)),
                metadata=data.get("metadata") if isinstance(data.get("metadata"), dict) else {},
            )
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_swarm_submit(self, data):
        swarm = server_context.get("swarm")
        if not swarm:
            self._send_json({"error": "Swarm unavailable"}, 503)
            return
        try:
            payload = swarm.submit(data)
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_store(self, data):
        # UNIFIED: personal_lattice — mirror_engine is read-only
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return

        val = (data.get("text") or data.get("value") or "").strip()
        if not val:
            self._send_json({"error": "text or value is required"}, 400)
            return

        # Determine contributor: explicit > agent: prefix > user
        contributor = (data.get("contributor") or data.get("source") or "user").strip()
        if data.get("agent") and not contributor.startswith("agent:"):
            contributor = f"agent:{data['agent']}"

        try:
            result = lattice.store_node(
                val,
                domain=data.get("domain", "personal"),
                category=data.get("category", "belief"),
                weight=float(data.get("weight", 0.62)),
                energy=float(data.get("energy", 0.68)),
                source=contributor,
                provenance={
                    "project_id": data.get("project_id") or data.get("domain") or "personal",
                    "contributor": contributor,
                },
                parent_id=data.get("parent_id"),
                project_id=data.get("project_id"),
            )
            result["contributor"] = contributor
            self._send_json(result)
        except Exception as e:
            logger.error(f"Store error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_self_model_store(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return

        text = (data.get("text") or data.get("value") or "").strip()
        kind = (data.get("kind") or data.get("category") or "").strip().lower()
        if not text:
            self._send_json({"error": "text or value is required"}, 400)
            return
        if not kind:
            self._send_json({"error": "kind or category is required"}, 400)
            return

        source = (data.get("source") or data.get("contributor") or "user").strip() or "user"
        try:
            result = lattice.store_self_model(
                text,
                kind=kind,
                domain=(data.get("domain") or "personal").strip() or "personal",
                source=source,
                project_id=(data.get("project_id") or data.get("project") or "").strip() or None,
                status=(data.get("status") or "active").strip() or "active",
                confidence=data.get("confidence"),
                evidence=data.get("evidence"),
                tags=data.get("tags"),
                provenance=dict(data.get("provenance") or {}),
                weight=float(data["weight"]) if data.get("weight") is not None else None,
                energy=float(data["energy"]) if data.get("energy") is not None else None,
                verified=bool(data.get("verified", False)),
                verified_by=(data.get("verified_by") or data.get("source") or "").strip() or None,
                timestamp=(data.get("timestamp") or "").strip() or None,
            )
            self._send_json(result)
        except Exception as e:
            logger.error(f"Self-model store error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_promises_store(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return

        text = (data.get("text") or data.get("value") or "").strip()
        if not text:
            self._send_json({"error": "text or value is required"}, 400)
            return

        source = (data.get("source") or data.get("contributor") or "user").strip() or "user"
        try:
            result = lattice.store_promise(
                text,
                domain=(data.get("domain") or "personal").strip() or "personal",
                source=source,
                project_id=(data.get("project_id") or data.get("project") or "").strip() or None,
                status=(data.get("status") or "active").strip() or "active",
                confidence=data.get("confidence"),
                evidence=data.get("evidence"),
                tags=data.get("tags"),
                provenance=dict(data.get("provenance") or {}),
                weight=float(data["weight"]) if data.get("weight") is not None else None,
                energy=float(data["energy"]) if data.get("energy") is not None else None,
                verified=bool(data.get("verified", False)),
                verified_by=(data.get("verified_by") or data.get("source") or "").strip() or None,
                timestamp=(data.get("timestamp") or "").strip() or None,
            )
            self._send_json(result)
        except Exception as e:
            logger.error(f"Promise store error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_experiments_start(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return

        text = (data.get("text") or data.get("value") or data.get("name") or data.get("label") or "").strip()
        hypothesis = (data.get("hypothesis") or "").strip()
        smallest_test = (data.get("smallest_test") or data.get("test") or "").strip()
        if not (text or hypothesis or smallest_test):
            self._send_json({"error": "text, hypothesis, or smallest_test is required"}, 400)
            return

        source = (data.get("source") or data.get("contributor") or "user").strip() or "user"
        try:
            result = lattice.store_experiment(
                text,
                hypothesis=hypothesis or None,
                trigger=(data.get("trigger") or "").strip() or None,
                smallest_test=smallest_test or None,
                review_at=(data.get("review_at") or data.get("review_date") or "").strip() or None,
                success_signal=(data.get("success_signal") or "").strip() or None,
                failure_signal=(data.get("failure_signal") or "").strip() or None,
                domain=(data.get("domain") or "personal").strip() or "personal",
                source=source,
                project_id=(data.get("project_id") or data.get("project") or "").strip() or None,
                status=(data.get("status") or "active").strip() or "active",
                confidence=data.get("confidence"),
                evidence=data.get("evidence"),
                tags=data.get("tags"),
                provenance=dict(data.get("provenance") or {}),
                weight=float(data["weight"]) if data.get("weight") is not None else None,
                energy=float(data["energy"]) if data.get("energy") is not None else None,
                verified=bool(data.get("verified", False)),
                verified_by=(data.get("verified_by") or data.get("source") or "").strip() or None,
                timestamp=(data.get("timestamp") or "").strip() or None,
            )
            self._send_json(result)
        except Exception as e:
            logger.error(f"Experiment start error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_experiments_close(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return

        experiment_id = (data.get("experiment_id") or data.get("id") or "").strip()
        if not experiment_id:
            self._send_json({"error": "experiment_id or id is required"}, 400)
            return
        outcome = (data.get("outcome") or data.get("result") or "").strip()
        if not outcome:
            self._send_json({"error": "outcome or result is required"}, 400)
            return

        try:
            payload = lattice.close_experiment(
                experiment_id,
                outcome=outcome,
                learning=(data.get("learning") or data.get("summary") or data.get("note") or "").strip(),
                result_summary=(data.get("result_summary") or data.get("result") or "").strip(),
                source=(data.get("source") or "api:experiment").strip() or "api:experiment",
                reviewed_at=(data.get("reviewed_at") or data.get("closed_at") or "").strip() or None,
                review_note=(data.get("review_note") or data.get("note") or "").strip(),
                status=(data.get("status") or "").strip() or None,
            )
            self._send_json(payload)
        except Exception as e:
            logger.error(f"Experiment close error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_run_ritual(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            payload = lattice.run_ritual(
                (data.get("ritual") or data.get("name") or "weekly_truth_audit").strip(),
                project_id=(data.get("project_id") or data.get("project") or "").strip() or None,
                source=(data.get("source") or "api:ritual").strip() or "api:ritual",
            )
            self._send_json(payload)
        except Exception as e:
            logger.error(f"Run ritual error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_noticing_scan(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            payload = lattice.run_silent_noticing_scan(
                project_id=(data.get("project_id") or data.get("project") or "").strip() or None,
                source=(data.get("source") or "api:noticing").strip() or "api:noticing",
            )
            self._send_json(payload)
        except Exception as e:
            logger.error(f"Noticing scan error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_trajectory_refresh(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            payload = lattice.generate_trajectory_forecast(
                project_id=(data.get("project_id") or data.get("project") or "").strip() or None,
                horizon_days=int(data.get("horizon_days", data.get("days", 30))),
                persist=True,
                source=(data.get("source") or "api:trajectory").strip() or "api:trajectory",
            )
            self._send_json(payload)
        except Exception as e:
            logger.error(f"Trajectory refresh error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_chat(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            payload = build_chat_payload(lattice, server_context.get("mind"), data)
            self._send_json(payload)
        except Exception as e:
            logger.error(f"Chat error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_mentions_resolve(self, data):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            self._send_json(resolve_mentions_payload(lattice, data))
        except Exception as e:
            logger.error(f"Mention resolve error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_resolve_tension(self, data):
        """POST /memory/resolve-tension — resolve a tension by ID with resolution text."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            tension_id = data.get("tension_id") or data.get("id")
            resolution = data.get("resolution", "Resolved via API")
            if not tension_id:
                self._send_json({"error": "tension_id is required"}, 400)
                return
            result = lattice.resolve_tension(tension_id, resolution)
            self._send_json(result)
        except Exception as e:
            logger.error(f"Resolve tension error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_dream_trigger(self, data):
        """POST /dream/trigger — manually trigger a dream cycle."""
        dream = server_context.get("dream")
        if not dream:
            self._send_json({"error": "Dream module unavailable"}, 503)
            return
        try:
            trigger = data.get("trigger", "manual")
            reason = data.get("reason", "Manual trigger")
            result = dream.run_once(trigger=trigger)
            result["triggered_by"] = "api"
            result["reason"] = reason
            self._send_json(result)
        except Exception as e:
            logger.error(f"Dream trigger error: {e}")
            import traceback
            self._send_json({"error": str(e), "traceback": traceback.format_exc()}, 500)

    def _handle_dream_stream(self):
        try:
            self.send_response(200)
            self.send_header("Content-type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
        except Exception as exc:
            if _is_client_disconnect(exc):
                logger.debug("Client disconnected before dream stream started")
                return
            raise

        dream = server_context.get('dream')
        if not dream:
            try:
                self.wfile.write(b'data: {"error": "Dream module unavailable"}\n\n')
                self.wfile.flush()
            except Exception as exc:
                if not _is_client_disconnect(exc):
                    raise
            return

        try:
            # subscribe() is a blocking generator — run it in a background thread
            # so do_GET can return and keep the connection alive
            import threading

            broken = (BrokenPipeError, ConnectionResetError, OSError)

            def stream_events():
                try:
                    for event in dream.subscribe():
                        msg = f"data: {json.dumps(event)}\n\n"
                        self.wfile.write(msg.encode('utf-8'))
                        self.wfile.flush()
                except broken:
                    pass  # Client disconnected — normal SSE behavior
                except Exception as e:
                    logger.error(f"Dream stream error: {e}")

            t = threading.Thread(target=stream_events, daemon=True)
            t.start()
            # Keep request alive; daemon thread exits when client disconnects
        except broken:
            pass
        except Exception as e:
            logger.error(f"Dream stream error: {e}")

    def _handle_tension(self):
        """GET /tension — return active tensions (alias for observatory compatibility)."""
        lattice = server_context.get('lattice')
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            tensions = lattice.find_tensions()
            self._send_json({"tensions": tensions, "count": len(tensions)})
        except Exception as e:
            logger.error(f"Tension error: {e}")
            self._send_json({"tensions": [], "count": 0})

    def _handle_autonomy_status(self):
        """GET /autonomy/status — full autonomy pipeline health check."""
        lattice = server_context.get('lattice')
        organism = server_context.get('organism')
        dream = server_context.get('dream')
        vessel = server_context.get('vessel')
        metabolism = server_context.get('metabolism')
        from reporting.runtime_snapshot import build_status_snapshot

        status_snapshot = build_status_snapshot(
            lattice=lattice,
            metabolism=metabolism,
            vessel=vessel,
            engine=server_context.get("engine"),
            registry=server_context.get("registry"),
            dream=dream,
            runtime_contract=_runtime_contract_payload(),
        )
        subsystem_health = status_snapshot.get("health_model", {})
        degraded_reasons = list(status_snapshot.get("degraded_reasons", []))
        oscillator_running = bool(organism._running) if organism and hasattr(organism, "_running") else False

        status = {
            "contract_version": "autonomy-status/v2",
            "subsystems": {
                "lattice": lattice is not None,
                "organism": organism is not None,
                "dream": dream is not None,
                "metabolism": metabolism is not None,
                "vessel": vessel is not None,
                "oscillators_running": oscillator_running,
            },
            "subsystem_health": subsystem_health,
            "oscillators": {},
            "tensions": {},
            "memory_growth": {},
            "metabolism": status_snapshot.get("metabolism", {}),
            "overall": {
                "state": "healthy",
                "degraded_reasons": degraded_reasons,
                "trust_warnings": status_snapshot.get("trust_warnings", []),
            },
            "status_summary": {
                "breathing_state": ((status_snapshot.get("organism") or {}).get("breathing_state") or ""),
                "mirror_M": ((status_snapshot.get("organism") or {}).get("mirror_M") or status_snapshot.get("mirror_M") or 0.0),
                "health_interpretation": status_snapshot.get("health_interpretation", ""),
            },
            "autonomy_score": 0.0,
        }

        # Oscillator health
        if organism:
            status["oscillators"] = {
                "brainstem": "active",   # always runs if organism is
                "cortex": "active",
                "subconscious": "active",
                "pedagogue": "active" if organism.pedagogue else "no_pedagogue",
                "appetite": "active" if organism.lattice else "no_lattice",
                "appetite_cycle": getattr(organism, '_appetite_cycle', 0),
                "subconscious_cycle": organism._subconscious_cycle,
            }

        # Tension pipeline
        if lattice:
            try:
                tensions = lattice.detect_tensions(limit=100)
                status["tensions"] = {
                    "active_count": len(tensions),
                    "pipeline": "dream_cycle",
                    "monitor": "active" if dream and dream._monitor_thread and dream._monitor_thread.is_alive() else "inactive",
                    "dream_idle": dream._dreaming.is_set() if dream and hasattr(dream, '_dreaming') else None,
                    "dream_threshold": dream.TENSION_TRIGGER_THRESHOLD if dream else None,
                    "contested_nodes": status_snapshot.get("contested_nodes", 0),
                    "verification_backlog": status_snapshot.get("verification_backlog", 0),
                    "oldest_pending_verification_age_seconds": (
                        ((status_snapshot.get("freshness") or {}).get("oldest_pending_verification_age_seconds"))
                    ),
                }
            except Exception as e:
                status["tensions"] = {"error": str(e)}

        # Memory growth
        if lattice:
            try:
                lat_status = lattice.get_status()
                status["memory_growth"] = {
                    "total_nodes": lat_status.get("total_nodes", 0),
                    "beliefs": lat_status.get("beliefs", 0),
                    "axioms": lat_status.get("axioms", 0),
                    "coherence": lat_status.get("mirror", 0.0),
                    "memory_tiers": lat_status.get("memory_tiers", {}),
                    "mention_resolution_quality": status_snapshot.get("mention_resolution_quality", {}),
                    "approval_backlog": status_snapshot.get("approval_backlog", {}),
                    "session_health": status_snapshot.get("session_health", {}),
                }
            except Exception as e:
                status["memory_growth"] = {"error": str(e)}

        # Vessel metabolic state
        if vessel:
            v = vessel.state
            status["vessel"] = {
                "stage": v.stage,
                "energy": round(v.energy, 1),
                "coherence": round(v.coherence, 3),
                "tension_count": getattr(v, 'tension_count', 0),
                "dreaming": getattr(v, 'dreaming', False),
                "mirror_M": round(getattr(v, 'mirror', 0.0), 4),
                "truth_sync": {
                    "node_count": getattr(v, "truth_node_count", 0),
                    "tension_count": getattr(v, "truth_tension_count", 0),
                    "coherence": round(float(getattr(v, "truth_coherence", 0.0) or 0.0), 3),
                    "gamma": round(float(getattr(v, "truth_gamma", 0.0) or 0.0), 3),
                    "last_sync": getattr(v, "last_truth_sync", 0.0),
                },
            }

        score_inputs: list[float] = []
        for payload in subsystem_health.values():
            if not isinstance(payload, dict):
                continue
            score_inputs.extend(
                [
                    1.0 if payload.get("subsystem_alive") else 0.0,
                    1.0 if payload.get("subsystem_fresh") else 0.0,
                    1.0 if payload.get("subsystem_effective") else 0.0,
                ]
            )
        if not score_inputs:
            score_inputs = [0.0]
        base_score = sum(score_inputs) / len(score_inputs)
        penalty = min(0.6, 0.08 * len(degraded_reasons))
        status["autonomy_score"] = round(max(base_score - penalty, 0.0), 3)
        status["overall"]["state"] = (
            "degraded" if degraded_reasons else ("healthy" if status["autonomy_score"] >= 0.75 else "stale")
        )

        self._send_json(status)

    def _handle_autonomy_trigger(self, data):
        """POST /autonomy/trigger — manually trigger a dream cycle for debugging/testing."""
        dream = server_context.get('dream')
        if not dream:
            self._send_json({"error": "Dream cycle not available"}, 503)
            return
        try:
            result = dream.run_once(trigger=data.get("trigger", "manual_api"))
            self._send_json({
                "status": result.get("status", "unknown"),
                "summary": result.get("summary", ""),
                "run_id": result.get("run", {}).get("id", "") if result.get("run") else "",
            })
        except Exception as e:
            logger.error(f"Autonomy trigger error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_metabolism_status(self):
        metabolism = server_context.get("metabolism")
        lattice = server_context.get("lattice")
        if not metabolism or not lattice:
            self._send_json({"error": "Metabolism unavailable"}, 503)
            return
        self._send_json(
            {
                "metabolism": metabolism.get_status(),
                "jobs": lattice.get_metabolism_jobs(limit=12),
            }
        )

    def _handle_metabolism_jobs(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        status = (params.get("status", [""])[0] or "").strip() or None
        limit = int((params.get("limit", ["20"])[0] or "20"))
        self._send_json(
            {
                "jobs": lattice.get_metabolism_jobs(status=status, limit=limit),
                "health": lattice.get_metabolism_health(),
            }
        )

    def _handle_metabolism_trigger(self, data):
        metabolism = server_context.get("metabolism")
        if not metabolism:
            self._send_json({"error": "Metabolism unavailable"}, 503)
            return
        job = metabolism.trigger(
            kind=(data.get("kind") or "wake_maintenance"),
            topic=data.get("topic"),
            payload=dict(data.get("payload") or {}),
        )
        self._send_json({"status": "queued", "job": job})

    def _handle_approvals_inbox(self, params):
        lattice = server_context.get("lattice")
        metabolism = server_context.get("metabolism")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        limit = int((params.get("limit", ["20"])[0] or "20"))
        from routes.reporting_routes import approvals_inbox_response

        self._send_json(approvals_inbox_response(lattice, metabolism=metabolism, limit=limit))

    def _handle_approvals_resolve(self, data):
        lattice = server_context.get("lattice")
        metabolism = server_context.get("metabolism")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        job_id = (data.get("job_id") or "").strip()
        if not job_id:
            self._send_json({"error": "job_id is required"}, 400)
            return
        decision = (data.get("decision") or "").strip().lower()
        actor = (data.get("actor") or "human").strip() or "human"
        note = (data.get("note") or "").strip()
        try:
            job = lattice.resolve_metabolism_approval(job_id, decision=decision, actor=actor, note=note)
            if not job:
                self._send_json({"error": "job not found"}, 404)
                return
            if metabolism and decision == "approve":
                metabolism._wake.set()
            self._send_json({"status": "ok", "job": job})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_actions_capabilities(self):
        action_bus = server_context.get("action_bus")
        if not action_bus:
            self._send_json({"error": "Action bus unavailable"}, 503)
            return
        self._send_json(action_bus.capabilities())

    def _handle_actions_history(self, params):
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        limit = int((params.get("limit", ["20"])[0] or "20"))
        status = (params.get("status", [""])[0] or "").strip() or None
        self._send_json(lattice.get_action_history(limit=limit, status=status))

    def _handle_ops_overview(self, params):
        lattice = server_context.get("lattice")
        registry = server_context.get("registry")
        metabolism = server_context.get("metabolism")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        limit = int((params.get("limit", ["10"])[0] or "10"))
        approvals_limit = int((params.get("approvals_limit", [str(limit)])[0] or str(limit)))
        action_limit = int((params.get("action_limit", [str(limit)])[0] or str(limit)))
        from routes.reporting_routes import ops_overview_response

        payload = ops_overview_response(
            lattice,
            registry=registry,
            metabolism=metabolism,
            limit=limit,
            approvals_limit=approvals_limit,
            action_limit=action_limit,
        )
        self._send_json(payload)

    def _handle_action_dispatch(self, data):
        action_bus = server_context.get("action_bus")
        if not action_bus:
            self._send_json({"error": "Action bus unavailable"}, 503)
            return
        try:
            from action_policy import ActionIntent

            intent = ActionIntent.from_payload(data)
            if not intent.name:
                self._send_json({"error": "name is required"}, 400)
                return
            self._send_json(action_bus.dispatch(intent))
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_all_memories(self):
        _lattice = server_context.get("lattice")
        if not _lattice:
            self._send_json({"error": "Lattice unavailable"}, 502)
            return
        try:
            _nodes = _lattice.list_nodes(limit=1000)
            self._send_json({"memories": _nodes})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_action_rules(self):
        """GET /memory/action-rules — return enforced action rules for agent enforcement."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            rules = lattice.list_nodes(category="action_rule", limit=100)
            enforced = [r for r in rules if r.get("enforced")]
            self._send_json({"action_rules": enforced, "count": len(enforced)})
        except Exception as e:
            logger.error(f"Action rules error: {e}")
            self._send_json({"action_rules": [], "count": 0})

    def _handle_nodes(self, params):
        """GET /nodes — return nodes from personal lattice for observatory map.
        Supports ?limit=N query parameter.
        """
        lattice = server_context.get('lattice')
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            limit = int(params.get("limit", [500])[0])
            limit = min(max(limit, 1), 5000)  # clamp between 1 and 5000
            nodes = lattice.list_nodes(limit=limit)
            total = limit  # approximate - actual total would require separate count query
            self._send_json({
                "nodes": nodes,
                "count": len(nodes),
                "total": total,
                "offset": 0,
                "limit": limit
            })
        except Exception as e:
            logger.error(f"Nodes error: {e}")
            self._send_json({"nodes": [], "count": 0, "total": 0, "offset": 0, "limit": limit})

    def _handle_manifold(self, params):
        """GET /manifold — Conversation Manifold data for geometric session visualization."""
        import time
        try:
            # Get window and radius params
            window = int(params.get("window", ["5"])[0])
            radius = float(params.get("radius", ["0.35"])[0])
            
            # Get lattice from server context
            lattice = server_context.get('lattice')
            if not lattice:
                self._send_json({
                    "session_id": "personal-mirror",
                    "trajectory": [],
                    "adjacent": [],
                    "centroid_position": None,
                    "stats": {"total_turns": 0, "adjacent_count": 0, "generated_at": time.time()}
                })
                return
            
            # Get experience nodes via dedicated method (chronological, deduped, filtered)
            recent_beliefs = lattice.get_conversation_turns(limit=window)
            trajectory = []
            for i, belief in enumerate(recent_beliefs):
                trajectory.append({
                    "turn_index": i,
                    "text": (belief.get('text') or '')[:200],
                    "node_id": belief.get('id', ''),
                    "ts": time.time() - (len(recent_beliefs) - i) * 60
                })
            
            # Get axioms and high-confidence beliefs as "adjacent possibilities"
            axioms = lattice.list_nodes(category="axiom", limit=10)
            verified = lattice.list_nodes(category="belief", limit=10)
            
            adjacent = []
            distance = 0.08
            for axiom in axioms[:6]:
                text = axiom.get('text', '')[:150]
                if text:
                    adjacent.append({
                        "id": axiom.get('id', ''),
                        "text": text,
                        "distance": distance,
                        "domain": axiom.get('domain', 'hermes'),
                        "resonance": float(axiom.get('energy', 0.8)),
                        "immutable": True
                    })
                    distance += 0.07
            
            for belief in verified[:3]:
                text = belief.get('text', '')[:150]
                if text and len(adjacent) < 9:
                    adjacent.append({
                        "id": belief.get('id', ''),
                        "text": text,
                        "distance": distance,
                        "domain": belief.get('domain', 'hermes'),
                        "resonance": float(belief.get('energy', 0.6)),
                        "immutable": False
                    })
                    distance += 0.07
            
            # Build response in expected format
            manifold_data = {
                "session_id": "personal-mirror-lattice",
                "trajectory": trajectory,
                "adjacent": adjacent,
                "centroid_position": None,
                "stats": {
                    "total_turns": len(trajectory),
                    "adjacent_count": len(adjacent),
                    "generated_at": time.time()
                }
            }
            
            self._send_json(manifold_data)
            
        except Exception as e:
            logger.error(f"Manifold error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self._send_json({
                "session_id": "personal-mirror-error",
                "trajectory": [],
                "adjacent": [],
                "centroid_position": None,
                "stats": {"total_turns": 0, "adjacent_count": 0, "generated_at": time.time()}
            })

    # ── OMP v2 Universal Protocol handlers ───────────────────────────────────

    def _handle_omp_manifest(self):
        """GET /omp/manifest — describes the substrate to new arrivals."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            self._send_json(lattice.get_manifest())
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_omp_join(self, data):
        """POST /omp/join — universal agent join handshake."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            result = lattice.register_agent(
                agent_id=data.get("agent_id", "unknown"),
                agent_name=data.get("agent_name", ""),
                capabilities=data.get("capabilities", []),
                description=data.get("description", ""),
                agent_type=data.get("agent_type", "ai"),
                model_version=data.get("model_version", "unknown-model"),
                metadata=data.get("metadata") if isinstance(data.get("metadata"), dict) else {},
            )
            self._send_json(result)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_convergence_data(self, params):
        """GET /convergence/data — full convergence geometry for visualization."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            limit = int(params.get("limit", ["120"])[0])
            limit = min(max(limit, 10), 250)
            data = lattice.compute_convergence_zones(limit=limit)
            self._send_json(data)
        except Exception as e:
            logger.error(f"Convergence error: {e}")
            import traceback; logger.error(traceback.format_exc())
            self._send_json({"error": str(e)}, 500)

    def _handle_convergence_stats(self):
        """GET /convergence/stats — quick convergence score without full geometry."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            data = lattice.compute_convergence_zones(limit=60)
            self._send_json(data.get("stats", {}))
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    # ── Wake Protocol handlers ────────────────────────────────────────────────

    def _handle_wake(self, params):
        """GET /wake?agent=claude-code — Bootstrap context for an AI agent.
        Returns identity chain, continuations, open questions, insights, trajectory.
        This is the 'who am I' packet injected at session start.
        """
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        agent_id = params.get("agent", params.get("agent_id", ["claude-code"]))[0]
        limit = int(params.get("limit", ["5"])[0])
        try:
            ctx = lattice.get_wake_context(agent_id, limit_each=limit)
            continuity_project = _continuity_project_id(
                lattice,
                {
                    "agent_id": agent_id,
                    "session_id": ((ctx.get("active_session") or {}).get("id") or "").strip(),
                    "project_id": ((ctx.get("active_session") or {}).get("current_project") or "").strip(),
                },
            )
            continuity = lattice.continuity_brief(
                project_id=continuity_project,
                agent_id=agent_id,
                limit=max(1, min(limit, 3)),
            )
            ctx["continuity"] = continuity
            ctx["project_scope"] = {
                "resolved_project_id": continuity_project or "",
                "agent_id": agent_id,
                "session_id": ((ctx.get("active_session") or {}).get("id") or "").strip(),
            }
            # Format as a readable boot prompt showing temporal arc, not just latest snapshot
            lines = [f"# Wake Context — {agent_id}", f"Sessions: {ctx.get('sessions_count', '?')} | Nodes: {ctx.get('lattice_nodes', '?')} | First contact: {(ctx.get('first_contact') or '')[:10]}"]
            # Identity trajectory — last 3 nodes in chronological order (arc, not just snapshot)
            identity_nodes = ctx["identity"]
            if identity_nodes:
                if len(identity_nodes) > 1:
                    lines.append(f"\n## Identity trajectory ({len(identity_nodes)} sessions shown):")
                    for i, node in enumerate(reversed(identity_nodes[-3:])):  # oldest first, last 3
                        prefix = "← " if i < len(identity_nodes) - 1 else "▶ "
                        lines.append(f"{prefix}[{node['created_at'][:10]}] {node['text']}")
                else:
                    lines.append(f"\n## Last known self ({identity_nodes[0]['created_at'][:10]}):")
                    lines.append(identity_nodes[0]["text"])
            if continuity.get("inferred_last_intent") or continuity.get("best_next_step"):
                lines.append("\n## Continuity brief:")
                last_intent = (continuity.get("inferred_last_intent") or {}).get("summary")
                if last_intent:
                    lines.append(f"Last intent: {last_intent}")
                for item in continuity.get("active_continuations", [])[:2]:
                    lines.append(f"Active: {item.get('text', '')}")
                for item in continuity.get("top_tensions", [])[:2]:
                    lines.append(f"Tension: {item.get('text', '')}")
                next_step = (continuity.get("best_next_step") or {}).get("text")
                if next_step:
                    lines.append(f"Best next step: {next_step}")
                else:
                    lines.append(
                        "Why no next step: "
                        + (
                            continuity.get("continuity_reason")
                            or "The continuity field has not resolved a next move yet."
                        )
                    )
            if ctx["continuations"]:
                lines.append("\n## Unfinished threads carrying forward:")
                for c in ctx["continuations"]:
                    urgency = f" (urgency={c['urgency']:.1f})" if c.get("urgency") else ""
                    lines.append(f"→ {c['text']}{urgency}")
            if ctx["questions"]:
                lines.append("\n## Open questions being held:")
                for q in ctx["questions"]:
                    lines.append(f"? {q['text']}")
            if ctx["insights"]:
                lines.append(f"\n## Recent insights ({len(ctx['insights'])} shown):")
                for ins in ctx["insights"]:
                    conf = f" [conf={ins['confidence']:.2f}]" if ins.get("confidence") else ""
                    lines.append(f"✦ {ins['text']}{conf}")
            ctx["boot_prompt"] = "\n".join(lines)
            self._send_json(ctx)
        except Exception as e:
            logger.error(f"Wake error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_identity_chain(self, params):
        """GET /identity/chain?agent=claude-code — The full autobiography of an agent."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        agent_id = params.get("agent", params.get("agent_id", ["claude-code"]))[0]
        limit = int(params.get("limit", ["50"])[0])
        try:
            chain = lattice.get_identity_chain(agent_id, limit=limit)
            self._send_json({"agent_id": agent_id, "chain": chain, "length": len(chain)})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_continuations(self, params):
        """GET /continuations?agent=claude-code — Pending continuations for an agent."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        agent_id = params.get("agent", params.get("agent_id", ["claude-code"]))[0]
        try:
            source = f"agent:{agent_id}"
            with lattice._conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM personal_nodes WHERE category='continuation' AND source=? "
                    "AND COALESCE(archived,0)=0 AND (verification_status IS NULL OR verification_status != 'picked_up') "
                    "ORDER BY energy DESC, created_at DESC LIMIT 20",
                    (source,)
                ).fetchall()
            items = []
            for row in rows:
                n = lattice._row_to_node(row)
                prov = n.get("provenance", {})
                items.append({
                    "id": n["id"],
                    "text": (n.get("text") or "")[:400],
                    "urgency": prov.get("urgency", n.get("weight", 0.7)),
                    "context": prov.get("context", ""),
                    "session_id": prov.get("session_id", ""),
                    "created_at": n.get("created_at", ""),
                })
            self._send_json({"continuations": items, "count": len(items), "agent_id": agent_id})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_session_begin(self, data):
        """POST /session/begin — Write an identity node at session start.
        Call this at the beginning of every Claude Code session.
        """
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            self._send_json(begin_session_payload(lattice, data))
        except Exception as e:
            logger.error(f"Session begin error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_session_end(self, data):
        """POST /session/end — Write session summary + continuations at session end.
        Call this at the end of every Claude Code session.
        """
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            agent_id = data.get("agent_id", "claude-code")
            session_id = data.get("session_id", "unknown")
            summary = data.get("summary", "").strip()
            continuations = data.get("continuations", [])
            insights = data.get("insights", [])
            questions = data.get("questions", [])
            stored = {"continuations": [], "insights": [], "questions": []}
            if summary:
                lattice.store_identity_node(
                    agent_id=agent_id,
                    text=f"Session end: {summary}",
                    session_id=session_id,
                    metadata={"phase": "session_end"},
                )
            for c in continuations:
                text = c if isinstance(c, str) else c.get("text", "")
                urgency = 0.8 if isinstance(c, str) else c.get("urgency", 0.8)
                if text:
                    n = lattice.store_continuation(agent_id, text, session_id, urgency=urgency)
                    stored["continuations"].append(n.get("id"))
            for ins in insights:
                text = ins if isinstance(ins, str) else ins.get("text", "")
                if text:
                    n = lattice.store_insight(agent_id, text, session_id)
                    stored["insights"].append(n.get("id"))
            for q in questions:
                text = q if isinstance(q, str) else q.get("text", "")
                if text:
                    n = lattice.store_question(agent_id, text, session_id)
                    stored["questions"].append(n.get("id"))
            if hasattr(lattice, "end_agent_session"):
                lattice.end_agent_session(
                    session_id,
                    agent_id=agent_id,
                    summary=summary,
                    metadata={
                        "continuations": stored["continuations"],
                        "insights": stored["insights"],
                        "questions": stored["questions"],
                    },
                )
            self._send_json({
                "status": "session_ended",
                "agent_id": agent_id,
                "session_id": session_id,
                "stored": stored,
            })
        except Exception as e:
            logger.error(f"Session end error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_store_continuation(self, data):
        """POST /memory/continuation — Store an unfinished thought."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            n = lattice.store_continuation(
                agent_id=data.get("agent_id", "claude-code"),
                text=data.get("text", ""),
                session_id=data.get("session_id", "unknown"),
                urgency=float(data.get("urgency", 0.7)),
                context=data.get("context", ""),
            )
            self._send_json(n)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_store_insight(self, data):
        """POST /memory/insight — Store a genuine realization."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            n = lattice.store_insight(
                agent_id=data.get("agent_id", "claude-code"),
                text=data.get("text", ""),
                session_id=data.get("session_id", "unknown"),
                confidence=float(data.get("confidence", 0.75)),
                domain=data.get("domain", "personal"),
            )
            self._send_json(n)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_store_question(self, data):
        """POST /memory/question — Store an open question."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            n = lattice.store_question(
                agent_id=data.get("agent_id", "claude-code"),
                text=data.get("text", ""),
                session_id=data.get("session_id", "unknown"),
                domain=data.get("domain", "personal"),
            )
            self._send_json(n)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_resolve_continuation(self, data):
        """POST /continuation/resolve — Mark a continuation as picked up."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            result = lattice.resolve_continuation(
                data.get("id") or data.get("continuation_id", ""),
                resolution=data.get("resolution", "picked_up"),
            )
            self._send_json(result or {"status": "resolved"})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_agents_status(self):
        """GET /agents/status — Return per-agent contribution stats from personal_lattice."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            if hasattr(lattice, "list_agent_registrations"):
                agents = lattice.list_agent_registrations(limit=100, include_sessions=True)
                self._send_json(
                    {
                        "agents": agents,
                        "count": len(agents),
                        "contract_ready": sum(1 for agent in agents if agent.get("contract_ready")),
                    }
                )
                return
            self._send_json({"agents": [], "count": 0, "contract_ready": 0})
        except Exception as e:
            logger.error(f"Agents status error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _mesh(self):
        mesh = server_context.get("mesh")
        if not mesh:
            self._send_json({"error": "Sovereign Mesh unavailable"}, 503)
            return None
        return mesh

    def _handle_mesh_manifest(self):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.get_manifest())
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_mesh_peers(self, params):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            limit = int(params.get("limit", ["25"])[0])
            self._send_json(mesh.list_peers(limit=limit))
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_mesh_peers_sync(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            peer_id = (data.get("peer_id") or "").strip()
            limit = int(data.get("limit") or 100)
            refresh_manifest = bool(data.get("refresh_manifest"))
            if peer_id:
                self._send_json(mesh.sync_peer(peer_id, limit=limit, refresh_manifest=refresh_manifest))
                return
            self._send_json(mesh.sync_all_peers(limit=limit, refresh_manifest=refresh_manifest))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_stream(self, params):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            since = int(params.get("since", ["0"])[0])
            limit = int(params.get("limit", ["50"])[0])
            payload = mesh.stream_snapshot(since_seq=since, limit=limit)
            upgrade = (self.headers.get("Upgrade") or "").strip().lower()
            if upgrade == "websocket":
                self._send_websocket_json(payload)
                return
            self._send_json(payload)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_mesh_handshake(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.accept_handshake(data))
        except Exception as e:
            code = 409 if e.__class__.__name__.lower().find("replay") >= 0 else 400
            self._send_json({"error": str(e)}, code)

    def _handle_mesh_lease_acquire(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.accept_lease_request(data, route="/mesh/lease/acquire"))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_lease_heartbeat(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.accept_lease_request(data, route="/mesh/lease/heartbeat"))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_lease_release(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.accept_lease_request(data, route="/mesh/lease/release"))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_job_submit(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.accept_job_submission(data))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_job_schedule(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(
                mesh.schedule_job(
                    dict(data.get("job") or {}),
                    request_id=(data.get("request_id") or "").strip() or None,
                    preferred_peer_id=(data.get("preferred_peer_id") or "").strip(),
                    allow_local=bool(data.get("allow_local", True)),
                    allow_remote=bool(data.get("allow_remote", True)),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_job_get(self, path: str):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/jobs/"
            suffix = "/cancel"
            if not path.startswith(prefix) or path.endswith(suffix):
                self._send_json({"error": "job id is required"}, 400)
                return
            job_id = path[len(prefix):].strip("/")
            self._send_json(mesh.get_job(job_id))
        except Exception as e:
            self._send_json({"error": str(e)}, 404 if "not found" in str(e).lower() else 400)

    def _handle_mesh_job_resume(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/jobs/"
            suffix = "/resume"
            if not path.startswith(prefix) or not path.endswith(suffix):
                self._send_json({"error": "job id is required"}, 400)
                return
            job_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                mesh.resume_job(
                    job_id,
                    operator_id=(data.get("operator_id") or "").strip(),
                    reason=(data.get("reason") or "operator_resume_latest").strip(),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_job_resume_from_checkpoint(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/jobs/"
            suffix = "/resume-from-checkpoint"
            if not path.startswith(prefix) or not path.endswith(suffix):
                self._send_json({"error": "job id is required"}, 400)
                return
            checkpoint_artifact_id = (data.get("checkpoint_artifact_id") or "").strip()
            if not checkpoint_artifact_id:
                self._send_json({"error": "checkpoint_artifact_id is required"}, 400)
                return
            job_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                mesh.resume_job_from_checkpoint(
                    job_id,
                    checkpoint_artifact_id=checkpoint_artifact_id,
                    operator_id=(data.get("operator_id") or "").strip(),
                    reason=(data.get("reason") or "operator_resume_checkpoint").strip(),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_job_restart(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/jobs/"
            suffix = "/restart"
            if not path.startswith(prefix) or not path.endswith(suffix):
                self._send_json({"error": "job id is required"}, 400)
                return
            job_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                mesh.restart_job(
                    job_id,
                    operator_id=(data.get("operator_id") or "").strip(),
                    reason=(data.get("reason") or "operator_restart").strip(),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_workers(self, params):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            limit = int(params.get("limit", ["25"])[0])
            self._send_json(mesh.list_workers(limit=limit))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_queue(self, params):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            limit = int(params.get("limit", ["25"])[0])
            status = (params.get("status", [""])[0] or "").strip()
            self._send_json(mesh.list_queue_messages(limit=limit, status=status))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_queue_events(self, params):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            limit = int(params.get("limit", ["50"])[0])
            since_seq = int(params.get("since", ["0"])[0])
            queue_message_id = (params.get("queue_message_id", [""])[0] or "").strip()
            job_id = (params.get("job_id", [""])[0] or "").strip()
            self._send_json(
                mesh.list_queue_events(
                    since_seq=since_seq,
                    limit=limit,
                    queue_message_id=queue_message_id,
                    job_id=job_id,
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_queue_metrics(self):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.queue_metrics())
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_queue_replay(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(
                mesh.replay_queue_message(
                    queue_message_id=(data.get("queue_message_id") or "").strip(),
                    job_id=(data.get("job_id") or "").strip(),
                    reason=(data.get("reason") or "operator_replay").strip(),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_queue_ack_deadline(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(
                mesh.set_queue_ack_deadline(
                    queue_message_id=(data.get("queue_message_id") or "").strip(),
                    attempt_id=(data.get("attempt_id") or "").strip(),
                    ttl_seconds=int(data.get("ttl_seconds") or 0),
                    reason=(data.get("reason") or "operator_ack_deadline_update").strip(),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_scheduler_decisions(self, params):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            limit = int(params.get("limit", ["25"])[0])
            status = (params.get("status", [""])[0] or "").strip()
            target_type = (params.get("target_type", [""])[0] or "").strip()
            self._send_json(mesh.list_scheduler_decisions(limit=limit, status=status, target_type=target_type))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_worker_register(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            worker_id = (data.get("worker_id") or "").strip()
            if not worker_id:
                self._send_json({"error": "worker_id is required"}, 400)
                return
            self._send_json(
                {
                    "status": "ok",
                    "worker": mesh.register_worker(
                        worker_id=worker_id,
                        agent_id=(data.get("agent_id") or "").strip(),
                        capabilities=list(data.get("capabilities") or []),
                        resources=dict(data.get("resources") or {}),
                        labels=list(data.get("labels") or []),
                        max_concurrent_jobs=int(data.get("max_concurrent_jobs") or 1),
                        metadata=dict(data.get("metadata") or {}),
                        status=(data.get("status") or "active").strip().lower(),
                    ),
                }
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_worker_heartbeat(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/workers/"
            suffix = "/heartbeat"
            worker_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                {
                    "status": "ok",
                    "worker": mesh.heartbeat_worker(
                        worker_id,
                        status=(data.get("status") or "").strip(),
                        metadata=dict(data.get("metadata") or {}),
                    ),
                }
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_worker_poll(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/workers/"
            suffix = "/poll"
            worker_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(mesh.poll_jobs(worker_id, limit=int(data.get("limit") or 10)))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_worker_claim(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/workers/"
            suffix = "/claim"
            worker_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                mesh.claim_next_job(
                    worker_id,
                    job_id=(data.get("job_id") or "").strip(),
                    ttl_seconds=int(data.get("ttl_seconds") or 0),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_attempt_heartbeat(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/jobs/attempts/"
            suffix = "/heartbeat"
            attempt_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                {
                    "status": "ok",
                    "attempt": mesh.heartbeat_job_attempt(
                        attempt_id,
                        ttl_seconds=int(data.get("ttl_seconds") or 300),
                        metadata=dict(data.get("metadata") or {}),
                    ),
                }
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_attempt_complete(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/jobs/attempts/"
            suffix = "/complete"
            attempt_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                mesh.complete_job_attempt(
                    attempt_id,
                    data.get("result"),
                    media_type=(data.get("media_type") or "application/json").strip(),
                    executor=(data.get("executor") or "").strip(),
                    metadata=dict(data.get("metadata") or {}),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_attempt_fail(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/jobs/attempts/"
            suffix = "/fail"
            attempt_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                mesh.fail_job_attempt(
                    attempt_id,
                    error=(data.get("error") or "job attempt failed").strip(),
                    retryable=bool(data.get("retryable", True)),
                    metadata=dict(data.get("metadata") or {}),
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_job_cancel(self, path: str, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/jobs/"
            suffix = "/cancel"
            if not path.startswith(prefix) or not path.endswith(suffix):
                self._send_json({"error": "job id is required"}, 400)
                return
            job_id = path[len(prefix):-len(suffix)].strip("/")
            self._send_json(
                {
                    "status": "cancelled",
                    "job": mesh.cancel_job(job_id, reason=(data.get("reason") or "cancelled")),
                }
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_artifact_publish(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.accept_artifact_publish(data))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_artifact_list(self, params):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(
                mesh.list_artifacts(
                    limit=int(params.get("limit", ["25"])[0]),
                    artifact_kind=params.get("artifact_kind", [""])[0],
                    job_id=params.get("job_id", [""])[0],
                    attempt_id=params.get("attempt_id", [""])[0],
                    parent_artifact_id=params.get("parent_artifact_id", [""])[0],
                    owner_peer_id=params.get("owner_peer_id", [""])[0],
                    media_type=params.get("media_type", [""])[0],
                    retention_class=params.get("retention_class", [""])[0],
                )
            )
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_artifact_get(self, path: str, params):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            prefix = "/mesh/artifacts/"
            if not path.startswith(prefix):
                self._send_json({"error": "artifact id is required"}, 400)
                return
            artifact_id = path[len(prefix):].strip("/")
            include_content = params.get("include_content", ["1"])[0].strip() != "0"
            if include_content:
                metadata = mesh.get_artifact(artifact_id, include_content=False)
                is_public = mesh._policy_allows_peer(dict(metadata.get("policy") or {}), None)
                client_host = self.client_address[0] if getattr(self, "client_address", None) else None
                if not is_public and not _is_authorized_agent_request(
                    "GET",
                    _PROTECTED_MESH_ARTIFACT_CONTENT_PATH,
                    self.headers,
                    client_host,
                ):
                    self._send_json(
                        _authorization_failure_payload("GET", path, client_host),
                        401,
                    )
                    return
            self._send_json(mesh.get_artifact(artifact_id, requester_peer_id="", include_content=include_content))
        except Exception as e:
            self._send_json({"error": str(e)}, 404 if "not found" in str(e).lower() else 400)

    def _handle_mesh_artifact_purge(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.purge_expired_artifacts(limit=int(data.get("limit") or 100)))
        except Exception as e:
            self._send_json({"error": str(e)}, 400)

    def _handle_mesh_handoff(self, data):
        mesh = self._mesh()
        if not mesh:
            return
        try:
            self._send_json(mesh.accept_handoff(data))
        except Exception as e:
            code = 409 if e.__class__.__name__.lower().find("replay") >= 0 else 400
            self._send_json({"error": str(e)}, code)

    def _handle_activity(self):
        """GET /api/activity — Recent nodes with contributor field, newest first."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            params_raw = parse_qs(urlparse(self.path).query)
            limit = int(params_raw.get("limit", ["40"])[0])
            limit = min(max(limit, 1), 200)
            nodes = lattice.list_nodes(limit=limit)
            events = []
            for n in nodes:
                source = n.get("source", "user")
                if source.startswith("agent:"):
                    origin = "ai"
                    contributor_label = source.replace("agent:", "")
                elif source in ("user", "human"):
                    origin = "human"
                    contributor_label = "human"
                else:
                    # Heuristic: check provenance
                    prov = n.get("provenance") or {}
                    if isinstance(prov, str):
                        try:
                            import json as _json
                            prov = _json.loads(prov)
                        except Exception:
                            prov = {}
                    contrib = prov.get("contributor", source)
                    origin = "ai" if "agent" in contrib else "human"
                    contributor_label = contrib
                events.append({
                    "id": n.get("id", ""),
                    "text": (n.get("text") or "")[:200],
                    "category": n.get("category", "belief"),
                    "domain": n.get("domain", "personal"),
                    "origin": origin,
                    "contributor": contributor_label,
                    "source": source,
                    "energy": float(n.get("energy", 0.5)),
                    "weight": float(n.get("weight", 0.5)),
                    "created_at": n.get("created_at", ""),
                    "verified": bool(n.get("verified")),
                })
            self._send_json({"events": events, "count": len(events)})
        except Exception as e:
            logger.error(f"Activity error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_activity_sse(self):
        """GET /api/activity/sse — SSE stream of new nodes as they are stored."""
        import threading, time as _time
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
        except Exception as exc:
            if _is_client_disconnect(exc):
                logger.debug("Client disconnected before activity SSE started")
                return
            raise

        lattice = server_context.get("lattice")
        if not lattice:
            try:
                self.wfile.write(b'data: {"error": "Lattice unavailable"}\n\n')
                self.wfile.flush()
            except Exception as exc:
                if not _is_client_disconnect(exc):
                    raise
            return

        broken = (BrokenPipeError, ConnectionResetError, OSError)

        def _stream():
            last_seq = 0
            try:
                # Bootstrap: send last 10 events immediately
                events = lattice.get_events(after_seq=0, limit=10)
                if events:
                    last_seq = events[-1].get("seq", 0)
                    for ev in events:
                        msg = f"data: {json.dumps(ev)}\n\n"
                        self.wfile.write(msg.encode())
                    self.wfile.flush()
                while True:
                    _time.sleep(2)
                    new_events = lattice.get_events(after_seq=last_seq, limit=20)
                    for ev in new_events:
                        last_seq = max(last_seq, ev.get("seq", 0))
                        msg = f"data: {json.dumps(ev)}\n\n"
                        self.wfile.write(msg.encode())
                    if new_events:
                        self.wfile.flush()
            except broken:
                pass
            except Exception as exc:
                logger.error(f"Activity SSE error: {exc}")

        t = threading.Thread(target=_stream, daemon=True)
        t.start()

    # ── OMPv2 geometric endpoint handlers ────────────────────────────────────

    def _handle_omp_neighbors(self, params):
        """GET /omp/neighbors?q=TEXT&limit=10 — geometric neighbors in 768-dim space."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            q = params.get("q", [""])[0] or params.get("node_id", [""])[0]
            limit = int(params.get("limit", ["10"])[0])
            if not hasattr(lattice, "get_geometric_neighbors"):
                self._send_json({"error": "OMPv2 not installed"}, 501)
                return
            results = lattice.get_geometric_neighbors(q, limit=limit)
            self._send_json({"neighbors": results, "count": len(results), "query": q})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_omp_trajectory(self, params):
        """GET /omp/trajectory?limit=20 — temporal path through memory space."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            limit = int(params.get("limit", ["20"])[0])
            if not hasattr(lattice, "get_trajectory"):
                self._send_json({"error": "OMPv2 not installed"}, 501)
                return
            traj = lattice.get_trajectory(limit=limit)
            self._send_json({"trajectory": traj, "count": len(traj)})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_omp_centroid(self, params):
        """GET /omp/centroid — centroid of all nodes (or filtered set) in embedding space."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            category = params.get("category", [None])[0]
            if not hasattr(lattice, "get_manifold_centroid"):
                self._send_json({"error": "OMPv2 not installed"}, 501)
                return
            centroid = lattice.get_manifold_centroid(category=category)
            self._send_json(centroid)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_omp_adjacent(self, params):
        """GET /omp/adjacent?q=TEXT&limit=8 — adjacent possibilities: edge of known space."""
        lattice = server_context.get("lattice")
        if not lattice:
            self._send_json({"error": "Lattice unavailable"}, 503)
            return
        try:
            q = params.get("q", [""])[0]
            limit = int(params.get("limit", ["8"])[0])
            if not hasattr(lattice, "get_adjacent_possibilities"):
                self._send_json({"error": "OMPv2 not installed"}, 501)
                return
            adjacent = lattice.get_adjacent_possibilities(q, limit=limit)
            self._send_json({"adjacent": adjacent, "count": len(adjacent), "query": q})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_curriculum(self):
        """GET /curriculum — return simple curriculum response for observatory UI."""
        # Return a simple curriculum with default missions
        # The observatory's parseMissions expects curriculum.missions or curriculum.topics
        self._send_json({
            "missions": [
                {"id": "default-1", "search_query": "personal growth and learning", "title": "Explore personal development topics"},
                {"id": "default-2", "search_query": "curiosity and discovery", "title": "Follow curiosity paths"},
            ],
            "topics": []
        })

    # --- OMP v1 Agent Memory API ---
    def _handle_agent_memory_list(self, params):
        """GET /agent/memory/v1/list — List agent memories (OMP v1)."""
        lattice = server_context.get('lattice')
        if not lattice:
            self._send_json({"error": "lattice not ready", "memories": []})
            return
        
        limit = int(params.get("limit", ["50"])[0])
        offset = int(params.get("offset", ["0"])[0])
        domain = params.get("domain", [None])[0]
        mem_type = params.get("type", [None])[0]
        
        try:
            nodes = lattice.list_nodes(limit=limit + offset, category=mem_type)
            nodes = nodes[offset:offset + limit]
            
            memories = []
            for n in nodes:
                memories.append({
                    "id": n.get("id", ""),
                    "key": n.get("text", "")[:80],
                    "value": n.get("text", ""),
                    "type": n.get("category", "note"),
                    "domain": n.get("domain", "personal"),
                    "confidence": n.get("weight", 0.5),
                    "importance": n.get("energy", 0.5),
                    "tier": "long-term" if n.get("category") == "axiom" else "working",
                    "created_at": n.get("created_at", n.get("timestamp", "")),
                })
            
            self._send_json({"memories": memories, "total": len(memories)})
        except Exception as e:
            logger.error(f"[OMP] list error: {e}")
            self._send_json({"error": str(e), "memories": []})

    def _handle_agent_memory_tensions(self):
        """GET /agent/memory/v1/tensions — Get memory tensions (OMP v1)."""
        lattice = server_context.get('lattice')
        if not lattice:
            self._send_json({"tensions": [], "count": 0})
            return

        try:
            tensions = lattice.list_nodes(category="tension", limit=50)
            result = []
            for t in tensions:
                # Extract tension metadata from provenance
                provenance = t.get("provenance", "{}")
                try:
                    prov_json = json.loads(provenance) if isinstance(provenance, str) else provenance
                except:
                    prov_json = {}
                
                result.append({
                    "id": t.get("id", ""),
                    "note": t.get("text", ""),
                    "axiom": t.get("child_id") or t.get("parent_id") or "",
                    "domain": t.get("domain", "personal"),
                    "type": prov_json.get("kind", "TENSION"),
                    "similarity": float(prov_json.get("similarity", 0.0)),
                })
            self._send_json({"tensions": result, "count": len(result)})
        except Exception as e:
            logger.error(f"[OMP] tensions error: {e}")
            self._send_json({"tensions": [], "count": 0})

    def _handle_agent_memory_stats(self):
        """GET /agent/memory/v1/stats — Memory health dashboard (OMP v1)."""
        lattice = server_context.get('lattice')
        if not lattice:
            self._send_json({"error": "lattice not ready"}, 503)
            return
        try:
            status = lattice.get_status()
            self._send_json(status)
        except Exception as e:
            logger.error(f"[OMP] stats error: {e}")
            self._send_json({"error": str(e)}, 500)

    def _handle_agent_memory_recall(self, params):
        """GET /agent/memory/v1/recall — Semantic recall (OMP v1)."""
        lattice = server_context.get('lattice')
        if not lattice:
            self._send_json({"error": "lattice not ready", "memories": []})
            return
        query = params.get("query", [None])[0] or params.get("q", [""])[0]
        limit = int(params.get("limit", ["10"])[0])
        try:
            results = lattice.recall_semantic(query, max_results=limit)
            memories = [{
                "id": n.get("id", ""),
                "key": n.get("text", "")[:80],
                "value": n.get("text", ""),
                "type": n.get("category", "note"),
                "domain": n.get("domain", "personal"),
                "confidence": n.get("weight", 0.5),
                "importance": n.get("energy", 0.5),
                "similarity": n.get("similarity"),
                "tier": "long-term" if n.get("category") == "axiom" else "working",
                "created_at": n.get("created_at", ""),
            } for n in results]
            self._send_json({"memories": memories, "query": query, "count": len(memories)})
        except Exception as e:
            logger.error(f"[OMP] recall error: {e}")
            self._send_json({"error": str(e), "memories": []})

    # --- Utilities ---

    def _websocket_text_frame(self, text: str) -> bytes:
        payload = text.encode("utf-8")
        header = bytearray([0x81])
        length = len(payload)
        if length < 126:
            header.append(length)
        elif length < (1 << 16):
            header.append(126)
            header.extend(length.to_bytes(2, "big"))
        else:
            header.append(127)
            header.extend(length.to_bytes(8, "big"))
        return bytes(header) + payload

    def _send_websocket_json(self, data) -> bool:
        try:
            key = (self.headers.get("Sec-WebSocket-Key") or "").strip()
            if not key:
                self._send_json({"error": "Sec-WebSocket-Key is required for websocket upgrade"}, 400)
                return False
            accept = base64.b64encode(
                hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("utf-8")).digest()
            ).decode("ascii")
            payload = self._websocket_text_frame(json.dumps(data))
            self.send_response(101, "Switching Protocols")
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.connection.sendall(payload)
            self.close_connection = True
            return True
        except Exception as exc:
            if _is_client_disconnect(exc):
                logger.debug("Client disconnected during websocket bootstrap")
                return False
            raise

    def _serve_file(self, path: Path):
        mime_type, _ = mimetypes.guess_type(str(path))
        if not mime_type:
            mime_type = "text/plain"
        with open(path, "rb") as f:
            payload = f.read()
        self._write_response(payload, code=200, content_type=mime_type)

    def _send_json(self, data, code=200):
        payload = json.dumps(data).encode("utf-8")
        return self._write_response(payload, code=code, content_type="application/json")

    def _send_html(self, html: str, code=200):
        payload = html.encode("utf-8")
        return self._write_response(payload, code=code, content_type="text/html; charset=utf-8")

    def _send_404(self):
        self._write_response(b"<h1>404 Not Found</h1>", code=404, content_type="text/html")

    def _write_response(self, payload: bytes, *, code: int = 200, content_type: str = "application/octet-stream") -> bool:
        try:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return True
        except Exception as exc:
            if _is_client_disconnect(exc):
                logger.debug(
                    "Client disconnected before response completed: %s %s",
                    getattr(self, "command", "?"),
                    getattr(self, "path", "?"),
                )
                return False
            raise

def run_server(port: int = 8421) -> None:
    """Bind the HTTP socket before booting long-lived organism threads."""
    logging.basicConfig(level=logging.INFO)
    server = ThreadingHTTPServer(("0.0.0.0", port), MirrorHandler)
    try:
        initialize_organism()
    except Exception:
        server.server_close()
        raise

    print(f"✨ [Personal Mirror] Soul Implantation active on port {port}")
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    import sys

    port = 8421
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    try:
        run_server(port)
    except KeyboardInterrupt:
        print("Shutting down...")
