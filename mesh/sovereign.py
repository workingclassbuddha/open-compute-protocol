"""
mesh.sovereign — Sovereign Mesh, the current Python-first reference
implementation of Open Compute Protocol (OCP) v0.1.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime as dt
import hashlib
import ipaddress
import json
import logging
import os
import re
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from .crypto import SIGNATURE_SCHEME, generate_keypair, sign_message, verify_message

logger = logging.getLogger(__name__)

PROTOCOL_NAME = "Open Compute Protocol"
PROTOCOL_SHORT_NAME = "OCP"
PROTOCOL_RELEASE = "0.1"
IMPLEMENTATION_NAME = "Sovereign Mesh"
PROTOCOL_VERSION = "sovereign-mesh/v1"
MAX_CLOCK_SKEW_SECONDS = 300
OCI_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
OCP_RESULT_CONFIG_MEDIA_TYPE = "application/vnd.ocp.job-result.config.v1+json"
OCP_RESULT_ARTIFACT_TYPE = "application/vnd.ocp.job-result.v1"


class MeshError(RuntimeError):
    pass


class MeshSignatureError(MeshError):
    pass


class MeshReplayError(MeshError):
    pass


class MeshPolicyError(MeshError):
    pass


class MeshArtifactAccessError(MeshError):
    pass


def _utcnow_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def _utcnow() -> str:
    return _utcnow_dt().isoformat().replace("+00:00", "Z")


def _utc_after(seconds: int) -> str:
    return (_utcnow_dt() + dt.timedelta(seconds=max(0, int(seconds)))).isoformat().replace("+00:00", "Z")


def _json_dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _oci_digest(digest: str) -> str:
    token = str(digest or "").strip()
    if not token:
        return ""
    return token if ":" in token else f"sha256:{token}"


def _loads_json(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _b64encode(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def _b64decode(payload: str) -> bytes:
    return base64.b64decode((payload or "").encode("ascii"))


def _normalize_trust_tier(value: Optional[str]) -> str:
    token = (value or "").strip().lower()
    if token in {"self", "trusted", "partner", "market", "public", "blocked"}:
        return token
    return "trusted"


def _is_wildcard_host(host: str) -> bool:
    token = str(host or "").strip().lower()
    return token in {"", "0.0.0.0", "::", "[::]"}


def _is_loopback_host(host: str) -> bool:
    token = str(host or "").strip().lower()
    if token == "localhost":
        return True
    return token.startswith("127.")


def _is_wildcard_or_loopback_host(host: str) -> bool:
    return _is_wildcard_host(host) or _is_loopback_host(host)


def _normalize_base_url(url: str, *, fallback_url: str = "", replace_loopback: bool = False) -> str:
    token = str(url or "").strip().rstrip("/")
    fallback = str(fallback_url or "").strip().rstrip("/")
    if not token:
        return fallback
    parsed = urlparse(token)
    host = parsed.hostname or ""
    if _is_wildcard_host(host) and fallback:
        return fallback
    if replace_loopback and _is_loopback_host(host) and fallback:
        return fallback
    return token


def _discover_local_ipv4_addresses(*, bind_host: str = "") -> list[str]:
    seen: set[str] = set()
    bind_token = str(bind_host or "").strip()
    if bind_token and not _is_wildcard_or_loopback_host(bind_token):
        try:
            if ipaddress.ip_address(bind_token).version == 4:
                seen.add(bind_token)
        except ValueError:
            pass
    for family, _, _, _, sockaddr in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET, socket.SOCK_DGRAM):
        if family != socket.AF_INET or not sockaddr:
            continue
        host = str(sockaddr[0] or "").strip()
        if host and not _is_wildcard_or_loopback_host(host):
            seen.add(host)
    for probe_host in ("192.0.2.1", "10.255.255.255"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((probe_host, 80))
                host = str(sock.getsockname()[0] or "").strip()
                if host and not _is_wildcard_or_loopback_host(host):
                    seen.add(host)
        except OSError:
            continue
    ordered = sorted(
        (
            host
            for host in seen
            if host
            and not _is_wildcard_or_loopback_host(host)
            and ipaddress.ip_address(host).version == 4
        ),
        key=lambda host: (not ipaddress.ip_address(host).is_private, host),
    )
    return ordered


def _preferred_local_base_url(*, bind_host: str = "", port: int = 8421, scheme: str = "http") -> str:
    host_token = str(bind_host or "").strip()
    if host_token and not _is_wildcard_or_loopback_host(host_token):
        return f"{scheme}://{host_token}:{int(port)}"
    addresses = _discover_local_ipv4_addresses(bind_host=bind_host)
    if addresses:
        return f"{scheme}://{addresses[0]}:{int(port)}"
    return f"{scheme}://127.0.0.1:{int(port)}"


ARTIFACT_RETENTION_DEFAULTS = {
    "ephemeral": 3600,
    "session": 604800,
    "durable": 0,
}


def _normalize_retention_class(value: Optional[str]) -> str:
    token = (value or "").strip().lower()
    if token in ARTIFACT_RETENTION_DEFAULTS:
        return token
    return "durable"


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    token = str(value or "").strip().lower()
    return token in {"1", "true", "yes", "on", "enabled"}


def _normalize_policy(raw: Optional[dict]) -> dict:
    data = dict(raw or {})
    classification = (data.get("classification") or data.get("visibility") or data.get("label") or "trusted").strip().lower()
    if classification not in {"private", "trusted", "public"}:
        classification = "trusted"
    mode = (data.get("mode") or data.get("execution_mode") or "batch").strip().lower()
    if mode not in {"interactive", "batch"}:
        mode = "batch"
    secret_scopes = [str(item).strip() for item in (data.get("secret_scopes") or []) if str(item).strip()]
    normalized = {
        "classification": classification,
        "mode": mode,
        "secret_scopes": secret_scopes,
    }
    for key in ("retention", "notes", "max_runtime_seconds"):
        if key in data:
            normalized[key] = data[key]
    return normalized


def _normalize_secret_scopes(raw: Any) -> list[str]:
    return [str(item).strip() for item in (raw or []) if str(item).strip()]


def _normalize_env_var_name(name: Any) -> str:
    token = str(name or "").strip()
    if not token:
        return ""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token):
        raise MeshPolicyError(f"invalid environment variable name: {token}")
    return token


def _normalize_secret_source(value: Any) -> str:
    source = str(value or "inline").strip().lower() or "inline"
    if source not in {"inline", "env", "store", "file"}:
        raise MeshPolicyError(f"unsupported secret source: {source}")
    return source


def _secret_value_digest(value: Any) -> str:
    return _sha256_bytes(str(value).encode("utf-8"))


def _normalize_resources(raw: Optional[dict]) -> dict:
    data = dict(raw or {})
    normalized: dict[str, Any] = {}
    if data.get("cpu") is not None:
        try:
            normalized["cpu"] = max(0.0, float(data.get("cpu") or 0))
        except Exception:
            pass
    if data.get("memory_mb") is not None:
        try:
            normalized["memory_mb"] = max(0, int(data.get("memory_mb") or 0))
        except Exception:
            pass
    if data.get("disk_mb") is not None:
        try:
            normalized["disk_mb"] = max(0, int(data.get("disk_mb") or 0))
        except Exception:
            pass
    if data.get("gpus") is not None:
        try:
            normalized["gpus"] = max(0, int(data.get("gpus") or 0))
        except Exception:
            pass
    if data.get("gpu_vram_mb") is not None:
        try:
            normalized["gpu_vram_mb"] = max(0, int(data.get("gpu_vram_mb") or 0))
        except Exception:
            pass
    if data.get("gpu_class") is not None:
        normalized["gpu_class"] = _normalize_gpu_class(data.get("gpu_class"))
    if data.get("workload_class") is not None:
        normalized["workload_class"] = _normalize_workload_class(data.get("workload_class"))
    if data.get("network") is not None:
        normalized["network"] = str(data.get("network") or "").strip().lower() or "default"
    return normalized


GPU_CLASSES = {"none", "cuda", "rocm", "metal", "mps", "vulkan", "directml", "generic"}
WORKLOAD_CLASSES = {
    "default",
    "cpu_bound",
    "io_bound",
    "gpu_inference",
    "gpu_training",
    "mixed",
}
MISSION_STATUSES = {
    "planned",
    "active",
    "waiting",
    "checkpointed",
    "completed",
    "failed",
    "cancelled",
}
MISSION_PRIORITIES = {"low", "normal", "high", "critical"}


def _normalize_gpu_class(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_")
    if not token:
        return "none"
    alias = {
        "apple": "metal",
        "apple_silicon": "metal",
        "nvidia": "cuda",
        "amd": "rocm",
        "intel": "vulkan",
        "pytorch_mps": "mps",
        "d3d12": "directml",
        "gpu": "generic",
    }
    token = alias.get(token, token)
    if token in GPU_CLASSES:
        return token
    return "generic"


def _normalize_workload_class(value: Any) -> str:
    token = str(value or "").strip().lower()
    if not token:
        return "default"
    alias = {
        "cpu": "cpu_bound",
        "gpu": "gpu_inference",
        "training": "gpu_training",
        "inference": "gpu_inference",
        "io": "io_bound",
    }
    token = alias.get(token, token)
    if token in WORKLOAD_CLASSES:
        return token
    return "default"


def _normalize_mission_status(value: Any) -> str:
    token = str(value or "").strip().lower() or "planned"
    aliases = {
        "pending": "planned",
        "queued": "waiting",
        "accepted": "waiting",
        "retry_wait": "waiting",
        "running": "active",
        "resuming": "active",
        "attention": "failed",
        "rejected": "failed",
        "canceled": "cancelled",
    }
    token = aliases.get(token, token)
    if token in MISSION_STATUSES:
        return token
    return "planned"


def _normalize_mission_priority(value: Any) -> str:
    token = str(value or "").strip().lower() or "normal"
    aliases = {"medium": "normal", "urgent": "critical"}
    token = aliases.get(token, token)
    if token in MISSION_PRIORITIES:
        return token
    return "normal"


def _normalize_target_strategy(value: Any, *, default: str = "local") -> str:
    token = str(value or "").strip().lower()
    token = re.sub(r"[^a-z0-9]+", "_", token).strip("_")
    if not token:
        return default
    aliases = {
        "local_only": "local",
        "single_local": "local",
        "remote_only": "remote",
        "gpu_aware": "cooperative_gpu_aware",
    }
    return aliases.get(token, token)


def _normalize_mission_policy(raw: Optional[dict]) -> dict:
    data = dict(raw or {})
    normalized = _normalize_policy(data)
    for key in ("allow_local", "allow_remote", "auto_enlist", "approval_required", "max_parallel_jobs"):
        if key in data:
            normalized[key] = data[key]
    for key, value in data.items():
        if key not in normalized:
            normalized[key] = value
    return normalized


def _normalize_mission_continuity(raw: Optional[dict]) -> dict:
    data = dict(raw or {})
    resumable = _coerce_bool(
        data.get("resumable")
        if "resumable" in data
        else data.get("preserve_across_restart", True)
    )
    checkpoint_strategy = str(data.get("checkpoint_strategy") or ("inherit" if resumable else "none")).strip().lower()
    if checkpoint_strategy not in {"inherit", "none", "manual", "on_failure", "on_retry"}:
        checkpoint_strategy = "inherit" if resumable else "none"
    continuity = {
        "mode": str(data.get("mode") or ("durable" if resumable else "ephemeral")).strip().lower() or "durable",
        "resumable": resumable,
        "checkpoint_strategy": checkpoint_strategy,
        "allow_handoff": _coerce_bool(data.get("allow_handoff") if "allow_handoff" in data else True),
        "preserve_result_lineage": _coerce_bool(
            data.get("preserve_result_lineage") if "preserve_result_lineage" in data else True
        ),
    }
    if data.get("notes"):
        continuity["notes"] = str(data.get("notes") or "")
    if data.get("status_hint"):
        continuity["status_hint"] = _normalize_mission_status(data.get("status_hint"))
    for key, value in data.items():
        if key not in continuity:
            continuity[key] = value
    return continuity


def _normalize_compute_profile(raw: Optional[dict], device_class: str, execution_tier: str) -> dict:
    data = dict(raw or {})
    cpu_defaults = {
        "heavy": {"cpu_cores": 16, "memory_mb": 32768},
        "standard": {"cpu_cores": 8, "memory_mb": 16384},
        "light": {"cpu_cores": 4, "memory_mb": 6144},
        "control": {"cpu_cores": 2, "memory_mb": 2048},
        "sensor": {"cpu_cores": 1, "memory_mb": 512},
    }.get(execution_tier, {"cpu_cores": 4, "memory_mb": 8192})
    cpu_cores = max(0, int(data.get("cpu_cores") or data.get("cpu") or cpu_defaults["cpu_cores"]))
    cpu_threads_raw = data.get("cpu_threads")
    cpu_threads = max(cpu_cores, int(cpu_threads_raw or cpu_cores))
    memory_mb = max(0, int(data.get("memory_mb") or cpu_defaults["memory_mb"]))
    disk_mb_raw = data.get("disk_mb")
    disk_mb = max(0, int(disk_mb_raw or 0))
    gpu_count = max(0, int(data.get("gpu_count") or data.get("gpus") or 0))
    gpu_class = _normalize_gpu_class(data.get("gpu_class"))
    if gpu_count <= 0:
        gpu_class = "none"
    elif gpu_class == "none":
        gpu_class = "generic"
    gpu_vram_mb = max(0, int(data.get("gpu_vram_mb") or 0))
    fp16_tflops_raw = data.get("fp16_tflops") or data.get("fp16_throughput_tflops") or 0
    try:
        fp16_tflops = max(0.0, float(fp16_tflops_raw))
    except Exception:
        fp16_tflops = 0.0
    accelerators = _unique_tokens(data.get("accelerators"))
    if gpu_count > 0 and gpu_class not in {"none", ""}:
        if gpu_class not in accelerators:
            accelerators.append(gpu_class)
    raw_supports = data.get("supports_workload_classes") or data.get("workload_classes") or []
    supports = []
    for item in raw_supports:
        normalized = _normalize_workload_class(item)
        if normalized not in supports:
            supports.append(normalized)
    if not supports:
        default_supports = ["default", "cpu_bound", "io_bound"]
        if gpu_count > 0:
            default_supports.extend(["gpu_inference", "mixed"])
            if gpu_vram_mb >= 16384 and execution_tier in {"heavy", "standard"}:
                default_supports.append("gpu_training")
        supports = default_supports
    compute_tags = _unique_tokens(data.get("compute_tags"))
    if gpu_count > 0 and "gpu" not in compute_tags:
        compute_tags.append("gpu")
    if gpu_count > 0 and gpu_vram_mb >= 16384 and "large_gpu" not in compute_tags:
        compute_tags.append("large_gpu")
    if cpu_cores >= 16 and "cpu_heavy" not in compute_tags:
        compute_tags.append("cpu_heavy")
    return {
        "cpu_cores": cpu_cores,
        "cpu_threads": cpu_threads,
        "memory_mb": memory_mb,
        "disk_mb": disk_mb,
        "gpu_count": gpu_count,
        "gpu_class": gpu_class,
        "gpu_vram_mb": gpu_vram_mb,
        "fp16_tflops": round(fp16_tflops, 3),
        "accelerators": accelerators,
        "supports_workload_classes": supports,
        "compute_tags": compute_tags,
        "gpu_capable": bool(gpu_count > 0),
    }


def _normalize_offload_policy(raw: Optional[dict], profile: Optional[dict] = None) -> dict:
    data = dict(raw or {})
    base_profile = dict(profile or {})
    device_class = str(base_profile.get("device_class") or "full").strip().lower() or "full"
    power_profile = str(base_profile.get("power_profile") or "line_powered").strip().lower() or "line_powered"
    default_enabled = device_class in {"full", "relay"}
    enabled = _coerce_bool(data.get("enabled")) if "enabled" in data else default_enabled
    mode = str(data.get("mode") or ("approval" if device_class in {"full", "relay"} else "manual")).strip().lower() or "manual"
    if mode not in {"manual", "approval", "auto"}:
        mode = "manual"
    threshold = str(data.get("pressure_threshold") or ("saturated" if power_profile in {"battery", "mixed"} else "elevated")).strip().lower() or "elevated"
    if threshold not in {"idle", "nominal", "elevated", "saturated"}:
        threshold = "elevated"
    allowed_trust_tiers = _unique_tokens(data.get("allowed_trust_tiers") or ["trusted", "partner"])
    if not allowed_trust_tiers:
        allowed_trust_tiers = ["trusted", "partner"]
    allowed_device_classes = _unique_tokens(data.get("allowed_device_classes") or ["full", "relay"])
    if not allowed_device_classes:
        allowed_device_classes = ["full", "relay"]
    approval_trust_tiers = _unique_tokens(data.get("approval_trust_tiers") or ["partner", "market", "public"])
    approval_device_classes = _unique_tokens(data.get("approval_device_classes") or ["light", "micro"])
    max_auto_enlist = max(1, int(data.get("max_auto_enlist") or 2))
    min_candidate_score = int(data.get("min_candidate_score") or 0)
    allow_battery_helpers = _coerce_bool(data.get("allow_battery_helpers")) if "allow_battery_helpers" in data else False
    allow_remote_seek = _coerce_bool(data.get("allow_remote_seek")) if "allow_remote_seek" in data else False
    notify_on_action = _coerce_bool(data.get("notify_on_action")) if "notify_on_action" in data else True
    approval_for_gpu_helpers = (
        _coerce_bool(data.get("approval_for_gpu_helpers"))
        if "approval_for_gpu_helpers" in data
        else True
    )
    allowed_workload_classes = _unique_tokens(data.get("allowed_workload_classes") or [])
    approval_workload_classes = _unique_tokens(data.get("approval_workload_classes") or [])
    target_device_classes = _unique_tokens(data.get("target_device_classes") or ["full", "light", "micro"])
    return {
        "enabled": bool(enabled),
        "mode": mode,
        "pressure_threshold": threshold,
        "max_auto_enlist": max_auto_enlist,
        "min_candidate_score": min_candidate_score,
        "allowed_trust_tiers": allowed_trust_tiers,
        "allowed_device_classes": allowed_device_classes,
        "approval_trust_tiers": approval_trust_tiers,
        "approval_device_classes": approval_device_classes,
        "allow_battery_helpers": bool(allow_battery_helpers),
        "allow_remote_seek": bool(allow_remote_seek),
        "notify_on_action": bool(notify_on_action),
        "approval_for_gpu_helpers": bool(approval_for_gpu_helpers),
        "allowed_workload_classes": allowed_workload_classes,
        "approval_workload_classes": approval_workload_classes,
        "target_device_classes": target_device_classes,
    }


def _pressure_rank(value: Any) -> int:
    token = str(value or "idle").strip().lower() or "idle"
    return {
        "idle": 0,
        "nominal": 1,
        "elevated": 2,
        "saturated": 3,
    }.get(token, 0)


def _normalize_preference_token(value: Any) -> str:
    token = str(value or "").strip().lower() or "allow"
    aliases = {
        "preferred": "prefer",
        "preferred_auto": "prefer",
        "never": "deny",
        "blocked": "deny",
        "ask": "approval",
        "manual": "approval",
    }
    token = aliases.get(token, token)
    if token not in {"prefer", "allow", "approval", "avoid", "deny"}:
        token = "allow"
    return token


def _unique_tokens(values: Any) -> list[str]:
    seen: list[str] = []
    for item in (values or []):
        token = str(item or "").strip().lower()
        if token and token not in seen:
            seen.append(token)
    return seen


def _normalize_device_profile(raw: Optional[dict]) -> dict:
    data = dict(raw or {})
    device_class = str(data.get("device_class") or "full").strip().lower() or "full"
    if device_class not in {"full", "light", "micro", "relay"}:
        device_class = "full"
    execution_tier = str(
        data.get("execution_tier")
        or {"full": "standard", "light": "light", "micro": "sensor", "relay": "control"}.get(device_class, "standard")
    ).strip().lower() or "standard"
    if execution_tier not in {"heavy", "standard", "light", "control", "sensor"}:
        execution_tier = "standard"
    power_profile = str(
        data.get("power_profile")
        or {"full": "line_powered", "light": "battery", "micro": "battery", "relay": "line_powered"}.get(device_class, "line_powered")
    ).strip().lower() or "line_powered"
    if power_profile not in {"line_powered", "battery", "mixed"}:
        power_profile = "line_powered"
    network_profile = str(
        data.get("network_profile")
        or {"micro": "intermittent", "light": "wifi", "relay": "broadband"}.get(device_class, "broadband")
    ).strip().lower() or "broadband"
    if network_profile not in {"wired", "broadband", "wifi", "metered", "intermittent"}:
        network_profile = "broadband"
    mobility = str(
        data.get("mobility")
        or {"full": "fixed", "light": "mobile", "micro": "wearable", "relay": "fixed"}.get(device_class, "fixed")
    ).strip().lower() or "fixed"
    if mobility not in {"fixed", "portable", "mobile", "wearable"}:
        mobility = "fixed"
    form_factor = str(
        data.get("form_factor")
        or {"full": "workstation", "light": "phone", "micro": "watch", "relay": "relay"}.get(device_class, "workstation")
    ).strip().lower() or "workstation"
    if form_factor not in {"server", "workstation", "laptop", "tablet", "phone", "watch", "relay", "edge"}:
        form_factor = "workstation"
    default_compute_ready = execution_tier in {"heavy", "standard", "light"} and device_class != "micro"
    compute_ready = _coerce_bool(data.get("compute_ready")) if "compute_ready" in data else default_compute_ready
    default_accepts_remote_jobs = compute_ready and execution_tier in {"heavy", "standard", "light"} and device_class in {"full", "light"}
    accepts_remote_jobs = (
        _coerce_bool(data.get("accepts_remote_jobs"))
        if "accepts_remote_jobs" in data
        else default_accepts_remote_jobs
    )
    artifact_mirror_capable = (
        _coerce_bool(data.get("artifact_mirror_capable"))
        if "artifact_mirror_capable" in data
        else device_class in {"full", "light", "relay"}
    )
    approval_capable = (
        _coerce_bool(data.get("approval_capable"))
        if "approval_capable" in data
        else device_class in {"full", "light", "micro", "relay"}
    )
    secure_secret_capable = (
        _coerce_bool(data.get("secure_secret_capable"))
        if "secure_secret_capable" in data
        else device_class in {"full", "light", "micro"}
    )
    intermittent = (
        _coerce_bool(data.get("intermittent"))
        if "intermittent" in data
        else network_profile in {"metered", "intermittent"} or mobility in {"mobile", "wearable"}
    )
    sleep_capable = (
        _coerce_bool(data.get("sleep_capable"))
        if "sleep_capable" in data
        else intermittent or device_class in {"light", "micro"} or mobility in {"mobile", "wearable"}
    )
    preferred_sync_interval_seconds = max(
        15,
        int(
            data.get("preferred_sync_interval_seconds")
            or {"full": 30, "relay": 60, "light": 120, "micro": 300}.get(device_class, 60)
        ),
    )
    offline_grace_seconds = max(
        preferred_sync_interval_seconds,
        int(
            data.get("offline_grace_seconds")
            or {"full": 300, "relay": 900, "light": 1800, "micro": 3600}.get(device_class, 900)
        ),
    )
    labels = _unique_tokens(data.get("labels"))
    roles = _unique_tokens(
        data.get("roles")
        or {
            "full": ["compute", "storage", "control"],
            "light": ["control", "approval", "edge"],
            "micro": ["approval", "presence", "sensor"],
            "relay": ["relay", "storage", "control"],
        }.get(device_class, ["compute"])
    )
    if compute_ready and "compute" not in roles:
        roles.append("compute")
    if artifact_mirror_capable and "storage" not in roles and device_class in {"full", "relay"}:
        roles.append("storage")
    if approval_capable and "approval" not in roles and device_class in {"light", "micro"}:
        roles.append("approval")
    compute_profile = _normalize_compute_profile(
        data.get("compute_profile") or {},
        device_class=device_class,
        execution_tier=execution_tier,
    )
    helper_state_token = str(data.get("helper_state") or "active").strip().lower()
    if helper_state_token not in {"active", "draining", "retired"}:
        helper_state_token = "active"
    helper_role = str(data.get("helper_role") or "").strip().lower()
    if helper_role not in {"", "controller", "helper", "relay", "drain"}:
        helper_role = ""
    if compute_profile["gpu_capable"]:
        if "gpu_helper" not in labels:
            labels.append("gpu_helper")
        if "compute" not in roles:
            roles.append("compute")
    offload_policy = _normalize_offload_policy(data.get("offload_policy") or {}, {
        "device_class": device_class,
        "power_profile": power_profile,
    })
    return {
        "device_class": device_class,
        "execution_tier": execution_tier,
        "power_profile": power_profile,
        "network_profile": network_profile,
        "mobility": mobility,
        "form_factor": form_factor,
        "compute_ready": bool(compute_ready),
        "accepts_remote_jobs": bool(accepts_remote_jobs),
        "artifact_mirror_capable": bool(artifact_mirror_capable),
        "approval_capable": bool(approval_capable),
        "secure_secret_capable": bool(secure_secret_capable),
        "intermittent": bool(intermittent),
        "sleep_capable": bool(sleep_capable),
        "battery_powered": power_profile in {"battery", "mixed"},
        "preferred_sync_interval_seconds": preferred_sync_interval_seconds,
        "offline_grace_seconds": offline_grace_seconds,
        "labels": labels,
        "roles": roles,
        "compute_profile": compute_profile,
        "helper_state": helper_state_token,
        "helper_role": helper_role,
        "offload_policy": offload_policy,
    }


def _normalize_notification_status(value: Optional[str]) -> str:
    token = str(value or "").strip().lower() or "unread"
    if token in {"unread", "acked", "dismissed"}:
        return token
    return "unread"


def _normalize_approval_status(value: Optional[str]) -> str:
    token = str(value or "").strip().lower() or "pending"
    if token in {"pending", "approved", "rejected", "deferred", "expired"}:
        return token
    return "pending"


def _compact_text(value: Any, *, limit: int = 120) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


@dataclasses.dataclass
class CapabilityCard:
    name: str
    kind: str
    available: bool
    description: str = ""
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class OrganismCard:
    organism_id: str
    node_id: str
    display_name: str
    public_key: str
    signature_scheme: str
    endpoint_url: str
    stream_url: str
    protocol_version: str
    trust_tier: str
    reachability: str
    supported_features: list[str]
    transports: list[dict[str, Any]]
    capability_cards: list[dict[str, Any]]
    policy_summary: dict[str, Any]
    device_profile: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class AgentPresence:
    organism_id: str
    peer_id: str
    agent_id: str
    agent_name: str
    agent_type: str
    runtime: str
    role: str
    scope: str
    interface: str
    status: str
    mesh_session_id: str
    capabilities: list[str]
    capability_cards: list[dict[str, Any]]
    active_session: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class LeaseRecord:
    id: str
    resource: str
    peer_id: str
    agent_id: str
    job_id: str
    status: str
    ttl_seconds: int
    lock_token: str
    metadata: dict[str, Any]
    created_at: str
    heartbeat_at: str
    expires_at: str
    released_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ArtifactRef:
    id: str
    digest: str
    media_type: str
    size_bytes: int
    owner_peer_id: str
    policy: dict[str, Any]
    path: str
    created_at: str
    metadata: dict[str, Any]
    retention_class: str
    retention_deadline_at: str
    download_url: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class HandoffPacket:
    id: str
    request_id: str
    from_peer_id: str
    to_peer_id: str
    from_agent: str
    to_agent: str
    summary: str
    intent: str
    constraints: dict[str, Any]
    artifact_refs: list[dict[str, Any]]
    status: str
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class WorkerCard:
    id: str
    peer_id: str
    agent_id: str
    status: str
    capabilities: list[str]
    resources: dict[str, Any]
    labels: list[str]
    max_concurrent_jobs: int
    metadata: dict[str, Any]
    created_at: str
    updated_at: str
    last_heartbeat_at: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class JobAttempt:
    id: str
    job_id: str
    attempt_number: int
    worker_id: str
    status: str
    lease_id: str
    executor: str
    result_ref: dict[str, Any]
    error: str
    metadata: dict[str, Any]
    started_at: str
    heartbeat_at: str
    finished_at: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class QueueMessage:
    id: str
    job_id: str
    queue_name: str
    status: str
    dedupe_key: str
    ack_deadline_seconds: int
    dead_letter_queue: str
    delivery_attempts: int
    visibility_timeout_at: str
    available_at: str
    claimed_at: str
    acked_at: str
    replay_deadline_at: str
    retention_deadline_at: str
    lease_id: str
    worker_id: str
    current_attempt_id: str
    last_error: str
    metadata: dict[str, Any]
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class MeshJob:
    id: str
    request_id: str
    kind: str
    origin: str
    target: str
    requirements: dict[str, Any]
    policy: dict[str, Any]
    payload_ref: dict[str, Any]
    artifact_inputs: list[dict[str, Any]]
    status: str
    result_ref: dict[str, Any]
    lease: dict[str, Any]
    metadata: dict[str, Any]
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class MissionRecord:
    id: str
    request_id: str
    title: str
    intent: str
    status: str
    priority: str
    workload_class: str
    origin_peer_id: str
    target_strategy: str
    policy: dict[str, Any]
    continuity: dict[str, Any]
    metadata: dict[str, Any]
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


class GolemMeshAdapter:
    def __init__(self, *, enabled: bool = False):
        self.enabled = bool(enabled)

    def capability_cards(self) -> list[dict[str, Any]]:
        return [
            CapabilityCard(
                name="golem-provider",
                kind="provider",
                available=self.enabled,
                description="Broker public or sandbox-approved workloads through a Golem lane.",
                metadata={"external_market": True},
            ).to_dict()
        ]

    def can_accept(self, policy: dict) -> bool:
        return self.enabled and (policy.get("classification") == "public")

    def execute_job(self, job_kind: str, payload: dict, policy: dict) -> dict:
        if not self.enabled:
            raise MeshPolicyError("golem adapter is not enabled on this organism")
        if policy.get("classification") != "public":
            raise MeshPolicyError("golem workloads must be public in v1")
        return {
            "status": "queued",
            "provider": "golem",
            "job_kind": job_kind,
            "payload": payload,
            "policy": policy,
            "adapter": "golem-mesh",
        }


class MeshPeerClient:
    def __init__(self, base_url: str, *, timeout: float = 8.0):
        self.base_url = (base_url or "").rstrip("/")
        self.timeout = float(timeout)

    def _request_json(self, method: str, path: str, payload: Optional[dict] = None, params: Optional[dict] = None) -> dict:
        url = self.base_url + path
        if params:
            query = urlencode({k: v for k, v in params.items() if v not in (None, "")})
            if query:
                url += ("&" if "?" in url else "?") + query
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(url, data=data, headers=headers, method=method.upper())
        with urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def manifest(self) -> dict:
        return self._request_json("GET", "/mesh/manifest")

    def device_profile(self) -> dict:
        return self._request_json("GET", "/mesh/device-profile")

    def update_device_profile(self, profile: dict) -> dict:
        return self._request_json("POST", "/mesh/device-profile", payload={"device_profile": dict(profile or {})})

    def stream_snapshot(self, *, since: int = 0, limit: int = 50) -> dict:
        return self._request_json("GET", "/mesh/stream", params={"since": since, "limit": limit})

    def sync_peers(self, *, peer_id: str = "", limit: int = 100, refresh_manifest: bool = False) -> dict:
        payload = {"limit": limit, "refresh_manifest": refresh_manifest}
        if peer_id:
            payload["peer_id"] = peer_id
        return self._request_json("POST", "/mesh/peers/sync", payload=payload)

    def list_discovery_candidates(self, *, limit: int = 25, status: str = "") -> dict:
        return self._request_json("GET", "/mesh/discovery/candidates", params={"limit": limit, "status": status})

    def seek_peers(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/discovery/seek", payload=payload)

    def scan_local_peers(self, payload: Optional[dict] = None) -> dict:
        return self._request_json("POST", "/mesh/discovery/scan-local", payload=payload or {})

    def connectivity_diagnostics(self) -> dict:
        return self._request_json("GET", "/mesh/connectivity/diagnostics")

    def connect_peer(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/peers/connect", payload=payload)

    def connect_all_peers(self, payload: Optional[dict] = None) -> dict:
        return self._request_json("POST", "/mesh/peers/connect-all", payload=payload or {})

    def mesh_pressure(self) -> dict:
        return self._request_json("GET", "/mesh/pressure")

    def list_helpers(self, *, limit: int = 100) -> dict:
        return self._request_json("GET", "/mesh/helpers", params={"limit": limit})

    def list_offload_preferences(self, *, limit: int = 100, peer_id: str = "", workload_class: str = "") -> dict:
        return self._request_json(
            "GET",
            "/mesh/helpers/preferences",
            params={"limit": limit, "peer_id": peer_id, "workload_class": workload_class},
        )

    def set_offload_preference(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/helpers/preferences/set", payload=payload)

    def plan_helper_enlistment(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/helpers/plan", payload=payload)

    def enlist_helper(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/helpers/enlist", payload=payload)

    def drain_helper(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/helpers/drain", payload=payload)

    def retire_helper(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/helpers/retire", payload=payload)

    def auto_seek_help(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/helpers/auto-seek", payload=payload)

    def evaluate_autonomous_offload(self) -> dict:
        return self._request_json("GET", "/mesh/helpers/autonomy")

    def run_autonomous_offload(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/helpers/autonomy/run", payload=payload)

    def handshake(self, envelope: dict) -> dict:
        return self._request_json("POST", "/mesh/handshake", payload=envelope)

    def submit_job(self, envelope: dict) -> dict:
        return self._request_json("POST", "/mesh/jobs/submit", payload=envelope)

    def schedule_job(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/jobs/schedule", payload=payload)

    def launch_cooperative_task(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/cooperative-tasks/launch", payload=payload)

    def list_cooperative_tasks(self, *, limit: int = 25, state: str = "") -> dict:
        return self._request_json("GET", "/mesh/cooperative-tasks", params={"limit": limit, "state": state})

    def get_cooperative_task(self, task_id: str) -> dict:
        return self._request_json("GET", f"/mesh/cooperative-tasks/{task_id}")

    def list_missions(self, *, limit: int = 25, status: str = "") -> dict:
        return self._request_json("GET", "/mesh/missions", params={"limit": limit, "status": status})

    def get_mission(self, mission_id: str) -> dict:
        return self._request_json("GET", f"/mesh/missions/{mission_id}")

    def launch_mission(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/missions/launch", payload=payload)

    def launch_test_mission(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/missions/test-launch", payload=payload)

    def cancel_mission(self, mission_id: str, *, reason: str = "mission_cancelled", operator_id: str = "") -> dict:
        payload = {"reason": reason}
        if operator_id:
            payload["operator_id"] = operator_id
        return self._request_json("POST", f"/mesh/missions/{mission_id}/cancel", payload=payload)

    def resume_mission(self, mission_id: str, *, reason: str = "mission_resume_latest", operator_id: str = "") -> dict:
        payload = {"reason": reason}
        if operator_id:
            payload["operator_id"] = operator_id
        return self._request_json("POST", f"/mesh/missions/{mission_id}/resume", payload=payload)

    def resume_mission_from_checkpoint(
        self,
        mission_id: str,
        *,
        reason: str = "mission_resume_checkpoint",
        operator_id: str = "",
        checkpoint_artifact_id: str = "",
    ) -> dict:
        payload = {"reason": reason}
        if operator_id:
            payload["operator_id"] = operator_id
        if checkpoint_artifact_id:
            payload["checkpoint_artifact_id"] = checkpoint_artifact_id
        return self._request_json("POST", f"/mesh/missions/{mission_id}/resume-from-checkpoint", payload=payload)

    def restart_mission(self, mission_id: str, *, reason: str = "mission_restart", operator_id: str = "") -> dict:
        payload = {"reason": reason}
        if operator_id:
            payload["operator_id"] = operator_id
        return self._request_json("POST", f"/mesh/missions/{mission_id}/restart", payload=payload)

    def list_scheduler_decisions(self, *, limit: int = 25, status: str = "", target_type: str = "") -> dict:
        return self._request_json(
            "GET",
            "/mesh/scheduler/decisions",
            params={"limit": limit, "status": status, "target_type": target_type},
        )

    def get_job(self, job_id: str) -> dict:
        return self._request_json("GET", f"/mesh/jobs/{job_id}")

    def cancel_job(self, job_id: str, *, reason: str = "cancelled") -> dict:
        return self._request_json("POST", f"/mesh/jobs/{job_id}/cancel", payload={"reason": reason})

    def resume_job(
        self,
        job_id: str,
        *,
        reason: str = "operator_resume_latest",
        operator_id: str = "",
    ) -> dict:
        payload = {"reason": reason}
        if operator_id:
            payload["operator_id"] = operator_id
        return self._request_json("POST", f"/mesh/jobs/{job_id}/resume", payload=payload)

    def resume_job_from_checkpoint(
        self,
        job_id: str,
        *,
        checkpoint_artifact_id: str,
        reason: str = "operator_resume_checkpoint",
        operator_id: str = "",
    ) -> dict:
        payload = {
            "checkpoint_artifact_id": checkpoint_artifact_id,
            "reason": reason,
        }
        if operator_id:
            payload["operator_id"] = operator_id
        return self._request_json("POST", f"/mesh/jobs/{job_id}/resume-from-checkpoint", payload=payload)

    def restart_job(
        self,
        job_id: str,
        *,
        reason: str = "operator_restart",
        operator_id: str = "",
    ) -> dict:
        payload = {"reason": reason}
        if operator_id:
            payload["operator_id"] = operator_id
        return self._request_json("POST", f"/mesh/jobs/{job_id}/restart", payload=payload)

    def publish_artifact(self, envelope: dict) -> dict:
        return self._request_json("POST", "/mesh/artifacts/publish", payload=envelope)

    def list_artifacts(
        self,
        *,
        limit: int = 25,
        artifact_kind: str = "",
        digest: str = "",
        job_id: str = "",
        attempt_id: str = "",
        parent_artifact_id: str = "",
        owner_peer_id: str = "",
        media_type: str = "",
        retention_class: str = "",
    ) -> dict:
        return self._request_json(
            "GET",
            "/mesh/artifacts",
            params={
                "limit": limit,
                "artifact_kind": artifact_kind,
                "digest": digest,
                "job_id": job_id,
                "attempt_id": attempt_id,
                "parent_artifact_id": parent_artifact_id,
                "owner_peer_id": owner_peer_id,
                "media_type": media_type,
                "retention_class": retention_class,
            },
        )

    def get_artifact(self, artifact_id: str, *, peer_id: str = "", include_content: bool = True) -> dict:
        return self._request_json(
            "GET",
            f"/mesh/artifacts/{artifact_id}",
            params={"peer_id": peer_id, "include_content": 1 if include_content else 0},
        )

    def replicate_artifact(
        self,
        *,
        peer_id: str,
        artifact_id: str = "",
        digest: str = "",
        pin: bool = False,
    ) -> dict:
        payload = {"peer_id": peer_id, "pin": bool(pin)}
        if artifact_id:
            payload["artifact_id"] = artifact_id
        if digest:
            payload["digest"] = digest
        return self._request_json("POST", "/mesh/artifacts/replicate", payload=payload)

    def replicate_artifact_graph(
        self,
        *,
        peer_id: str,
        artifact_id: str = "",
        digest: str = "",
        pin: bool = False,
    ) -> dict:
        payload = {"peer_id": peer_id, "pin": bool(pin)}
        if artifact_id:
            payload["artifact_id"] = artifact_id
        if digest:
            payload["digest"] = digest
        return self._request_json("POST", "/mesh/artifacts/replicate-graph", payload=payload)

    def set_artifact_pin(self, artifact_id: str, *, pinned: bool = True, reason: str = "operator_pin") -> dict:
        return self._request_json(
            "POST",
            "/mesh/artifacts/pin",
            payload={"artifact_id": artifact_id, "pinned": bool(pinned), "reason": reason},
        )

    def verify_artifact_mirror(
        self,
        artifact_id: str,
        *,
        peer_id: str = "",
        source_artifact_id: str = "",
        digest: str = "",
    ) -> dict:
        payload = {"artifact_id": artifact_id}
        if peer_id:
            payload["peer_id"] = peer_id
        if source_artifact_id:
            payload["source_artifact_id"] = source_artifact_id
        if digest:
            payload["digest"] = digest
        return self._request_json("POST", "/mesh/artifacts/verify-mirror", payload=payload)

    def purge_artifacts(self, *, limit: int = 100) -> dict:
        return self._request_json("POST", "/mesh/artifacts/purge", payload={"limit": limit})

    def submit_handoff(self, envelope: dict) -> dict:
        return self._request_json("POST", "/mesh/agents/handoff", payload=envelope)

    def list_notifications(
        self,
        *,
        limit: int = 25,
        status: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
    ) -> dict:
        return self._request_json(
            "GET",
            "/mesh/notifications",
            params={
                "limit": limit,
                "status": status,
                "target_peer_id": target_peer_id,
                "target_agent_id": target_agent_id,
            },
        )

    def publish_notification(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/notifications/publish", payload=payload)

    def ack_notification(
        self,
        notification_id: str,
        *,
        status: str = "acked",
        actor_peer_id: str = "",
        actor_agent_id: str = "",
        reason: str = "",
    ) -> dict:
        payload = {"status": status}
        if actor_peer_id:
            payload["actor_peer_id"] = actor_peer_id
        if actor_agent_id:
            payload["actor_agent_id"] = actor_agent_id
        if reason:
            payload["reason"] = reason
        return self._request_json("POST", f"/mesh/notifications/{notification_id}/ack", payload=payload)

    def list_approvals(
        self,
        *,
        limit: int = 25,
        status: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
    ) -> dict:
        return self._request_json(
            "GET",
            "/mesh/approvals",
            params={
                "limit": limit,
                "status": status,
                "target_peer_id": target_peer_id,
                "target_agent_id": target_agent_id,
            },
        )

    def request_approval(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/approvals/request", payload=payload)

    def resolve_approval(
        self,
        approval_id: str,
        *,
        decision: str,
        operator_peer_id: str = "",
        operator_agent_id: str = "",
        reason: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        payload = {"decision": decision}
        if operator_peer_id:
            payload["operator_peer_id"] = operator_peer_id
        if operator_agent_id:
            payload["operator_agent_id"] = operator_agent_id
        if reason:
            payload["reason"] = reason
        if metadata:
            payload["metadata"] = dict(metadata)
        return self._request_json("POST", f"/mesh/approvals/{approval_id}/resolve", payload=payload)

    def list_workers(self, *, limit: int = 25) -> dict:
        return self._request_json("GET", "/mesh/workers", params={"limit": limit})

    def list_queue_messages(self, *, limit: int = 25, status: str = "") -> dict:
        return self._request_json("GET", "/mesh/queue", params={"limit": limit, "status": status})

    def list_queue_events(
        self,
        *,
        since_seq: int = 0,
        limit: int = 50,
        queue_message_id: str = "",
        job_id: str = "",
    ) -> dict:
        return self._request_json(
            "GET",
            "/mesh/queue/events",
            params={
                "since": since_seq,
                "limit": limit,
                "queue_message_id": queue_message_id,
                "job_id": job_id,
            },
        )

    def queue_metrics(self) -> dict:
        return self._request_json("GET", "/mesh/queue/metrics")

    def replay_queue_message(self, *, queue_message_id: str = "", job_id: str = "", reason: str = "operator_replay") -> dict:
        payload = {"reason": reason}
        if queue_message_id:
            payload["queue_message_id"] = queue_message_id
        if job_id:
            payload["job_id"] = job_id
        return self._request_json("POST", "/mesh/queue/replay", payload=payload)

    def set_queue_ack_deadline(
        self,
        *,
        queue_message_id: str = "",
        attempt_id: str = "",
        ttl_seconds: int = 0,
        reason: str = "operator_ack_deadline_update",
    ) -> dict:
        payload = {"ttl_seconds": ttl_seconds, "reason": reason}
        if queue_message_id:
            payload["queue_message_id"] = queue_message_id
        if attempt_id:
            payload["attempt_id"] = attempt_id
        return self._request_json("POST", "/mesh/queue/ack-deadline", payload=payload)

    def register_worker(self, payload: dict) -> dict:
        return self._request_json("POST", "/mesh/workers/register", payload=payload)

    def heartbeat_worker(self, worker_id: str, payload: Optional[dict] = None) -> dict:
        return self._request_json("POST", f"/mesh/workers/{worker_id}/heartbeat", payload=payload or {})

    def poll_jobs(self, worker_id: str, *, limit: int = 10) -> dict:
        return self._request_json("POST", f"/mesh/workers/{worker_id}/poll", payload={"limit": limit})

    def claim_job(self, worker_id: str, *, job_id: str = "", ttl_seconds: int = 0) -> dict:
        payload = {"ttl_seconds": ttl_seconds}
        if job_id:
            payload["job_id"] = job_id
        return self._request_json("POST", f"/mesh/workers/{worker_id}/claim", payload=payload)

    def heartbeat_attempt(self, attempt_id: str, *, ttl_seconds: int = 300, metadata: Optional[dict] = None) -> dict:
        payload = {"ttl_seconds": ttl_seconds}
        if metadata:
            payload["metadata"] = dict(metadata)
        return self._request_json("POST", f"/mesh/jobs/attempts/{attempt_id}/heartbeat", payload=payload)

    def complete_attempt(
        self,
        attempt_id: str,
        result: Any,
        *,
        media_type: str = "application/json",
        executor: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        payload = {"result": result, "media_type": media_type}
        if executor:
            payload["executor"] = executor
        if metadata:
            payload["metadata"] = dict(metadata)
        return self._request_json("POST", f"/mesh/jobs/attempts/{attempt_id}/complete", payload=payload)

    def fail_attempt(
        self,
        attempt_id: str,
        *,
        error: str,
        retryable: bool = True,
        metadata: Optional[dict] = None,
    ) -> dict:
        payload = {"error": error, "retryable": bool(retryable)}
        if metadata:
            payload["metadata"] = dict(metadata)
        return self._request_json("POST", f"/mesh/jobs/attempts/{attempt_id}/fail", payload=payload)


class HostMeshAdapter:
    def __init__(self, mesh: "SovereignMesh"):
        self.mesh = mesh

    def export_agent_presence(self, *, limit: int = 25) -> list[dict]:
        return self.mesh.export_agent_presence(limit=limit)

    def export_beacons(self, *, limit: int = 10) -> list[dict]:
        return self.mesh.export_beacons(limit=limit)

    def build_handoff_packet(
        self,
        *,
        to_peer_id: str,
        from_agent: str,
        to_agent: str,
        summary: str,
        intent: str,
        constraints: Optional[dict] = None,
        artifact_refs: Optional[list[dict]] = None,
        request_id: Optional[str] = None,
    ) -> dict:
        body = {
            "handoff": {
                "to_peer_id": to_peer_id,
                "from_agent": from_agent,
                "to_agent": to_agent,
                "summary": summary,
                "intent": intent,
                "constraints": dict(constraints or {}),
                "artifact_refs": list(artifact_refs or []),
            }
        }
        return self.mesh.build_signed_envelope("/mesh/agents/handoff", body, request_id=request_id)

    def build_remote_metabolism_job(
        self,
        *,
        target_peer_id: str,
        kind: str,
        topic: str,
        payload: Optional[dict] = None,
        policy: Optional[dict] = None,
        request_id: Optional[str] = None,
    ) -> dict:
        body = {
            "job": {
                "kind": "host.runtime.trigger",
                "origin": self.mesh.node_id,
                "target": target_peer_id,
                "requirements": {"capabilities": ["metabolism-executor"]},
                "policy": _normalize_policy(policy or {"classification": "trusted", "mode": "batch"}),
                "payload": {
                    "kind": kind,
                    "topic": topic,
                    "payload": dict(payload or {}),
                },
                "artifact_inputs": [],
            }
        }
        return self.mesh.build_signed_envelope("/mesh/jobs/submit", body, request_id=request_id)


PersonalMirrorMeshAdapter = HostMeshAdapter


class SovereignMesh:
    def __init__(
        self,
        lattice,
        *,
        registry=None,
        metabolism=None,
        swarm=None,
        workspace_root: Optional[str] = None,
        base_url: Optional[str] = None,
        identity_dir: Optional[str] = None,
        display_name: Optional[str] = None,
        node_id: Optional[str] = None,
        golem_enabled: bool = False,
        docker_enabled: Optional[bool] = None,
        wasm_enabled: Optional[bool] = None,
        device_profile: Optional[dict] = None,
    ):
        self.lattice = lattice
        self.registry = registry
        self.metabolism = metabolism
        self.swarm = swarm
        self.workspace_root = Path(workspace_root or Path.cwd()).resolve()
        self.mesh_root = Path(identity_dir or (self.workspace_root / ".mesh")).resolve()
        self.artifact_root = self.mesh_root / "artifacts"
        self.mesh_root.mkdir(parents=True, exist_ok=True)
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self.base_url = (
            base_url
            or os.environ.get("OCP_BASE_URL")
            or os.environ.get("PERSONAL_MIRROR_BASE_URL")
            or "http://localhost:8421"
        ).rstrip("/")
        self.docker_enabled = self._resolve_docker_enabled(docker_enabled)
        self.wasm_enabled = self._resolve_wasm_enabled(wasm_enabled)
        self.wasm_runtime = self._resolve_wasm_runtime()
        self.node_id, self.display_name, self.private_key, self.public_key = self._load_or_create_identity(
            explicit_node_id=node_id,
            explicit_display_name=display_name,
        )
        self.device_profile = self._load_or_create_device_profile(explicit_device_profile=device_profile)
        self.golem_adapter = GolemMeshAdapter(enabled=golem_enabled)
        self.host_adapter = HostMeshAdapter(self)
        self.mirror_adapter = self.host_adapter
        self._init_db()

    def _resolve_docker_enabled(self, explicit: Optional[bool]) -> bool:
        if explicit is not None:
            return bool(explicit)
        env_value = str(os.environ.get("OCP_ENABLE_DOCKER") or "").strip().lower()
        if env_value in {"1", "true", "yes", "on", "enabled"}:
            return True
        if env_value in {"0", "false", "no", "off", "disabled"}:
            return False
        return shutil.which("docker") is not None

    def _resolve_wasm_enabled(self, explicit: Optional[bool]) -> bool:
        if explicit is not None:
            return bool(explicit)
        env_value = str(os.environ.get("OCP_ENABLE_WASM") or "").strip().lower()
        if env_value in {"1", "true", "yes", "on", "enabled"}:
            return True
        if env_value in {"0", "false", "no", "off", "disabled"}:
            return False
        return shutil.which("wasmtime") is not None

    def _resolve_wasm_runtime(self) -> str:
        explicit = str(
            os.environ.get("OCP_WASM_RUNTIME")
            or os.environ.get("OCP_WASMTIME_BIN")
            or ""
        ).strip()
        if explicit:
            return explicit
        return shutil.which("wasmtime") or "wasmtime"

    def _load_or_create_identity(
        self,
        *,
        explicit_node_id: Optional[str] = None,
        explicit_display_name: Optional[str] = None,
    ) -> tuple[str, str, str, str]:
        path = self.mesh_root / "identity.json"
        if path.exists():
            data = _loads_json(path.read_text(encoding="utf-8"), {})
            private_key = (data.get("private_key") or "").strip()
            public_key = (data.get("public_key") or "").strip()
            node_id = (explicit_node_id or data.get("node_id") or "").strip()
            display_name = (explicit_display_name or data.get("display_name") or "").strip()
            if private_key and public_key and node_id:
                return node_id, display_name or node_id, private_key, public_key

        private_key, public_key = generate_keypair()
        hostname = socket.gethostname().split(".")[0] or "organism"
        node_id = (explicit_node_id or f"{hostname}-{uuid.uuid4().hex[:8]}").strip()
        display_name = (explicit_display_name or f"{hostname} organism").strip()
        payload = {
            "node_id": node_id,
            "display_name": display_name,
            "private_key": private_key,
            "public_key": public_key,
            "signature_scheme": SIGNATURE_SCHEME,
            "created_at": _utcnow(),
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return node_id, display_name, private_key, public_key

    def _load_or_create_device_profile(self, *, explicit_device_profile: Optional[dict] = None) -> dict:
        path = self.mesh_root / "device_profile.json"
        stored = {}
        if path.exists():
            stored = _loads_json(path.read_text(encoding="utf-8"), {})
        merged = dict(stored)
        if explicit_device_profile is not None:
            explicit_map = dict(explicit_device_profile or {})
            for key in (
                "compute_ready",
                "accepts_remote_jobs",
                "artifact_mirror_capable",
                "approval_capable",
                "secure_secret_capable",
                "intermittent",
                "sleep_capable",
                "battery_powered",
                "preferred_sync_interval_seconds",
                "offline_grace_seconds",
            ):
                if key not in explicit_map:
                    merged.pop(key, None)
            merged.update(explicit_map)
        profile = _normalize_device_profile(merged)
        path.write_text(json.dumps(profile, indent=2, sort_keys=True), encoding="utf-8")
        return profile

    def update_device_profile(self, profile: Optional[dict]) -> dict:
        incoming = dict(profile or {})
        current = dict(self.device_profile or {})
        for key in (
            "compute_ready",
            "accepts_remote_jobs",
            "artifact_mirror_capable",
            "approval_capable",
            "secure_secret_capable",
            "intermittent",
            "sleep_capable",
            "battery_powered",
            "preferred_sync_interval_seconds",
            "offline_grace_seconds",
        ):
            if key not in incoming:
                current.pop(key, None)
        if "compute_profile" in incoming:
            current_compute = dict(current.get("compute_profile") or {})
            incoming_compute = dict(incoming.get("compute_profile") or {})
            current_compute.update(incoming_compute)
            incoming["compute_profile"] = current_compute
        if "offload_policy" in incoming:
            current_offload = dict(current.get("offload_policy") or {})
            incoming_offload = dict(incoming.get("offload_policy") or {})
            current_offload.update(incoming_offload)
            incoming["offload_policy"] = current_offload
        current.update(incoming)
        self.device_profile = self._load_or_create_device_profile(explicit_device_profile=current)
        return {
            "status": "ok",
            "device_profile": dict(self.device_profile),
            "updated_at": _utcnow(),
        }

    def _conn(self):
        return self.lattice._conn()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
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
                """
            )
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

    def _canonical_signing_bytes(self, route: str, body: dict, request_meta: dict) -> bytes:
        payload = {
            "route": route,
            "body": body,
            "node_id": request_meta["node_id"],
            "timestamp": request_meta["timestamp"],
            "nonce": request_meta["nonce"],
            "request_id": request_meta["request_id"],
            "protocol_version": request_meta["protocol_version"],
        }
        return _json_dump(payload).encode("utf-8")

    def build_signed_envelope(
        self,
        route: str,
        body: dict,
        *,
        request_id: Optional[str] = None,
        timestamp: Optional[str] = None,
        nonce: Optional[str] = None,
    ) -> dict:
        request_meta = {
            "node_id": self.node_id,
            "timestamp": timestamp or _utcnow(),
            "nonce": nonce or uuid.uuid4().hex,
            "request_id": request_id or uuid.uuid4().hex,
            "protocol_family": PROTOCOL_SHORT_NAME,
            "protocol_release": PROTOCOL_RELEASE,
            "implementation": IMPLEMENTATION_NAME,
            "protocol_version": PROTOCOL_VERSION,
        }
        signature = sign_message(
            self.private_key,
            self._canonical_signing_bytes(route, body, request_meta),
        )
        request_meta["signature_scheme"] = SIGNATURE_SCHEME
        request_meta["signature"] = signature
        return {"request": request_meta, "body": dict(body or {})}

    def _parse_timestamp(self, value: str) -> dt.datetime:
        sample = (value or "").strip()
        if not sample:
            raise MeshSignatureError("timestamp is required")
        if sample.endswith("Z"):
            sample = sample[:-1] + "+00:00"
        return dt.datetime.fromisoformat(sample).astimezone(dt.timezone.utc)

    def _remember_nonce(self, peer_id: str, nonce: str, route: str, request_id: str) -> None:
        with self._conn() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO mesh_seen_nonces (peer_id, nonce, route, request_id, seen_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (peer_id, nonce, route, request_id, _utcnow()),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise MeshReplayError(f"nonce already seen for peer {peer_id}") from exc

    def _get_peer_row(self, peer_id: str):
        with self._conn() as conn:
            return conn.execute("SELECT * FROM mesh_peers WHERE peer_id=?", ((peer_id or "").strip(),)).fetchone()

    def _verify_envelope(
        self,
        envelope: dict,
        *,
        route: str,
        peer_card: Optional[dict] = None,
    ) -> tuple[str, dict, dict, Optional[dict]]:
        request_meta = dict(envelope.get("request") or {})
        body = dict(envelope.get("body") or {})
        if not request_meta:
            raise MeshSignatureError("request metadata is required")

        peer_id = (request_meta.get("node_id") or "").strip()
        if not peer_id:
            raise MeshSignatureError("node_id is required")

        protocol_version = (request_meta.get("protocol_version") or "").strip()
        if protocol_version != PROTOCOL_VERSION:
            raise MeshSignatureError("unsupported mesh protocol version")

        signature_scheme = (request_meta.get("signature_scheme") or "").strip()
        if signature_scheme != SIGNATURE_SCHEME:
            raise MeshSignatureError("unsupported signature scheme")

        timestamp = self._parse_timestamp(request_meta.get("timestamp"))
        if abs((_utcnow_dt() - timestamp).total_seconds()) > MAX_CLOCK_SKEW_SECONDS:
            raise MeshSignatureError("stale or future timestamp")

        signature = (request_meta.get("signature") or "").strip()
        if not signature:
            raise MeshSignatureError("signature is required")

        public_key = ""
        peer_row = None
        if peer_card is not None:
            public_key = (peer_card.get("public_key") or "").strip()
        else:
            peer_row = self._get_peer_row(peer_id)
            if peer_row is None:
                raise MeshSignatureError(f"unknown peer {peer_id}")
            public_key = (peer_row["public_key"] or "").strip()

        if not public_key:
            raise MeshSignatureError("public key unavailable for verification")

        signing_bytes = self._canonical_signing_bytes(route, body, request_meta)
        if not verify_message(public_key, signing_bytes, signature):
            raise MeshSignatureError("signature verification failed")

        self._remember_nonce(
            peer_id,
            (request_meta.get("nonce") or "").strip(),
            route,
            (request_meta.get("request_id") or "").strip(),
        )
        return peer_id, request_meta, body, self._row_to_peer(peer_row) if peer_row is not None else None

    def _record_event(self, event_type: str, *, peer_id: str = "", request_id: str = "", payload: Optional[dict] = None) -> dict:
        payload = dict(payload or {})
        event_id = str(uuid.uuid4())
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_events (id, event_type, peer_id, request_id, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event_id, event_type, peer_id or None, request_id or None, json.dumps(payload), now),
            )
            row = conn.execute("SELECT * FROM mesh_events WHERE id=?", (event_id,)).fetchone()
            conn.commit()
        try:
            self.lattice.log_event(
                "mesh",
                f"{event_type} · {peer_id or self.node_id}",
                source="sovereign_mesh",
                payload=payload,
            )
        except Exception:
            logger.debug("mesh event mirror logging failed", exc_info=True)
        if self.registry is not None:
            try:
                self.registry.log_action(
                    event_type,
                    agent_id=peer_id or self.node_id,
                    agent_name=payload.get("display_name") or payload.get("from_agent") or payload.get("to_agent"),
                    resource=payload.get("resource") or payload.get("job_id") or payload.get("handoff_id"),
                    details=payload,
                )
            except Exception:
                logger.debug("mesh registry logging failed", exc_info=True)
        return self._row_to_event(row)

    def _policy_allows_peer(self, policy: dict, peer: Optional[dict]) -> bool:
        classification = policy.get("classification") or "trusted"
        trust_tier = "public" if peer is None else _normalize_trust_tier(peer.get("trust_tier"))
        if trust_tier == "blocked":
            return False
        if trust_tier == "self":
            return True
        if classification == "public":
            return trust_tier in {"trusted", "partner", "market", "public"}
        if classification == "trusted":
            return trust_tier in {"trusted", "partner"}
        return trust_tier in {"trusted", "partner"}

    def capability_cards(self) -> list[dict]:
        cards = [
            CapabilityCard(
                name="python",
                kind="runtime",
                available=True,
                description="Execute Python-backed mesh adapters and bounded workers.",
            ).to_dict(),
            CapabilityCard(
                name="shell",
                kind="runtime",
                available=True,
                description="Execute bounded shell commands through the durable worker runtime.",
            ).to_dict(),
            CapabilityCard(
                name="docker",
                kind="runtime",
                available=self.docker_enabled,
                description="Execute OCI-compatible containers through the local Docker runtime.",
            ).to_dict(),
            CapabilityCard(
                name="wasm",
                kind="runtime",
                available=self.wasm_enabled,
                description="Execute WebAssembly components through a local Wasmtime runtime.",
            ).to_dict(),
            CapabilityCard(
                name="agent-runtime",
                kind="coordination",
                available=True,
                description="Export agent presence, sessions, and handoff packets.",
            ).to_dict(),
            CapabilityCard(
                name="worker-runtime",
                kind="executor",
                available=True,
                description="Queue durable mesh jobs and run them through registered workers.",
            ).to_dict(),
            CapabilityCard(
                name="artifact-store",
                kind="storage",
                available=True,
                description="Store signed artifacts and exchange refs between organisms.",
            ).to_dict(),
            CapabilityCard(
                name="secret-store",
                kind="storage",
                available=True,
                description="Resolve scoped local secrets for runtime delivery without surfacing raw values in job metadata.",
            ).to_dict(),
            CapabilityCard(
                name="registry-locking",
                kind="coordination",
                available=self.registry is not None,
                description="Map advisory mesh leases into the local registry when available.",
            ).to_dict(),
            CapabilityCard(
                name="metabolism-executor",
                kind="executor",
                available=self.metabolism is not None,
                description="Run bounded background workload triggers through a local host integration.",
            ).to_dict(),
            CapabilityCard(
                name="swarm-submit",
                kind="executor",
                available=self.swarm is not None,
                description="Accept bounded swarm finding ingestion as a remote execution target.",
            ).to_dict(),
            CapabilityCard(
                name="operator-control",
                kind="coordination",
                available=True,
                description="Durable notifications and approval inboxes for operator, phone, watch, and relay control flows.",
            ).to_dict(),
            CapabilityCard(
                name="peer-seek",
                kind="coordination",
                available=True,
                description="Probe candidate peers, record discovery state, and optionally auto-connect to reachable organisms.",
            ).to_dict(),
            CapabilityCard(
                name="cooperative-fanout",
                kind="executor",
                available=True,
                description="Shard one larger task into child jobs and spread them across local and remote peers.",
            ).to_dict(),
        ]
        compute_profile = dict(self.device_profile.get("compute_profile") or {})
        gpu_count = int(compute_profile.get("gpu_count") or 0)
        gpu_class = str(compute_profile.get("gpu_class") or "none")
        gpu_vram_mb = int(compute_profile.get("gpu_vram_mb") or 0)
        supports_classes = list(compute_profile.get("supports_workload_classes") or [])
        accelerators = list(compute_profile.get("accelerators") or [])
        gpu_description_bits = []
        if gpu_count > 0:
            gpu_description_bits.append(f"{gpu_count}x {gpu_class.upper()}")
            if gpu_vram_mb > 0:
                gpu_description_bits.append(f"{gpu_vram_mb} MB VRAM")
            if supports_classes:
                gpu_description_bits.append("workloads: " + ",".join(supports_classes))
        gpu_description = (
            " / ".join(gpu_description_bits)
            if gpu_description_bits
            else "No GPU accelerators reported on this device."
        )
        cards.append(
            CapabilityCard(
                name="gpu-runtime",
                kind="runtime",
                available=bool(compute_profile.get("gpu_capable")),
                description=f"GPU-aware execution and scheduling hints. {gpu_description}",
                metadata={
                    "gpu_count": gpu_count,
                    "gpu_class": gpu_class,
                    "gpu_vram_mb": gpu_vram_mb,
                    "accelerators": accelerators,
                    "supports_workload_classes": supports_classes,
                    "cpu_cores": int(compute_profile.get("cpu_cores") or 0),
                    "memory_mb": int(compute_profile.get("memory_mb") or 0),
                    "fp16_tflops": float(compute_profile.get("fp16_tflops") or 0.0),
                },
            ).to_dict()
        )
        cards.append(
            CapabilityCard(
                name="helper-enlistment",
                kind="coordination",
                available=True,
                description="Autonomous enlistment, drain, and retirement of helper peers for overflow compute.",
                metadata={
                    "helper_state": str(self.device_profile.get("helper_state") or "active"),
                    "helper_role": str(self.device_profile.get("helper_role") or ""),
                },
            ).to_dict()
        )
        cards.extend(self.golem_adapter.capability_cards())
        return cards

    def _row_to_secret(self, row, *, include_value: bool = False) -> Optional[dict]:
        if row is None:
            return None
        secret = {
            "id": row["id"],
            "scope": row["scope"] or "",
            "name": row["name"] or "",
            "metadata": _loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        value = row["value"] or ""
        secret["value_present"] = bool(value)
        secret["value_digest"] = _secret_value_digest(value) if value else ""
        if include_value:
            secret["value"] = value
        return secret

    def put_secret(
        self,
        name: str,
        value: Any,
        *,
        scope: str,
        metadata: Optional[dict] = None,
    ) -> dict:
        secret_name = str(name or "").strip()
        secret_scope = str(scope or "").strip()
        if not secret_name:
            raise MeshPolicyError("secret name is required")
        if not secret_scope:
            raise MeshPolicyError("secret scope is required")
        now = _utcnow()
        secret_id = uuid.uuid5(uuid.NAMESPACE_URL, f"ocp-secret:{secret_scope}:{secret_name}").hex
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_secrets (id, scope, name, value, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope, name) DO UPDATE SET
                    value=excluded.value,
                    metadata=excluded.metadata,
                    updated_at=excluded.updated_at
                """,
                (
                    secret_id,
                    secret_scope,
                    secret_name,
                    str(value),
                    json.dumps(dict(metadata or {})),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM mesh_secrets WHERE scope=? AND name=?",
                (secret_scope, secret_name),
            ).fetchone()
            conn.commit()
        return self._row_to_secret(row) or {}

    def get_secret(self, name: str, *, scope: str, include_value: bool = False) -> dict:
        secret_name = str(name or "").strip()
        secret_scope = str(scope or "").strip()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mesh_secrets WHERE scope=? AND name=?",
                (secret_scope, secret_name),
            ).fetchone()
        if row is None:
            raise MeshPolicyError("secret not found")
        return self._row_to_secret(row, include_value=include_value) or {}

    def list_secrets(self, *, limit: int = 25, scope: str = "") -> dict:
        params: list[Any] = []
        query = "SELECT * FROM mesh_secrets"
        if str(scope or "").strip():
            query += " WHERE scope=?"
            params.append(str(scope or "").strip())
        query += " ORDER BY updated_at DESC, created_at DESC LIMIT ?"
        params.append(max(1, int(limit or 25)))
        with self._conn() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        secrets_list = [self._row_to_secret(row) for row in rows if row is not None]
        return {"count": len(secrets_list), "secrets": secrets_list}

    def _resolve_secret_file_path(self, raw_path: Any) -> Path:
        candidate = Path(str(raw_path or "").strip())
        if not str(candidate):
            raise MeshPolicyError("file secret binding requires path")
        if not candidate.is_absolute():
            candidate = (self.workspace_root / candidate).resolve()
        else:
            candidate = candidate.resolve()
        if self.workspace_root != candidate and self.workspace_root not in candidate.parents:
            raise MeshPolicyError("file secret path must stay inside workspace_root")
        if not candidate.exists():
            raise MeshPolicyError("file secret path does not exist")
        if not candidate.is_file():
            raise MeshPolicyError("file secret path must be a file")
        return candidate

    def _update_peer_record(
        self,
        peer_id: str,
        *,
        metadata: Optional[dict] = None,
        status: Optional[str] = None,
        mesh_session_id: Optional[str] = None,
        last_seen_at: Optional[str] = None,
    ) -> dict:
        existing = self._get_peer_row(peer_id)
        if existing is None:
            raise MeshPolicyError("peer not found")
        merged_metadata = dict(_loads_json(existing["metadata"], {}))
        merged_metadata.update(dict(metadata or {}))
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_peers
                SET metadata=?,
                    status=?,
                    mesh_session_id=?,
                    updated_at=?,
                    last_seen_at=?
                WHERE peer_id=?
                """,
                (
                    json.dumps(merged_metadata),
                    status or existing["status"],
                    mesh_session_id if mesh_session_id is not None else existing["mesh_session_id"],
                    now,
                    last_seen_at or now,
                    peer_id,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_peers WHERE peer_id=?", (peer_id,)).fetchone()
        return self._row_to_peer(row)

    def remember_peer_card(
        self,
        peer_card: dict,
        *,
        trust_tier: Optional[str] = None,
        metadata: Optional[dict] = None,
        mesh_session_id: str = "",
        status: str = "connected",
    ) -> dict:
        peer_id = (peer_card.get("organism_id") or peer_card.get("node_id") or "").strip()
        if not peer_id:
            raise MeshPolicyError("peer card is missing organism_id")
        existing = self._get_peer_row(peer_id)
        existing_public_key = (existing["public_key"] or "").strip() if existing else ""
        new_public_key = (peer_card.get("public_key") or "").strip()
        if existing_public_key and existing_public_key != new_public_key:
            raise MeshSignatureError("peer public key changed; explicit trust reset required")
        now = _utcnow()
        merged_metadata = dict(_loads_json(existing["metadata"], {})) if existing else {}
        merged_metadata.update(dict(metadata or {}))
        resolved_trust = _normalize_trust_tier((existing["trust_tier"] if existing else None) or trust_tier or peer_card.get("trust_tier") or "trusted")
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_peers
                (peer_id, display_name, public_key, signature_scheme, endpoint_url, stream_url, trust_tier, reachability,
                 status, mesh_session_id, protocol_version, capability_cards, card, metadata, created_at, updated_at,
                 last_seen_at, last_handshake_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(peer_id) DO UPDATE SET
                    display_name=excluded.display_name,
                    public_key=excluded.public_key,
                    signature_scheme=excluded.signature_scheme,
                    endpoint_url=excluded.endpoint_url,
                    stream_url=excluded.stream_url,
                    trust_tier=excluded.trust_tier,
                    reachability=excluded.reachability,
                    status=excluded.status,
                    mesh_session_id=excluded.mesh_session_id,
                    protocol_version=excluded.protocol_version,
                    capability_cards=excluded.capability_cards,
                    card=excluded.card,
                    metadata=excluded.metadata,
                    updated_at=excluded.updated_at,
                    last_seen_at=excluded.last_seen_at,
                    last_handshake_at=excluded.last_handshake_at
                """,
                (
                    peer_id,
                    peer_card.get("display_name") or peer_id,
                    new_public_key,
                    peer_card.get("signature_scheme") or SIGNATURE_SCHEME,
                    peer_card.get("endpoint_url") or "",
                    peer_card.get("stream_url") or "",
                    resolved_trust,
                    peer_card.get("reachability") or "direct",
                    status,
                    mesh_session_id,
                    peer_card.get("protocol_version") or PROTOCOL_VERSION,
                    json.dumps(peer_card.get("capability_cards") or []),
                    json.dumps(peer_card),
                    json.dumps(merged_metadata),
                    existing["created_at"] if existing else now,
                    now,
                    now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_peers WHERE peer_id=?", (peer_id,)).fetchone()
        return self._row_to_peer(row)

    def get_manifest(self) -> dict:
        parsed_base = urlparse(self.base_url)
        advertised_base_url = _normalize_base_url(
            self.base_url,
            fallback_url=_preferred_local_base_url(
                bind_host=parsed_base.hostname or "",
                port=int(parsed_base.port or 8421),
            ),
        )
        card = OrganismCard(
            organism_id=self.node_id,
            node_id=self.node_id,
            display_name=self.display_name,
            public_key=self.public_key,
            signature_scheme=SIGNATURE_SCHEME,
            endpoint_url=advertised_base_url,
            stream_url=f"{advertised_base_url}/mesh/stream",
            protocol_version=PROTOCOL_VERSION,
            trust_tier="self",
            reachability="local-first",
            supported_features=[
                "handshake",
                "agent-presence",
                "beacons",
                "leases",
                "jobs",
                "workers",
                "job-attempts",
                "artifacts",
                "handoffs",
                "notifications",
                "approvals",
                "secrets",
                "peer-discovery",
                "cooperative-tasks",
            ],
            transports=[
                {"name": "http", "mode": "request-response"},
                {"name": "ws", "mode": "bootstrap-stream"},
            ],
            capability_cards=self.capability_cards(),
            policy_summary={
                "private_requires_trusted_peer": True,
                "trusted_requires_trusted_peer": True,
                "public_allows_market_peers": True,
                "secret_scopes_default": [],
            },
            device_profile=dict(self.device_profile),
        )
        workers = self.list_workers(limit=20)["workers"]
        return {
            "protocol": PROTOCOL_NAME,
            "protocol_short_name": PROTOCOL_SHORT_NAME,
            "protocol_release": PROTOCOL_RELEASE,
            "spec_status": "draft",
            "protocol_version": PROTOCOL_VERSION,
            "implementation": {
                "name": IMPLEMENTATION_NAME,
                "wire_protocol_version": PROTOCOL_VERSION,
            },
            "device_profile": dict(self.device_profile),
            "sync_policy": self._device_profile_sync_policy(self.device_profile),
            "organism_card": card.to_dict(),
            "reliability": self._local_reliability_summary(),
            "queue_metrics": self.queue_metrics(),
            "agent_presence": self.export_agent_presence(limit=50),
            "beacons": self.export_beacons(limit=12),
            "workers": workers,
        }

    def export_agent_presence(self, *, limit: int = 25) -> list[dict]:
        if not hasattr(self.lattice, "list_agent_registrations"):
            return []
        agents = self.lattice.list_agent_registrations(limit=limit, include_sessions=True)
        results = []
        for agent in agents:
            capability_cards = [
                CapabilityCard(
                    name=str(cap),
                    kind="agent-capability",
                    available=True,
                ).to_dict()
                for cap in (agent.get("capabilities") or [])
            ]
            active_session = dict(agent.get("active_session") or {})
            presence = AgentPresence(
                organism_id=self.node_id,
                peer_id=self.node_id,
                agent_id=agent.get("agent_id") or "",
                agent_name=agent.get("agent_name") or agent.get("agent_id") or "",
                agent_type=agent.get("agent_type") or "ai",
                runtime=agent.get("runtime") or "",
                role=agent.get("role") or "",
                scope=agent.get("scope") or "",
                interface=agent.get("interface") or "",
                status="active" if active_session else (agent.get("status") or "joined"),
                mesh_session_id=active_session.get("id") or f"mesh:{agent.get('agent_id')}",
                capabilities=list(agent.get("capabilities") or []),
                capability_cards=capability_cards,
                active_session=active_session,
            )
            results.append(presence.to_dict())
        return results

    def export_beacons(self, *, limit: int = 10) -> list[dict]:
        if self.registry is None or not hasattr(self.registry, "get_beacons"):
            return []
        try:
            return list(self.registry.get_beacons(limit=limit))
        except Exception:
            logger.debug("mesh beacon export failed", exc_info=True)
            return []

    def accept_handshake(self, envelope: dict) -> dict:
        body = dict(envelope.get("body") or {})
        peer_card = dict(body.get("peer_card") or {})
        if not peer_card:
            raise MeshSignatureError("peer_card is required for handshake")

        peer_id, request_meta, _, _ = self._verify_envelope(
            envelope,
            route="/mesh/handshake",
            peer_card=peer_card,
        )
        if peer_id != (peer_card.get("organism_id") or peer_card.get("node_id") or "").strip():
            raise MeshSignatureError("request node_id does not match peer card")

        trust_tier = _normalize_trust_tier(peer_card.get("trust_tier") or "trusted")
        if trust_tier == "blocked":
            raise MeshPolicyError(f"peer {peer_id} is blocked")
        mesh_session_id = f"mesh-{uuid.uuid4().hex[:12]}"
        row = self.remember_peer_card(
            peer_card,
            trust_tier=trust_tier,
            mesh_session_id=mesh_session_id,
            status="connected",
            metadata={
                "remote_agent_presence": list(body.get("agent_presence") or []),
                "remote_beacons": list(body.get("beacons") or []),
                "remote_device_profile": _normalize_device_profile(peer_card.get("device_profile") or {}),
                "last_request_id": request_meta.get("request_id"),
            },
        )
        self._record_event(
            "mesh.handshake.accepted",
            peer_id=peer_id,
            request_id=request_meta.get("request_id") or "",
            payload={"display_name": peer_card.get("display_name") or peer_id, "mesh_session_id": mesh_session_id},
        )
        return {
            "status": "ok",
            "mesh_session_id": mesh_session_id,
            "peer": row,
            "manifest": self.get_manifest(),
            "stream": self.stream_snapshot(limit=25),
            "accepted_at": _utcnow(),
        }

    def _resolve_peer_client(
        self,
        peer_id: str = "",
        *,
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
    ) -> tuple[MeshPeerClient, dict]:
        if client is not None:
            if peer_id:
                peer = self._row_to_peer(self._get_peer_row(peer_id))
                return client, peer or {}
            return client, {}
        peer = self._row_to_peer(self._get_peer_row(peer_id)) if peer_id else {}
        endpoint_url = (base_url or (peer or {}).get("endpoint_url") or "").strip()
        if not endpoint_url:
            raise MeshPolicyError("peer endpoint_url is unavailable")
        return MeshPeerClient(endpoint_url), peer or {}

    def connect_peer(
        self,
        *,
        base_url: str,
        trust_tier: str = "trusted",
        timeout: float = 8.0,
    ) -> dict:
        normalized_base_url = _normalize_base_url(base_url)
        client = MeshPeerClient(normalized_base_url, timeout=timeout)
        remote_manifest = client.manifest()
        remote_card = dict(remote_manifest.get("organism_card") or {})
        remote_endpoint_url = _normalize_base_url(
            remote_card.get("endpoint_url") or "",
            fallback_url=normalized_base_url,
            replace_loopback=True,
        )
        envelope = self.build_signed_envelope(
            "/mesh/handshake",
            {
                "peer_card": {**self.get_manifest()["organism_card"], "trust_tier": _normalize_trust_tier(trust_tier)},
                "agent_presence": self.export_agent_presence(limit=20),
                "beacons": self.export_beacons(limit=10),
            },
        )
        response = client.handshake(envelope)
        self.remember_peer_card(
            {**remote_card, "endpoint_url": remote_endpoint_url, "stream_url": f"{remote_endpoint_url}/mesh/stream"},
            trust_tier=trust_tier,
            mesh_session_id=response.get("mesh_session_id") or "",
            status="connected",
            metadata={
                "remote_agent_presence": list(remote_manifest.get("agent_presence") or []),
                "remote_beacons": list(remote_manifest.get("beacons") or []),
                "remote_workers": list(remote_manifest.get("workers") or []),
                "remote_queue_metrics": dict(remote_manifest.get("queue_metrics") or {}),
                "remote_device_profile": _normalize_device_profile(
                    remote_manifest.get("device_profile")
                    or remote_card.get("device_profile")
                    or {}
                ),
                "last_remote_handshake": response.get("accepted_at"),
            },
        )
        self._record_event(
            "mesh.handshake.sent",
            peer_id=remote_card.get("organism_id") or remote_card.get("node_id") or "",
            request_id=envelope["request"]["request_id"],
            payload={"base_url": normalized_base_url, "trust_tier": _normalize_trust_tier(trust_tier)},
        )
        return {"status": "ok", "remote_manifest": remote_manifest, "response": response}

    def _discovery_candidate_by_peer_id(self, peer_id: str) -> Optional[dict]:
        token = str(peer_id or "").strip()
        if not token:
            return None
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM mesh_discovery_candidates
                WHERE peer_id=?
                ORDER BY last_seen_at DESC, updated_at DESC
                LIMIT 1
                """,
                (token,),
            ).fetchone()
        return self._row_to_discovery_candidate(row)

    def _scan_urls_for_address(self, address: str, *, port: int, limit: int) -> list[str]:
        ip = ipaddress.ip_address(str(address or "").strip())
        if ip.version != 4 or ip.is_loopback or ip.is_unspecified:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for prefix in (28, 24):
            network = ipaddress.ip_network(f"{ip}/{prefix}", strict=False)
            ordered_hosts = sorted(
                (host for host in network.hosts() if host != ip),
                key=lambda host: (abs(int(host) - int(ip)), int(host)),
            )
            for host in ordered_hosts:
                url = f"http://{host}:{int(port)}"
                if url not in seen:
                    seen.add(url)
                    urls.append(url)
                if len(urls) >= max(1, int(limit)):
                    return urls
        return urls

    def suggest_local_scan_urls(self, *, port: int = 0, limit: int = 24) -> list[str]:
        parsed_base = urlparse(self.base_url)
        resolved_port = int(port or parsed_base.port or 8421)
        urls: list[str] = []
        seen: set[str] = set()

        def append(raw_value: str) -> None:
            token = _normalize_base_url(raw_value).rstrip("/")
            if not token or token in seen or token == self.base_url.rstrip("/"):
                return
            seen.add(token)
            urls.append(token)

        peer_rows = list(self.list_peers(limit=12).get("peers") or [])
        for peer in peer_rows:
            append(peer.get("endpoint_url") or "")
        candidate_rows = list(self.list_discovery_candidates(limit=12).get("candidates") or [])
        for candidate in candidate_rows:
            append(candidate.get("endpoint_url") or candidate.get("base_url") or "")
        bind_host = parsed_base.hostname or ""
        for address in _discover_local_ipv4_addresses(bind_host=bind_host):
            for url in self._scan_urls_for_address(address, port=resolved_port, limit=limit):
                append(url)
                if len(urls) >= max(1, int(limit)):
                    return urls
        return urls

    def connectivity_diagnostics(self, *, port: int = 0, limit: int = 24) -> dict:
        parsed_base = urlparse(self.base_url)
        bind_host = parsed_base.hostname or ""
        resolved_port = int(port or parsed_base.port or 8421)
        local_addresses = _discover_local_ipv4_addresses(bind_host=bind_host)
        candidate_rows = list(self.list_discovery_candidates(limit=8).get("candidates") or [])
        recent_errors = [
            {
                "base_url": candidate.get("base_url") or "",
                "display_name": candidate.get("display_name") or candidate.get("peer_id") or "",
                "error": candidate.get("last_error") or "",
                "last_error_at": candidate.get("last_error_at") or candidate.get("updated_at") or "",
            }
            for candidate in candidate_rows
            if str(candidate.get("last_error") or "").strip()
        ][:4]
        return {
            "status": "ok",
            "base_url": _normalize_base_url(
                self.base_url,
                fallback_url=_preferred_local_base_url(bind_host=bind_host, port=resolved_port),
            ),
            "bind_host": bind_host or "127.0.0.1",
            "port": resolved_port,
            "local_ipv4": local_addresses,
            "scan_urls": self.suggest_local_scan_urls(port=resolved_port, limit=limit),
            "recent_errors": recent_errors,
        }

    def scan_local_peers(
        self,
        *,
        trust_tier: str = "trusted",
        timeout: float = 0.8,
        limit: int = 24,
        port: int = 0,
    ) -> dict:
        resolved_port = int(port or urlparse(self.base_url).port or 8421)
        suggested_urls = self.suggest_local_scan_urls(port=resolved_port, limit=limit)
        result = self.seek_peers(
            base_urls=suggested_urls,
            port=resolved_port,
            trust_tier=trust_tier,
            auto_connect=False,
            include_self=False,
            limit=limit,
            timeout=timeout,
            refresh_known=True,
        )
        result["suggested_urls"] = suggested_urls
        result["diagnostics"] = self.connectivity_diagnostics(port=resolved_port, limit=limit)
        return result

    def connect_device(
        self,
        *,
        base_url: str = "",
        peer_id: str = "",
        trust_tier: str = "trusted",
        timeout: float = 3.0,
        refresh_manifest: bool = True,
    ) -> dict:
        peer_token = str(peer_id or "").strip()
        base_token = _normalize_base_url(base_url)
        peer = self._row_to_peer(self._get_peer_row(peer_token)) if peer_token else {}
        if not base_token and peer:
            base_token = _normalize_base_url(peer.get("endpoint_url") or "")
        if not base_token and peer_token:
            candidate = self._discovery_candidate_by_peer_id(peer_token)
            if candidate:
                base_token = _normalize_base_url(candidate.get("endpoint_url") or candidate.get("base_url") or "")
        if not base_token:
            raise MeshPolicyError("peer base_url is required")
        connection = self.connect_peer(base_url=base_token, trust_tier=trust_tier, timeout=timeout)
        remote_manifest = dict(connection.get("remote_manifest") or {})
        remote_card = dict(remote_manifest.get("organism_card") or {})
        connected_peer_id = str(remote_card.get("organism_id") or remote_card.get("node_id") or peer_token).strip()
        sync_result = self.sync_peer(connected_peer_id, base_url=base_token, limit=20, refresh_manifest=refresh_manifest)
        resolved_peer = self._row_to_peer(self._get_peer_row(connected_peer_id)) or dict(sync_result.get("peer") or {})
        return {
            "status": "ok",
            "peer": resolved_peer,
            "peer_id": connected_peer_id,
            "base_url": base_token,
            "connection": connection,
            "sync": sync_result,
        }

    def connect_all_devices(
        self,
        *,
        trust_tier: str = "trusted",
        timeout: float = 3.0,
        scan_timeout: float = 0.8,
        limit: int = 24,
        port: int = 0,
        refresh_manifest: bool = True,
    ) -> dict:
        scan_result = self.scan_local_peers(
            trust_tier=trust_tier,
            timeout=scan_timeout,
            limit=limit,
            port=port,
        )
        peer_rows = list((self.list_peers(limit=max(limit * 2, 24)) or {}).get("peers") or [])
        candidate_rows = list((self.list_discovery_candidates(limit=max(limit * 2, 24)) or {}).get("candidates") or [])

        results: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for peer in peer_rows:
            peer_id = str(peer.get("peer_id") or "").strip()
            endpoint_url = _normalize_base_url(peer.get("endpoint_url") or "")
            if not peer_id and not endpoint_url:
                continue
            key = f"peer:{peer_id or endpoint_url}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            if peer_id == self.node_id:
                continue
            results.append(
                {
                    "status": "already_connected",
                    "peer_id": peer_id,
                    "base_url": endpoint_url,
                    "peer": dict(peer),
                }
            )

        for candidate in candidate_rows:
            peer_id = str(candidate.get("peer_id") or "").strip()
            endpoint_url = _normalize_base_url(candidate.get("endpoint_url") or candidate.get("base_url") or "")
            if not peer_id and not endpoint_url:
                continue
            key = f"candidate:{peer_id or endpoint_url}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            if peer_id == self.node_id:
                continue
            try:
                connected = self.connect_device(
                    base_url=endpoint_url,
                    peer_id=peer_id,
                    trust_tier=trust_tier,
                    timeout=timeout,
                    refresh_manifest=refresh_manifest,
                )
                results.append(
                    {
                        "status": "connected",
                        "peer_id": str(connected.get("peer_id") or peer_id).strip(),
                        "base_url": endpoint_url,
                        "peer": dict(connected.get("peer") or {}),
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "status": "error",
                        "peer_id": peer_id,
                        "base_url": endpoint_url,
                        "error": str(exc),
                    }
                )

        connected_count = sum(1 for item in results if item.get("status") == "connected")
        ready_count = sum(1 for item in results if item.get("status") == "already_connected")
        error_count = sum(1 for item in results if item.get("status") == "error")
        mesh_peer_snapshot = self.list_peers(limit=max(limit * 2, 24))
        mesh_peer_ids = [
            str(peer.get("peer_id") or "").strip()
            for peer in list(mesh_peer_snapshot.get("peers") or [])
            if str(peer.get("peer_id") or "").strip()
        ]
        return {
            "status": "ok",
            "scan": scan_result,
            "results": results,
            "connected": connected_count,
            "already_connected": ready_count,
            "errors": error_count,
            "count": len(results),
            "mesh": {
                "peer_count": int(mesh_peer_snapshot.get("count") or len(mesh_peer_ids)),
                "peer_ids": mesh_peer_ids,
            },
        }

    def launch_test_mission(
        self,
        *,
        peer_id: str = "",
        base_url: str = "",
        trust_tier: str = "trusted",
        timeout: float = 3.0,
        request_id: Optional[str] = None,
    ) -> dict:
        peer_token = str(peer_id or "").strip()
        connection: Optional[dict] = None
        if not peer_token:
            connection = self.connect_device(
                base_url=base_url,
                trust_tier=trust_tier,
                timeout=timeout,
                refresh_manifest=True,
            )
            peer_token = str(connection.get("peer_id") or "").strip()
        elif self._get_peer_row(peer_token) is None and str(base_url or "").strip():
            connection = self.connect_device(
                peer_id=peer_token,
                base_url=base_url,
                trust_tier=trust_tier,
                timeout=timeout,
                refresh_manifest=True,
            )
            peer_token = str(connection.get("peer_id") or peer_token).strip()
        if not peer_token:
            raise MeshPolicyError("test mission target peer is required")

        proof_filename = "ocp_connect_proof.txt"
        proof_code = (
            "from pathlib import Path\n"
            "import tempfile\n"
            "path = Path(tempfile.gettempdir()) / 'ocp_connect_proof.txt'\n"
            "path.write_text('mission ran on remote helper\\n')\n"
            "print(str(path))\n"
            "print(path.read_text().strip())\n"
        )
        mission = self.launch_mission(
            title="Mesh Test Mission",
            intent="Verify peer connectivity and remote execution from the Connect Devices control surface.",
            request_id=(request_id or f"mesh-test-mission-{uuid.uuid4().hex[:12]}").strip(),
            priority="high",
            workload_class="connectivity_test",
            target_strategy="cooperative_spread",
            continuity={"resumable": True},
            metadata={"control_flow": "connect_devices", "test_mission": True, "proof_filename": proof_filename},
            cooperative_task={
                "name": "mesh-test-remote-proof",
                "strategy": "spread",
                "allow_local": False,
                "allow_remote": True,
                "target_peer_ids": [peer_token],
                "base_job": {
                    "kind": "python.inline",
                    "dispatch_mode": "inline",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"code": proof_code},
                    "metadata": {"workload_class": "connectivity_test"},
                },
                "shards": [{"label": "remote-proof", "payload": {"code": proof_code}}],
            },
        )
        return {
            "status": "ok",
            "peer_id": peer_token,
            "base_url": _normalize_base_url(base_url) or str(((connection or {}).get("base_url")) or ""),
            "proof": {"filename": proof_filename, "location_hint": "system temp directory"},
            "connection": connection or {},
            "mission": mission,
        }

    def list_remote_events(self, peer_id: str, *, since_remote_seq: int = 0, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_remote_events
                WHERE peer_id=? AND remote_seq > ?
                ORDER BY remote_seq ASC
                LIMIT ?
                """,
                ((peer_id or "").strip(), max(0, int(since_remote_seq)), max(1, int(limit))),
            ).fetchall()
        return [
            {
                "peer_id": row["peer_id"],
                "remote_seq": int(row["remote_seq"]),
                "event_id": row["event_id"] or "",
                "event_type": row["event_type"],
                "request_id": row["request_id"] or "",
                "payload": _loads_json(row["payload"], {}),
                "remote_created_at": row["remote_created_at"] or "",
                "synced_at": row["synced_at"],
            }
            for row in rows
        ]

    def _import_remote_events(self, peer_id: str, remote_events: list[dict]) -> int:
        imported = 0
        with self._conn() as conn:
            for event in remote_events:
                try:
                    conn.execute(
                        """
                        INSERT INTO mesh_remote_events
                        (peer_id, remote_seq, event_id, event_type, request_id, payload, remote_created_at, synced_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            peer_id,
                            int(event.get("seq") or 0),
                            event.get("id") or "",
                            event.get("event_type") or "unknown",
                            event.get("request_id") or "",
                            json.dumps(event.get("payload") or {}),
                            event.get("created_at") or "",
                            _utcnow(),
                        ),
                    )
                    imported += 1
                except sqlite3.IntegrityError:
                    continue
            conn.commit()
        return imported

    def _peer_heartbeat_snapshot(self, remote_stream: dict, *, device_profile: Optional[dict] = None) -> dict:
        presence = list(remote_stream.get("agent_presence") or [])
        beacons = list(remote_stream.get("beacons") or [])
        active_agents = sum(1 for item in presence if (item.get("status") or "").strip().lower() == "active")
        sync_policy = self._device_profile_sync_policy(device_profile or {})
        status = "active" if active_agents else "idle"
        if not presence and not beacons:
            status = "intermittent_idle" if sync_policy["mode"] == "intermittent" else "quiet"
        return {
            "status": status,
            "active_agents": active_agents,
            "agent_count": len(presence),
            "beacon_count": len(beacons),
            "sync_mode": sync_policy["mode"],
            "sleep_capable": sync_policy["sleep_capable"],
            "preferred_sync_interval_seconds": sync_policy["preferred_sync_interval_seconds"],
            "remote_generated_at": remote_stream.get("generated_at") or "",
            "checked_at": _utcnow(),
        }

    def sync_peer(
        self,
        peer_id: str,
        *,
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
        limit: int = 100,
        refresh_manifest: bool = False,
    ) -> dict:
        remote_client, peer = self._resolve_peer_client(peer_id, client=client, base_url=base_url)
        if not peer:
            raise MeshPolicyError("peer must be connected before sync")
        remote_manifest = None
        if refresh_manifest or not peer.get("card"):
            remote_manifest = remote_client.manifest()
            remote_card = dict(remote_manifest.get("organism_card") or {})
            remote_device_profile = _normalize_device_profile(
                remote_manifest.get("device_profile")
                or remote_card.get("device_profile")
                or {}
            )
            normalized_endpoint_url = _normalize_base_url(
                remote_card.get("endpoint_url") or "",
                fallback_url=_normalize_base_url(base_url or peer.get("endpoint_url") or ""),
                replace_loopback=True,
            )
            self.remember_peer_card(
                {**remote_card, "endpoint_url": normalized_endpoint_url, "stream_url": f"{normalized_endpoint_url}/mesh/stream"},
                trust_tier=peer.get("trust_tier"),
                mesh_session_id=peer.get("mesh_session_id") or "",
                status="connected",
                metadata={
                    "remote_agent_presence": list(remote_manifest.get("agent_presence") or []),
                    "remote_beacons": list(remote_manifest.get("beacons") or []),
                    "remote_workers": list(remote_manifest.get("workers") or []),
                    "remote_queue_metrics": dict(remote_manifest.get("queue_metrics") or {}),
                    "remote_device_profile": remote_device_profile,
                    "remote_sync_policy": dict(
                        remote_manifest.get("sync_policy")
                        or self._device_profile_sync_policy(remote_device_profile)
                    ),
                    "last_manifest_refresh_at": _utcnow(),
                },
            )
            peer = self._row_to_peer(self._get_peer_row(peer_id)) or peer
        remote_cursor = int(((peer.get("metadata") or {}).get("remote_cursor") or 0))
        remote_stream = remote_client.stream_snapshot(since=remote_cursor, limit=max(1, int(limit)))
        imported = self._import_remote_events(peer_id, list(remote_stream.get("events") or []))
        remote_device_profile = _normalize_device_profile(
            remote_stream.get("device_profile")
            or ((remote_manifest or {}).get("device_profile") if isinstance(remote_manifest, dict) else {})
            or ((peer.get("card") or {}).get("device_profile") if isinstance(peer.get("card"), dict) else {})
            or {}
        )
        heartbeat = self._peer_heartbeat_snapshot(remote_stream, device_profile=remote_device_profile)
        next_cursor = int(remote_stream.get("next_cursor") or remote_cursor)
        updated_peer = self._update_peer_record(
            peer_id,
            metadata={
                "remote_cursor": next_cursor,
                "last_sync_at": _utcnow(),
                "remote_agent_presence": list(remote_stream.get("agent_presence") or []),
                "remote_beacons": list(remote_stream.get("beacons") or []),
                "remote_workers": list(remote_stream.get("workers") or []),
                "remote_queue_metrics": dict(
                    remote_stream.get("queue_metrics")
                    or ((remote_manifest or {}).get("queue_metrics") if isinstance(remote_manifest, dict) else {})
                    or {}
                ),
                "remote_device_profile": remote_device_profile,
                "remote_sync_policy": dict(
                    remote_stream.get("sync_policy")
                    or ((remote_manifest or {}).get("sync_policy") if isinstance(remote_manifest, dict) else {})
                    or self._device_profile_sync_policy(remote_device_profile)
                ),
                "remote_generated_at": remote_stream.get("generated_at") or "",
                "remote_transport": dict(remote_stream.get("transport") or {}),
                "last_imported_event_count": imported,
                "heartbeat": heartbeat,
            },
            status="connected",
        )
        self._record_event(
            "mesh.peer.synced",
            peer_id=peer_id,
            payload={"imported_events": imported, "next_cursor": next_cursor},
        )
        self._record_event(
            "mesh.peer.heartbeat",
            peer_id=peer_id,
            payload=heartbeat,
        )
        return {
            "status": "ok",
            "peer": updated_peer,
            "imported_events": imported,
            "next_cursor": next_cursor,
            "heartbeat": heartbeat,
            "remote_stream": remote_stream,
            "remote_manifest": remote_manifest,
        }

    def sync_all_peers(
        self,
        *,
        limit: int = 100,
        refresh_manifest: bool = False,
    ) -> dict:
        peers = self.list_peers(limit=500).get("peers", [])
        results = []
        failures = []
        for peer in peers:
            try:
                results.append(
                    self.sync_peer(
                        peer["peer_id"],
                        limit=limit,
                        refresh_manifest=refresh_manifest,
                    )
                )
            except Exception as exc:
                failures.append({"peer_id": peer["peer_id"], "error": str(exc)})
                try:
                    self._update_peer_record(
                        peer["peer_id"],
                        metadata={"last_sync_error": str(exc), "last_sync_at": _utcnow()},
                        status="degraded",
                    )
                except Exception:
                    logger.debug("mesh peer degradation update failed", exc_info=True)
        return {"status": "ok", "results": results, "failures": failures}

    def list_peers(self, *, limit: int = 25) -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_peers
                ORDER BY last_seen_at DESC, updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        peers = [self._row_to_peer(row) for row in rows]
        trust_counts = {}
        device_counts = {}
        synced_recently = 0
        degraded = 0
        intermittent = 0
        for peer in peers:
            trust_counts[peer["trust_tier"]] = trust_counts.get(peer["trust_tier"], 0) + 1
            device_class = ((peer.get("device_profile") or {}).get("device_class") or "full").strip().lower()
            device_counts[device_class] = device_counts.get(device_class, 0) + 1
            if (peer.get("sync_policy") or {}).get("mode") == "intermittent":
                intermittent += 1
            if peer.get("sync_state", {}).get("last_sync_at"):
                synced_recently += 1
            if peer.get("status") == "degraded":
                degraded += 1
        return {
            "organism_id": self.node_id,
            "count": len(peers),
            "peers": peers,
            "health": {
                "connected": sum(1 for peer in peers if peer.get("status") == "connected"),
                "synced_recently": synced_recently,
                "degraded": degraded,
                "intermittent": intermittent,
                "trust_tiers": trust_counts,
                "device_classes": device_counts,
            },
        }

    def _row_to_discovery_candidate(self, row) -> Optional[dict]:
        if row is None:
            return None
        return {
            "base_url": row["base_url"] or "",
            "peer_id": row["peer_id"] or "",
            "display_name": row["display_name"] or "",
            "endpoint_url": row["endpoint_url"] or "",
            "status": row["status"] or "discovered",
            "trust_tier": _normalize_trust_tier(row["trust_tier"]),
            "device_profile": _normalize_device_profile(_loads_json(row["device_profile"], {})),
            "manifest": _loads_json(row["manifest"], {}),
            "metadata": _loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_seen_at": row["last_seen_at"] or "",
            "last_error": row["last_error"] or "",
            "last_error_at": row["last_error_at"] or "",
        }

    def _remember_discovery_candidate(
        self,
        *,
        base_url: str,
        peer_id: str = "",
        display_name: str = "",
        endpoint_url: str = "",
        status: str = "discovered",
        trust_tier: str = "trusted",
        device_profile: Optional[dict] = None,
        manifest: Optional[dict] = None,
        metadata: Optional[dict] = None,
        last_error: str = "",
    ) -> dict:
        base_token = str(base_url or "").rstrip("/")
        if not base_token:
            raise MeshPolicyError("discovery candidate base_url is required")
        now = _utcnow()
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT * FROM mesh_discovery_candidates WHERE base_url=?",
                (base_token,),
            ).fetchone()
            existing_metadata = _loads_json(existing["metadata"], {}) if existing is not None else {}
            merged_metadata = {**existing_metadata, **dict(metadata or {})}
            conn.execute(
                """
                INSERT INTO mesh_discovery_candidates
                (base_url, peer_id, display_name, endpoint_url, status, trust_tier, device_profile, manifest, metadata,
                 created_at, updated_at, last_seen_at, last_error, last_error_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(base_url) DO UPDATE SET
                    peer_id=excluded.peer_id,
                    display_name=excluded.display_name,
                    endpoint_url=excluded.endpoint_url,
                    status=excluded.status,
                    trust_tier=excluded.trust_tier,
                    device_profile=excluded.device_profile,
                    manifest=excluded.manifest,
                    metadata=excluded.metadata,
                    updated_at=excluded.updated_at,
                    last_seen_at=excluded.last_seen_at,
                    last_error=excluded.last_error,
                    last_error_at=excluded.last_error_at
                """,
                (
                    base_token,
                    str(peer_id or "").strip(),
                    str(display_name or "").strip(),
                    str(endpoint_url or "").strip() or base_token,
                    str(status or "discovered").strip().lower() or "discovered",
                    _normalize_trust_tier(trust_tier),
                    json.dumps(_normalize_device_profile(device_profile or {})),
                    json.dumps(dict(manifest or {})),
                    json.dumps(merged_metadata),
                    existing["created_at"] if existing is not None else now,
                    now,
                    now,
                    str(last_error or ""),
                    now if str(last_error or "").strip() else (existing["last_error_at"] if existing is not None else ""),
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM mesh_discovery_candidates WHERE base_url=?",
                (base_token,),
            ).fetchone()
        return self._row_to_discovery_candidate(row) or {}

    def list_discovery_candidates(self, *, limit: int = 25, status: str = "") -> dict:
        query = ["SELECT * FROM mesh_discovery_candidates"]
        params: list[Any] = []
        status_token = str(status or "").strip().lower()
        if status_token:
            query.append("WHERE status=?")
            params.append(status_token)
        query.append("ORDER BY last_seen_at DESC, updated_at DESC LIMIT ?")
        params.append(max(1, int(limit or 25)))
        with self._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(params)).fetchall()
        candidates = [self._row_to_discovery_candidate(row) for row in rows]
        return {"peer_id": self.node_id, "count": len(candidates), "candidates": candidates}

    def _seek_candidate_urls(
        self,
        *,
        base_urls: Optional[list[str]] = None,
        hosts: Optional[list[str]] = None,
        cidr: str = "",
        port: int = 8421,
        include_self: bool = False,
        limit: int = 32,
    ) -> list[str]:
        seen: list[str] = []
        max_count = max(1, int(limit or 32))

        def append_url(raw_value: str) -> None:
            token = str(raw_value or "").strip()
            if not token:
                return
            if "://" not in token:
                token = f"http://{token}"
            token = token.rstrip("/")
            if not include_self and token == self.base_url.rstrip("/"):
                return
            if token not in seen and len(seen) < max_count:
                seen.append(token)

        for item in (base_urls or []):
            append_url(str(item or ""))
        for item in (hosts or []):
            host_token = str(item or "").strip()
            if not host_token:
                continue
            if "://" in host_token:
                append_url(host_token)
            else:
                append_url(f"http://{host_token}:{int(port or 8421)}")
        cidr_token = str(cidr or "").strip()
        if cidr_token and len(seen) < max_count:
            network = ipaddress.ip_network(cidr_token, strict=False)
            for host in network.hosts():
                append_url(f"http://{host}:{int(port or 8421)}")
                if len(seen) >= max_count:
                    break
        return seen

    def seek_peers(
        self,
        *,
        base_urls: Optional[list[str]] = None,
        hosts: Optional[list[str]] = None,
        cidr: str = "",
        port: int = 8421,
        trust_tier: str = "trusted",
        auto_connect: bool = False,
        include_self: bool = False,
        limit: int = 32,
        timeout: float = 2.0,
        refresh_known: bool = True,
    ) -> dict:
        urls = self._seek_candidate_urls(
            base_urls=base_urls,
            hosts=hosts,
            cidr=cidr,
            port=port,
            include_self=include_self,
            limit=limit,
        )
        results = []
        connected = 0
        discovered = 0
        errors = 0
        for base_url in urls:
            try:
                client = MeshPeerClient(base_url, timeout=float(timeout or 2.0))
                manifest = client.manifest()
                remote_card = dict(manifest.get("organism_card") or {})
                peer_id = str(remote_card.get("organism_id") or remote_card.get("node_id") or "").strip()
                endpoint_url = _normalize_base_url(
                    remote_card.get("endpoint_url") or "",
                    fallback_url=base_url,
                    replace_loopback=True,
                )
                candidate = self._remember_discovery_candidate(
                    base_url=base_url,
                    peer_id=peer_id,
                    display_name=remote_card.get("display_name") or peer_id,
                    endpoint_url=endpoint_url,
                    status="discovered",
                    trust_tier=trust_tier,
                    device_profile=manifest.get("device_profile") or remote_card.get("device_profile") or {},
                    manifest=manifest,
                    metadata={
                        "supported_features": list((remote_card.get("supported_features") or [])),
                        "candidate_kind": "seek",
                    },
                )
                result: dict[str, Any] = {
                    "base_url": base_url,
                    "peer_id": peer_id,
                    "display_name": candidate.get("display_name") or peer_id,
                    "status": "discovered",
                    "candidate": candidate,
                }
                discovered += 1
                if peer_id == self.node_id:
                    result["status"] = "self"
                elif auto_connect:
                    if self._get_peer_row(peer_id) is None:
                        connection = self.connect_peer(base_url=base_url, trust_tier=trust_tier, timeout=float(timeout or 2.0))
                    elif refresh_known:
                        connection = self.sync_peer(peer_id, client=client, refresh_manifest=True)
                    else:
                        connection = {"status": "known"}
                    connected += 1 if result["status"] != "self" else 0
                    result["status"] = "connected"
                    result["connection"] = connection
                    result["candidate"] = self._remember_discovery_candidate(
                        base_url=base_url,
                        peer_id=peer_id,
                        display_name=remote_card.get("display_name") or peer_id,
                        endpoint_url=endpoint_url,
                        status="connected",
                        trust_tier=trust_tier,
                        device_profile=manifest.get("device_profile") or remote_card.get("device_profile") or {},
                        manifest=manifest,
                        metadata={"auto_connect": True},
                    )
                results.append(result)
            except Exception as exc:
                errors += 1
                candidate = self._remember_discovery_candidate(
                    base_url=base_url,
                    status="error",
                    trust_tier=trust_tier,
                    metadata={"candidate_kind": "seek"},
                    last_error=str(exc),
                )
                results.append({"base_url": base_url, "status": "error", "error": str(exc), "candidate": candidate})
        self._record_event(
            "mesh.discovery.seek",
            peer_id=self.node_id,
            payload={
                "candidate_count": len(urls),
                "discovered": discovered,
                "connected": connected,
                "errors": errors,
                "auto_connect": bool(auto_connect),
            },
        )
        return {
            "peer_id": self.node_id,
            "count": len(results),
            "discovered": discovered,
            "connected": connected,
            "errors": errors,
            "results": results,
        }

    def list_execution_targets(
        self,
        job: dict,
        *,
        preferred_peer_id: str = "",
        allow_local: bool = True,
        allow_remote: bool = True,
    ) -> dict:
        normalized_job = dict(job or {})
        targets = []
        if allow_local:
            score, reasons = self._local_candidate_score(normalized_job)
            targets.append(
                {
                    "target_type": "local",
                    "peer_id": self.node_id,
                    "score": score,
                    "eligible": score > -10000,
                    "reasons": reasons,
                    "device_profile": dict(self.device_profile),
                    "queue_metrics": self.queue_metrics(),
                }
            )
        if allow_remote:
            for peer in self.list_peers(limit=500).get("peers", []):
                score, reasons = self._peer_candidate_score(peer, normalized_job)
                if preferred_peer_id and peer["peer_id"] == preferred_peer_id:
                    score += 500
                    reasons = list(reasons) + ["preferred_peer"]
                targets.append(
                    {
                        "target_type": "peer",
                        "peer_id": peer["peer_id"],
                        "score": score,
                        "eligible": score > -10000,
                        "reasons": reasons,
                        "device_profile": dict(peer.get("device_profile") or {}),
                        "queue_metrics": dict((peer.get("metadata") or {}).get("remote_queue_metrics") or {}),
                    }
                )
        targets.sort(key=lambda item: (item["score"], item["target_type"] == "local"), reverse=True)
        return {
            "status": "ok",
            "count": len(targets),
            "targets": targets,
            "eligible": [target for target in targets if target.get("eligible")],
        }

    def _merge_cooperative_child_job(self, base_job: dict, shard: dict, *, group_id: str, shard_index: int, shard_count: int) -> dict:
        merged = dict(base_job or {})
        shard_spec = dict(shard or {})
        merged_payload = dict(base_job.get("payload") or {})
        merged_payload.update(dict(shard_spec.get("payload") or {}))
        merged["payload"] = merged_payload
        merged_requirements = dict(base_job.get("requirements") or {})
        merged_requirements.update(dict(shard_spec.get("requirements") or {}))
        merged["requirements"] = merged_requirements
        merged_policy = dict(base_job.get("policy") or {})
        merged_policy.update(dict(shard_spec.get("policy") or {}))
        merged["policy"] = merged_policy
        merged_metadata = dict(base_job.get("metadata") or {})
        merged_metadata.update(dict(shard_spec.get("metadata") or {}))
        merged_metadata["cooperative_task"] = {
            "task_id": group_id,
            "shard_index": shard_index,
            "shard_count": shard_count,
            "label": str(shard_spec.get("label") or f"shard-{shard_index + 1}"),
        }
        merged["metadata"] = merged_metadata
        merged["artifact_inputs"] = list(base_job.get("artifact_inputs") or []) + list(shard_spec.get("artifact_inputs") or [])
        for key in ("kind", "dispatch_mode"):
            if key in shard_spec:
                merged[key] = shard_spec[key]
        return merged

    def _resolve_cooperative_child_job(self, child: dict) -> dict:
        child_spec = dict(child or {})
        peer_id = str(child_spec.get("peer_id") or self.node_id).strip() or self.node_id
        job_id = str(child_spec.get("job_id") or "").strip()
        snapshot = dict(child_spec.get("job_snapshot") or {})
        if not job_id:
            return snapshot
        try:
            if peer_id == self.node_id:
                job = self.get_job(job_id)
            else:
                remote_client, _ = self._resolve_peer_client(peer_id)
                job = remote_client.get_job(job_id)
            return job
        except Exception as exc:
            stale = dict(snapshot)
            stale["resolution_error"] = str(exc)
            return stale

    def _cooperative_task_state(self, child_jobs: list[dict]) -> dict:
        counts: dict[str, int] = {}
        for child_job in child_jobs:
            status = str((child_job or {}).get("status") or "unknown").strip().lower() or "unknown"
            counts[status] = counts.get(status, 0) + 1
        total = len(child_jobs)
        active = counts.get("queued", 0) + counts.get("retry_wait", 0) + counts.get("running", 0) + counts.get("resuming", 0)
        if total > 0 and counts.get("completed", 0) == total:
            state = "completed"
        elif active > 0:
            state = "active"
        elif counts.get("checkpointed", 0) > 0:
            state = "checkpointed"
        elif counts.get("failed", 0) > 0 or counts.get("rejected", 0) > 0:
            state = "attention"
        elif counts.get("cancelled", 0) == total and total > 0:
            state = "cancelled"
        else:
            state = "pending"
        return {"state": state, "counts": counts, "total": total}

    def _cooperative_target_profiles(self, target_peer_ids: list[str], strategy: str) -> dict:
        """Materialise device/compute profiles + enlistment state for shard placement."""
        profiles: dict[str, dict] = {}
        peers_index: dict[str, dict] = {}
        for peer in self.list_peers(limit=500).get("peers", []):
            peer_id = peer.get("peer_id") or ""
            if peer_id:
                peers_index[peer_id] = peer
        for peer_id in target_peer_ids:
            if peer_id == self.node_id:
                compute_profile = dict(self.device_profile.get("compute_profile") or {})
                profiles[peer_id] = {
                    "peer_id": peer_id,
                    "is_local": True,
                    "device_profile": dict(self.device_profile),
                    "compute_profile": compute_profile,
                    "enlistment": {"state": "self", "role": "controller", "mode": "local"},
                    "load": {"queue_depth": self._local_queue_depth(), "pressure": self.queue_metrics().get("pressure", "idle")},
                }
                continue
            peer = peers_index.get(peer_id)
            if peer is None:
                profiles[peer_id] = {
                    "peer_id": peer_id,
                    "is_local": False,
                    "device_profile": {},
                    "compute_profile": {},
                    "enlistment": {"state": "unknown"},
                    "load": {"queue_depth": 0, "pressure": "unknown"},
                }
                continue
            device_profile = dict(peer.get("device_profile") or {})
            compute_profile = dict(device_profile.get("compute_profile") or {})
            enlistment = dict((peer.get("metadata") or {}).get("enlistment") or {})
            profiles[peer_id] = {
                "peer_id": peer_id,
                "is_local": False,
                "device_profile": device_profile,
                "compute_profile": compute_profile,
                "enlistment": enlistment,
                "load": self._peer_load_summary(peer),
            }
        return profiles

    def _shard_workload_requirements(self, base_job: dict, shard: dict) -> dict:
        """Compute effective GPU/cpu/memory requirements for a shard."""
        base_placement = self._normalized_placement(base_job)
        shard_placement_raw = dict((shard or {}).get("placement") or {})
        shard_requirements_raw = dict((shard or {}).get("requirements") or {})
        # Normalize shard placement by re-applying logic via synthetic job
        synthetic = {
            "placement": {**dict(base_job.get("placement") or {}), **shard_placement_raw},
            "requirements": {**dict(base_job.get("requirements") or {}), **shard_requirements_raw},
            "metadata": dict((shard or {}).get("metadata") or base_job.get("metadata") or {}),
        }
        shard_placement = self._normalized_placement(synthetic)
        workload_class = shard_placement.get("workload_class") or base_placement.get("workload_class") or "default"
        gpu_required = bool(shard_placement.get("gpu_required") or base_placement.get("gpu_required"))
        min_gpu_vram_mb = int(shard_placement.get("min_gpu_vram_mb") or base_placement.get("min_gpu_vram_mb") or 0)
        min_memory_mb = int(shard_placement.get("min_memory_mb") or base_placement.get("min_memory_mb") or 0)
        min_cpu_cores = float(shard_placement.get("min_cpu_cores") or base_placement.get("min_cpu_cores") or 0)
        gpu_class_preferred = shard_placement.get("gpu_class_preferred") or base_placement.get("gpu_class_preferred") or ""
        return {
            "workload_class": workload_class,
            "gpu_required": gpu_required,
            "min_gpu_vram_mb": min_gpu_vram_mb,
            "min_memory_mb": min_memory_mb,
            "min_cpu_cores": min_cpu_cores,
            "gpu_class_preferred": gpu_class_preferred,
            "placement": shard_placement,
        }

    def _peer_meets_shard(self, profile: dict, shard_needs: dict) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        compute = dict(profile.get("compute_profile") or {})
        device_profile = dict(profile.get("device_profile") or {})
        if shard_needs.get("gpu_required") and not compute.get("gpu_capable"):
            return False, ["gpu_required_not_available"]
        if shard_needs.get("min_gpu_vram_mb") and compute.get("gpu_capable"):
            if int(compute.get("gpu_vram_mb") or 0) < int(shard_needs.get("min_gpu_vram_mb") or 0):
                return False, ["gpu_vram_insufficient"]
        if shard_needs.get("min_memory_mb"):
            if int(compute.get("memory_mb") or 0) < int(shard_needs.get("min_memory_mb") or 0):
                return False, ["memory_insufficient"]
        if shard_needs.get("min_cpu_cores"):
            if float(compute.get("cpu_cores") or 0) < float(shard_needs.get("min_cpu_cores") or 0):
                return False, ["cpu_insufficient"]
        helper_state = str(device_profile.get("helper_state") or "active").strip().lower()
        if helper_state == "retired" and not profile.get("is_local"):
            return False, ["helper_retired"]
        if helper_state == "draining":
            reasons.append("helper_draining")
        enlistment = dict(profile.get("enlistment") or {})
        if enlistment.get("state") == "enlisted":
            reasons.append("enlisted")
        if compute.get("gpu_capable"):
            reasons.append("gpu_capable")
        return True, reasons

    def _select_cooperative_shard_target(
        self,
        *,
        base_job: dict,
        shard: dict,
        normalized_targets: list[str],
        peer_profiles: dict,
        used_assignments: dict,
        strategy: str,
        shard_index: int,
    ) -> str:
        needs = self._shard_workload_requirements(base_job, shard)
        candidates: list[tuple[int, str, list[str]]] = []
        for peer_id in normalized_targets:
            profile = peer_profiles.get(peer_id) or {}
            eligible, tags = self._peer_meets_shard(profile, needs)
            if not eligible:
                continue
            load_depth = int((profile.get("load") or {}).get("queue_depth") or 0)
            base_score = 1000 - used_assignments.get(peer_id, 0) * 60 - load_depth * 15
            compute = dict(profile.get("compute_profile") or {})
            if needs.get("gpu_required") and compute.get("gpu_capable"):
                base_score += 220
                if (
                    needs.get("gpu_class_preferred")
                    and compute.get("gpu_class") == needs.get("gpu_class_preferred")
                ):
                    base_score += 80
            if needs.get("workload_class") in {"gpu_training", "gpu_inference", "mixed"} and compute.get("gpu_capable"):
                base_score += 120
            if needs.get("workload_class") == "cpu_bound" and not compute.get("gpu_capable"):
                base_score += 60
            if "enlisted" in tags:
                base_score += 140
            if "helper_draining" in tags:
                base_score -= 250
            if profile.get("is_local") and strategy == "remote-only":
                continue
            candidates.append((base_score, peer_id, tags))
        if candidates:
            candidates.sort(key=lambda item: item[0], reverse=True)
            return candidates[0][1]
        # Fallback: round-robin if nothing matched
        return normalized_targets[shard_index % len(normalized_targets)]

    def _shard_placement_summary(
        self,
        *,
        base_job: dict,
        shard: dict,
        target_peer_id: str,
        peer_profiles: dict,
    ) -> dict:
        needs = self._shard_workload_requirements(base_job, shard)
        profile = peer_profiles.get(target_peer_id) or {}
        compute = dict(profile.get("compute_profile") or {})
        return {
            "workload_class": needs.get("workload_class"),
            "gpu_required": bool(needs.get("gpu_required")),
            "min_gpu_vram_mb": int(needs.get("min_gpu_vram_mb") or 0),
            "min_memory_mb": int(needs.get("min_memory_mb") or 0),
            "min_cpu_cores": float(needs.get("min_cpu_cores") or 0),
            "target_gpu_capable": bool(compute.get("gpu_capable")),
            "target_gpu_class": compute.get("gpu_class") or "",
            "target_gpu_vram_mb": int(compute.get("gpu_vram_mb") or 0),
            "target_cpu_cores": int(compute.get("cpu_cores") or 0),
            "target_memory_mb": int(compute.get("memory_mb") or 0),
            "target_enlistment_state": str((profile.get("enlistment") or {}).get("state") or ""),
            "target_is_local": bool(profile.get("is_local")),
        }

    def launch_cooperative_task(
        self,
        *,
        base_job: dict,
        shards: list[dict],
        name: str = "",
        request_id: Optional[str] = None,
        strategy: str = "spread",
        allow_local: bool = True,
        allow_remote: bool = True,
        target_peer_ids: Optional[list[str]] = None,
        auto_enlist: bool = False,
    ) -> dict:
        base_job = dict(base_job or {})
        shard_specs = [dict(item or {}) for item in (shards or [])]
        if not base_job:
            raise MeshPolicyError("base_job is required")
        if not shard_specs:
            raise MeshPolicyError("at least one shard is required")
        task_request_id = str(request_id or uuid.uuid4().hex).strip()
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT * FROM mesh_cooperative_tasks WHERE request_id=?",
                (task_request_id,),
            ).fetchone()
        if existing is not None:
            task = self.get_cooperative_task(existing["id"])
            task["deduped"] = True
            return task
        strategy_token = str(strategy or "spread").strip().lower() or "spread"
        if strategy_token not in {"spread", "local-only", "remote-only", "gpu-aware"}:
            strategy_token = "spread"
        targets_input = [str(item or "").strip() for item in (target_peer_ids or []) if str(item or "").strip()]
        # Optionally auto-enlist helpers before placement
        auto_enlist_result: Optional[dict] = None
        if auto_enlist and strategy_token != "local-only":
            try:
                auto_enlist_result = self.auto_seek_help(job=base_job, max_enlist=3, reason="cooperative_launch")
            except Exception as exc:
                auto_enlist_result = {"error": str(exc)}
        if not targets_input:
            eligible_targets = self.list_execution_targets(
                base_job,
                allow_local=allow_local if strategy_token != "remote-only" else False,
                allow_remote=allow_remote if strategy_token != "local-only" else False,
            ).get("eligible", [])
            if strategy_token == "spread":
                targets_input = [target["peer_id"] for target in eligible_targets]
            elif strategy_token == "local-only":
                targets_input = [self.node_id]
            elif strategy_token == "gpu-aware":
                targets_input = [target["peer_id"] for target in eligible_targets]
            else:
                targets_input = [target["peer_id"] for target in eligible_targets if target.get("target_type") == "peer"]
        normalized_targets: list[str] = []
        for peer_id in targets_input:
            peer_token = str(peer_id or "").strip()
            if peer_token and peer_token not in normalized_targets:
                normalized_targets.append(peer_token)
        if not normalized_targets:
            raise MeshPolicyError("no eligible cooperative task target found")
        task_id = str(uuid.uuid4())
        # Cache known targets' capability/profile info for per-shard GPU-aware placement
        peer_profiles = self._cooperative_target_profiles(normalized_targets, strategy_token)
        child_specs = []
        used_assignments: dict[str, int] = {peer_id: 0 for peer_id in normalized_targets}
        for index, shard in enumerate(shard_specs):
            explicit_target = str(shard.get("target_peer_id") or "").strip()
            if explicit_target:
                target_peer_id = explicit_target
            else:
                target_peer_id = self._select_cooperative_shard_target(
                    base_job=base_job,
                    shard=shard,
                    normalized_targets=normalized_targets,
                    peer_profiles=peer_profiles,
                    used_assignments=used_assignments,
                    strategy=strategy_token,
                    shard_index=index,
                )
            used_assignments[target_peer_id] = used_assignments.get(target_peer_id, 0) + 1
            child_job = self._merge_cooperative_child_job(base_job, shard, group_id=task_id, shard_index=index, shard_count=len(shard_specs))
            child_request_id = f"{task_request_id}:shard:{index + 1}"
            if target_peer_id == self.node_id:
                response = self.submit_local_job({**child_job, "target": self.node_id}, request_id=child_request_id)
            else:
                response = self.dispatch_job_to_peer(target_peer_id, {**child_job, "target": target_peer_id}, request_id=child_request_id)
            shard_placement_summary = self._shard_placement_summary(
                base_job=base_job,
                shard=shard,
                target_peer_id=target_peer_id,
                peer_profiles=peer_profiles,
            )
            child_specs.append(
                {
                    "shard_index": index,
                    "label": str(shard.get("label") or f"shard-{index + 1}"),
                    "peer_id": target_peer_id,
                    "job_id": ((response.get("job") or {}).get("id") or "").strip(),
                    "request_id": child_request_id,
                    "job_snapshot": dict(response.get("job") or {}),
                    "placement": shard_placement_summary,
                }
            )
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_cooperative_tasks
                (id, request_id, name, strategy, base_job, shard_count, shard_jobs, target_peers, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    task_request_id,
                    str(name or "").strip(),
                    strategy_token,
                    json.dumps(base_job),
                    len(shard_specs),
                    json.dumps(child_specs),
                    json.dumps(normalized_targets),
                    json.dumps(
                        {
                            "allow_local": bool(allow_local),
                            "allow_remote": bool(allow_remote),
                            "auto_enlist": bool(auto_enlist),
                            "auto_enlist_result": auto_enlist_result or {},
                        }
                    ),
                    now,
                    now,
                ),
            )
            conn.commit()
        self._record_event(
            "mesh.cooperative_task.launched",
            peer_id=self.node_id,
            request_id=task_request_id,
            payload={
                "task_id": task_id,
                "strategy": strategy_token,
                "shard_count": len(shard_specs),
                "target_peers": normalized_targets,
            },
        )
        return self.get_cooperative_task(task_id)

    def get_cooperative_task(self, task_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mesh_cooperative_tasks WHERE id=?",
                ((task_id or "").strip(),),
            ).fetchone()
        if row is None:
            raise MeshPolicyError("cooperative task not found")
        child_specs = list(_loads_json(row["shard_jobs"], []))
        child_jobs = []
        for child in child_specs:
            resolved_job = self._resolve_cooperative_child_job(child)
            child_jobs.append({**dict(child or {}), "job": resolved_job})
        summary = self._cooperative_task_state([child.get("job") or {} for child in child_jobs])
        return {
            "id": row["id"],
            "request_id": row["request_id"] or "",
            "name": row["name"] or "",
            "strategy": row["strategy"] or "spread",
            "base_job": _loads_json(row["base_job"], {}),
            "target_peers": list(_loads_json(row["target_peers"], [])),
            "metadata": _loads_json(row["metadata"], {}),
            "shard_count": int(row["shard_count"] or 0),
            "state": summary["state"],
            "summary": summary,
            "children": child_jobs,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_cooperative_tasks(self, *, limit: int = 25, state: str = "") -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_cooperative_tasks
                ORDER BY created_at DESC, updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit or 25)),),
            ).fetchall()
        tasks = [self.get_cooperative_task(row["id"]) for row in rows]
        state_token = str(state or "").strip().lower()
        if state_token:
            tasks = [task for task in tasks if str(task.get("state") or "").strip().lower() == state_token]
        return {"peer_id": self.node_id, "count": len(tasks), "tasks": tasks}

    # ---------------------------- mission layer ---------------------------

    def _store_mission_row(
        self,
        *,
        mission_id: str,
        request_id: str,
        title: str,
        intent: str,
        status: str,
        priority: str,
        workload_class: str,
        origin_peer_id: str,
        target_strategy: str,
        policy: Optional[dict] = None,
        continuity: Optional[dict] = None,
        metadata: Optional[dict] = None,
        child_job_ids: Optional[list[str]] = None,
        cooperative_task_ids: Optional[list[str]] = None,
        latest_checkpoint_ref: Optional[dict] = None,
        result_ref: Optional[dict] = None,
        result_bundle_ref: Optional[dict] = None,
        created_at: Optional[str] = None,
    ) -> dict:
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_missions
                (id, request_id, title, intent, status, priority, workload_class, origin_peer_id, target_strategy,
                 policy, continuity, metadata, child_job_ids, cooperative_task_ids, latest_checkpoint_ref,
                 result_ref, result_bundle_ref, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    title=excluded.title,
                    intent=excluded.intent,
                    status=excluded.status,
                    priority=excluded.priority,
                    workload_class=excluded.workload_class,
                    target_strategy=excluded.target_strategy,
                    policy=excluded.policy,
                    continuity=excluded.continuity,
                    metadata=excluded.metadata,
                    child_job_ids=excluded.child_job_ids,
                    cooperative_task_ids=excluded.cooperative_task_ids,
                    latest_checkpoint_ref=excluded.latest_checkpoint_ref,
                    result_ref=excluded.result_ref,
                    result_bundle_ref=excluded.result_bundle_ref,
                    updated_at=excluded.updated_at
                """,
                (
                    mission_id,
                    request_id,
                    str(title or "").strip(),
                    str(intent or "").strip(),
                    _normalize_mission_status(status),
                    _normalize_mission_priority(priority),
                    _normalize_workload_class(workload_class),
                    str(origin_peer_id or self.node_id).strip() or self.node_id,
                    _normalize_target_strategy(target_strategy),
                    json.dumps(_normalize_mission_policy(policy or {})),
                    json.dumps(_normalize_mission_continuity(continuity or {})),
                    json.dumps(dict(metadata or {})),
                    json.dumps(_unique_tokens(child_job_ids or [])),
                    json.dumps(_unique_tokens(cooperative_task_ids or [])),
                    json.dumps(dict(latest_checkpoint_ref or {})),
                    json.dumps(dict(result_ref or {})),
                    json.dumps(dict(result_bundle_ref or {})),
                    created_at or now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_missions WHERE request_id=?", ((request_id or "").strip(),)).fetchone()
        return self._row_to_mission(row)

    def _existing_mission_by_request(self, request_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_missions WHERE request_id=?", ((request_id or "").strip(),)).fetchone()
        return self._row_to_mission(row) if row is not None else None

    def _mission_status_from_children(self, child_jobs: list[dict], cooperative_tasks: list[dict], metadata: dict) -> str:
        statuses = [str((job or {}).get("status") or "").strip().lower() for job in child_jobs if job]
        task_states = [str((task or {}).get("state") or "").strip().lower() for task in cooperative_tasks if task]
        if not statuses and not task_states:
            return "failed" if str(metadata.get("launch_error") or "").strip() else "planned"
        if statuses and all(status == "completed" for status in statuses):
            return "completed"
        if statuses and all(status == "cancelled" for status in statuses):
            return "cancelled"
        if any(status in {"running", "resuming"} for status in statuses):
            return "active"
        if any(status in {"accepted", "queued", "retry_wait"} for status in statuses) or "pending" in task_states:
            return "waiting"
        if "active" in task_states:
            return "active"
        if any(status == "checkpointed" for status in statuses) or "checkpointed" in task_states:
            return "checkpointed"
        if any(status in {"failed", "rejected"} for status in statuses) or "attention" in task_states:
            return "failed"
        if any(status == "cancelled" for status in statuses):
            return "cancelled"
        return "planned"

    def _mission_runtime_summary(
        self,
        *,
        mission: dict,
        child_jobs: list[dict],
        cooperative_tasks: list[dict],
    ) -> tuple[dict, dict, dict, dict, dict]:
        status_counts: dict[str, int] = {}
        task_state_counts: dict[str, int] = {}
        latest_checkpoint_ref: dict[str, Any] = {}
        latest_checkpoint_at = ""
        result_ref: dict[str, Any] = {}
        result_bundle_ref: dict[str, Any] = {}
        latest_result_at = ""
        recovery_hints: list[str] = []
        resume_count = 0
        last_child_update = str(mission.get("updated_at") or "")
        lineage_jobs: list[dict[str, Any]] = []
        lineage_tasks: list[dict[str, Any]] = []

        for child in child_jobs:
            status = str((child or {}).get("status") or "unknown").strip().lower() or "unknown"
            status_counts[status] = status_counts.get(status, 0) + 1
            updated_at = str((child or {}).get("updated_at") or "")
            if updated_at and updated_at > last_child_update:
                last_child_update = updated_at
            checkpoint_ref = dict((child or {}).get("latest_checkpoint_ref") or {})
            checkpointed_at = str((child or {}).get("checkpointed_at") or updated_at or "")
            if checkpoint_ref and checkpointed_at >= latest_checkpoint_at:
                latest_checkpoint_at = checkpointed_at
                latest_checkpoint_ref = checkpoint_ref
            child_result_ref = dict((child or {}).get("result_ref") or {})
            child_bundle_ref = dict((child or {}).get("result_bundle_ref") or {})
            if (child_result_ref or child_bundle_ref) and updated_at >= latest_result_at:
                latest_result_at = updated_at
                if child_result_ref:
                    result_ref = child_result_ref
                if child_bundle_ref:
                    result_bundle_ref = child_bundle_ref
            resume_count += int((child or {}).get("resume_count") or 0)
            recovery_hint = str(((child or {}).get("recovery") or {}).get("recovery_hint") or "").strip()
            if recovery_hint and recovery_hint not in recovery_hints:
                recovery_hints.append(recovery_hint)
            lineage_jobs.append(
                {
                    "id": str((child or {}).get("id") or ""),
                    "status": status,
                    "updated_at": updated_at,
                    "checkpoint_ref": checkpoint_ref,
                    "result_ref": child_result_ref,
                    "result_bundle_ref": child_bundle_ref,
                }
            )

        for task in cooperative_tasks:
            state = str((task or {}).get("state") or "pending").strip().lower() or "pending"
            task_state_counts[state] = task_state_counts.get(state, 0) + 1
            updated_at = str((task or {}).get("updated_at") or "")
            if updated_at and updated_at > last_child_update:
                last_child_update = updated_at
            lineage_tasks.append(
                {
                    "id": str((task or {}).get("id") or ""),
                    "state": state,
                    "updated_at": updated_at,
                    "shard_count": int((task or {}).get("shard_count") or 0),
                }
            )

        stored_continuity = dict(mission.get("continuity") or {})
        continuity = {
            **stored_continuity,
            "resumable": bool(stored_continuity.get("resumable")) or any(
                bool(((child or {}).get("recovery") or {}).get("resumable")) for child in child_jobs
            ),
            "checkpoint_ready": bool(latest_checkpoint_ref),
            "latest_checkpoint_ref": latest_checkpoint_ref,
            "resume_count": resume_count,
            "recovery_hints": recovery_hints[:4],
            "child_status_counts": status_counts,
            "task_state_counts": task_state_counts,
            "last_child_update_at": last_child_update,
        }
        summary = {
            "job_count": len(child_jobs),
            "cooperative_task_count": len(cooperative_tasks),
            "child_status_counts": status_counts,
            "task_state_counts": task_state_counts,
        }
        lineage = {
            "jobs": lineage_jobs,
            "cooperative_tasks": lineage_tasks,
            "latest_checkpoint_ref": latest_checkpoint_ref,
            "result_ref": result_ref,
            "result_bundle_ref": result_bundle_ref,
        }
        return continuity, summary, latest_checkpoint_ref, {"result_ref": result_ref, "result_bundle_ref": result_bundle_ref}, lineage

    def _refresh_mission_runtime(self, mission: dict) -> dict:
        mission_data = dict(mission or {})
        child_job_ids = _unique_tokens(mission_data.get("child_job_ids") or [])
        cooperative_task_ids = _unique_tokens(mission_data.get("cooperative_task_ids") or [])
        cooperative_tasks: list[dict] = []
        for task_id in cooperative_task_ids:
            try:
                cooperative_tasks.append(self.get_cooperative_task(task_id))
            except Exception as exc:
                cooperative_tasks.append({"id": task_id, "state": "failed", "error": str(exc), "children": []})
        for task in cooperative_tasks:
            for child in list(task.get("children") or []):
                job_id = str(child.get("job_id") or ((child.get("job") or {}).get("id")) or "").strip()
                if job_id and job_id not in child_job_ids:
                    child_job_ids.append(job_id)
        child_jobs: list[dict] = []
        for job_id in child_job_ids:
            try:
                child_jobs.append(self.get_job(job_id))
            except Exception as exc:
                child_jobs.append({"id": job_id, "status": "failed", "resolution_error": str(exc), "updated_at": _utcnow()})
        continuity, summary, latest_checkpoint_ref, result_refs, lineage = self._mission_runtime_summary(
            mission=mission_data,
            child_jobs=child_jobs,
            cooperative_tasks=cooperative_tasks,
        )
        metadata = dict(mission_data.get("metadata") or {})
        status = self._mission_status_from_children(child_jobs, cooperative_tasks, metadata)
        if any(key in metadata for key in ("launch_error", "last_control_error")) and not child_job_ids and not cooperative_task_ids:
            status = "failed"
        refreshed = mission_data | {
            "status": status,
            "continuity": continuity,
            "summary": summary,
            "latest_checkpoint_ref": latest_checkpoint_ref,
            "result_ref": dict(result_refs.get("result_ref") or {}),
            "result_bundle_ref": dict(result_refs.get("result_bundle_ref") or {}),
            "child_job_ids": child_job_ids,
            "cooperative_task_ids": cooperative_task_ids,
            "child_jobs": child_jobs,
            "cooperative_tasks": cooperative_tasks,
            "lineage": lineage,
        }
        stored_changed = (
            _normalize_mission_status(mission_data.get("status")) != status
            or dict(mission_data.get("continuity") or {}) != continuity
            or dict(mission_data.get("latest_checkpoint_ref") or {}) != latest_checkpoint_ref
            or dict(mission_data.get("result_ref") or {}) != refreshed["result_ref"]
            or dict(mission_data.get("result_bundle_ref") or {}) != refreshed["result_bundle_ref"]
            or _unique_tokens(mission_data.get("child_job_ids") or []) != child_job_ids
        )
        if stored_changed:
            refreshed = self._store_mission_row(
                mission_id=mission_data["id"],
                request_id=mission_data["request_id"],
                title=mission_data.get("title") or "",
                intent=mission_data.get("intent") or "",
                status=status,
                priority=mission_data.get("priority") or "normal",
                workload_class=mission_data.get("workload_class") or "default",
                origin_peer_id=mission_data.get("origin_peer_id") or self.node_id,
                target_strategy=mission_data.get("target_strategy") or "local",
                policy=mission_data.get("policy") or {},
                continuity=continuity,
                metadata=metadata,
                child_job_ids=child_job_ids,
                cooperative_task_ids=cooperative_task_ids,
                latest_checkpoint_ref=latest_checkpoint_ref,
                result_ref=refreshed["result_ref"],
                result_bundle_ref=refreshed["result_bundle_ref"],
                created_at=mission_data.get("created_at") or None,
            )
            refreshed = refreshed | {
                "summary": summary,
                "child_jobs": child_jobs,
                "cooperative_tasks": cooperative_tasks,
                "lineage": lineage,
            }
        return refreshed

    def launch_mission(
        self,
        *,
        title: str = "",
        intent: str = "",
        request_id: Optional[str] = None,
        priority: str = "normal",
        workload_class: str = "",
        target_strategy: str = "",
        policy: Optional[dict] = None,
        continuity: Optional[dict] = None,
        metadata: Optional[dict] = None,
        job: Optional[dict] = None,
        cooperative_task: Optional[dict] = None,
    ) -> dict:
        job_spec = dict(job or {})
        task_spec = dict(cooperative_task or {})
        if bool(job_spec) == bool(task_spec):
            raise MeshPolicyError("mission launch requires exactly one of job or cooperative_task")
        mission_request_id = str(request_id or uuid.uuid4().hex).strip()
        existing = self._existing_mission_by_request(mission_request_id)
        if existing is not None:
            mission = self.get_mission(existing["id"])
            mission["deduped"] = True
            return mission
        launch_type = "cooperative_task" if task_spec else "job"
        title_token = str(title or task_spec.get("name") or job_spec.get("kind") or "mission").strip() or "mission"
        intent_token = str(intent or f"Launch {launch_type.replace('_', ' ')}").strip()
        inferred_workload = workload_class
        if not inferred_workload:
            if task_spec:
                inferred_workload = (
                    dict(task_spec.get("base_job") or {}).get("metadata") or {}
                ).get("workload_class") or ""
            else:
                inferred_workload = (job_spec.get("metadata") or {}).get("workload_class") or ""
        target_strategy_token = _normalize_target_strategy(
            target_strategy
            or (f"cooperative_{str(task_spec.get('strategy') or 'spread')}" if task_spec else "local")
        )
        mission_metadata = dict(metadata or {})
        mission_metadata["launch"] = {"type": launch_type}
        created = self._store_mission_row(
            mission_id=str(uuid.uuid4()),
            request_id=mission_request_id,
            title=title_token,
            intent=intent_token,
            status="planned",
            priority=priority,
            workload_class=inferred_workload or "default",
            origin_peer_id=self.node_id,
            target_strategy=target_strategy_token,
            policy=policy or {},
            continuity=continuity or {},
            metadata=mission_metadata,
        )
        try:
            mission_context = {
                "mission_id": created["id"],
                "title": title_token,
                "intent": intent_token,
                "target_strategy": target_strategy_token,
            }
            child_job_ids: list[str] = []
            cooperative_task_ids: list[str] = []
            updated_metadata = dict(mission_metadata)
            if job_spec:
                launch_job = dict(job_spec)
                launch_job_metadata = dict(launch_job.get("metadata") or {})
                launch_job_metadata["mission"] = mission_context
                launch_job["metadata"] = launch_job_metadata
                response = self.submit_local_job(
                    {**launch_job, "target": self.node_id},
                    request_id=f"{mission_request_id}:job",
                )
                child_job_id = str(((response.get("job") or {}).get("id")) or "").strip()
                if child_job_id:
                    child_job_ids.append(child_job_id)
                updated_metadata["launch"]["job_request_id"] = f"{mission_request_id}:job"
                updated_metadata["launch"]["job_kind"] = launch_job.get("kind") or ""
            else:
                launch_task = dict(task_spec)
                base_job = dict(launch_task.get("base_job") or {})
                base_job_metadata = dict(base_job.get("metadata") or {})
                base_job_metadata["mission"] = mission_context
                base_job["metadata"] = base_job_metadata
                task = self.launch_cooperative_task(
                    base_job=base_job,
                    shards=list(launch_task.get("shards") or []),
                    name=str(launch_task.get("name") or title_token),
                    request_id=f"{mission_request_id}:coop",
                    strategy=str(launch_task.get("strategy") or "spread"),
                    allow_local=bool(launch_task.get("allow_local", True)),
                    allow_remote=bool(launch_task.get("allow_remote", True)),
                    target_peer_ids=list(launch_task.get("target_peer_ids") or []),
                    auto_enlist=bool(launch_task.get("auto_enlist", False)),
                )
                cooperative_task_ids.append(task["id"])
                child_job_ids.extend(
                    [
                        str(child.get("job_id") or "").strip()
                        for child in list(task.get("children") or [])
                        if str(child.get("job_id") or "").strip()
                    ]
                )
                updated_metadata["launch"]["cooperative_request_id"] = f"{mission_request_id}:coop"
                updated_metadata["launch"]["strategy"] = str(task.get("strategy") or launch_task.get("strategy") or "spread")
            created = self._store_mission_row(
                mission_id=created["id"],
                request_id=mission_request_id,
                title=title_token,
                intent=intent_token,
                status="waiting",
                priority=priority,
                workload_class=inferred_workload or "default",
                origin_peer_id=self.node_id,
                target_strategy=target_strategy_token,
                policy=policy or {},
                continuity=continuity or {},
                metadata=updated_metadata,
                child_job_ids=child_job_ids,
                cooperative_task_ids=cooperative_task_ids,
                created_at=created.get("created_at") or None,
            )
            self._record_event(
                "mesh.mission.launched",
                peer_id=self.node_id,
                request_id=mission_request_id,
                payload={
                    "mission_id": created["id"],
                    "launch_type": launch_type,
                    "child_job_count": len(child_job_ids),
                    "cooperative_task_count": len(cooperative_task_ids),
                },
            )
            return self.get_mission(created["id"])
        except Exception as exc:
            failed_metadata = dict(mission_metadata)
            failed_metadata["launch_error"] = str(exc)
            self._store_mission_row(
                mission_id=created["id"],
                request_id=mission_request_id,
                title=title_token,
                intent=intent_token,
                status="failed",
                priority=priority,
                workload_class=inferred_workload or "default",
                origin_peer_id=self.node_id,
                target_strategy=target_strategy_token,
                policy=policy or {},
                continuity=continuity or {},
                metadata=failed_metadata,
                created_at=created.get("created_at") or None,
            )
            raise

    def get_mission(self, mission_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_missions WHERE id=?", ((mission_id or "").strip(),)).fetchone()
        if row is None:
            raise MeshPolicyError("mission not found")
        return self._refresh_mission_runtime(self._row_to_mission(row))

    def list_missions(self, *, limit: int = 25, status: str = "") -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM mesh_missions
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (max(1, int(limit or 25)),),
            ).fetchall()
        missions = [self.get_mission(row["id"]) for row in rows]
        status_token = _normalize_mission_status(status) if str(status or "").strip() else ""
        if status_token:
            missions = [mission for mission in missions if mission.get("status") == status_token]
        return {"peer_id": self.node_id, "count": len(missions), "missions": missions}

    def _record_mission_control_action(self, mission: dict, *, action: str, operator_id: str = "", reason: str = "") -> dict:
        mission_data = dict(mission or {})
        metadata = dict(mission_data.get("metadata") or {})
        metadata["last_control_action"] = str(action or "").strip()
        metadata["last_control_at"] = _utcnow()
        metadata["last_control_by"] = str(operator_id or "")
        metadata["last_control_reason"] = str(reason or "")
        return self._store_mission_row(
            mission_id=mission_data["id"],
            request_id=mission_data["request_id"],
            title=mission_data.get("title") or "",
            intent=mission_data.get("intent") or "",
            status=mission_data.get("status") or "planned",
            priority=mission_data.get("priority") or "normal",
            workload_class=mission_data.get("workload_class") or "default",
            origin_peer_id=mission_data.get("origin_peer_id") or self.node_id,
            target_strategy=mission_data.get("target_strategy") or "local",
            policy=mission_data.get("policy") or {},
            continuity=mission_data.get("continuity") or {},
            metadata=metadata,
            child_job_ids=mission_data.get("child_job_ids") or [],
            cooperative_task_ids=mission_data.get("cooperative_task_ids") or [],
            latest_checkpoint_ref=mission_data.get("latest_checkpoint_ref") or {},
            result_ref=mission_data.get("result_ref") or {},
            result_bundle_ref=mission_data.get("result_bundle_ref") or {},
            created_at=mission_data.get("created_at") or None,
        )

    def _recover_mission(
        self,
        mission_id: str,
        *,
        operator_id: str = "",
        reason: str,
        mode: str,
        checkpoint_artifact_id: str = "",
    ) -> dict:
        mission = self.get_mission(mission_id)
        mode_token = str(mode or "").strip().lower() or "resume_latest"
        if mode_token not in {"resume_latest", "resume_checkpoint", "restart"}:
            raise MeshPolicyError("unsupported mission recovery mode")
        child_jobs = [dict(job or {}) for job in list(mission.get("child_jobs") or [])]
        if not child_jobs:
            raise MeshPolicyError("mission has no child jobs to recover")
        self._record_mission_control_action(mission, action=mode_token, operator_id=operator_id, reason=reason)
        updated_jobs = []
        queue_messages = []
        errors = []
        skipped = []
        explicit_checkpoint_id = str(checkpoint_artifact_id or "").strip()
        if explicit_checkpoint_id and len(child_jobs) != 1:
            raise MeshPolicyError("mission-level explicit checkpoint selection requires exactly one child job")
        for child_job in child_jobs:
            job_id = str(child_job.get("id") or "").strip()
            if not job_id:
                continue
            try:
                status = str(child_job.get("status") or "").strip().lower()
                if mode_token == "restart":
                    recovered = self.restart_job(job_id, operator_id=operator_id, reason=reason)
                elif mode_token == "resume_checkpoint":
                    checkpoint_id = explicit_checkpoint_id or str(
                        dict(child_job.get("latest_checkpoint_ref") or {}).get("id") or ""
                    ).strip()
                    if not checkpoint_id:
                        skipped.append({"job_id": job_id, "reason": "checkpoint_unavailable"})
                        continue
                    recovered = self.resume_job_from_checkpoint(
                        job_id,
                        checkpoint_artifact_id=checkpoint_id,
                        operator_id=operator_id,
                        reason=reason,
                    )
                else:
                    if status not in {"checkpointed", "retry_wait", "failed"}:
                        skipped.append({"job_id": job_id, "reason": f"status_{status or 'unknown'}"})
                        continue
                    recovered = self.resume_job(job_id, operator_id=operator_id, reason=reason)
                updated_jobs.append(recovered["job"])
                queue_messages.append(recovered["queue_message"])
            except Exception as exc:
                errors.append({"job_id": job_id, "error": str(exc)})
        if not updated_jobs and errors:
            raise MeshPolicyError(errors[0]["error"])
        if not updated_jobs and skipped:
            raise MeshPolicyError("mission has no recoverable child jobs for requested action")
        updated = self.get_mission(mission_id)
        event_type = {
            "resume_latest": "mesh.mission.resume_requested",
            "resume_checkpoint": "mesh.mission.resume_checkpoint_requested",
            "restart": "mesh.mission.restart_requested",
        }[mode_token]
        self._record_event(
            event_type,
            peer_id=self.node_id,
            request_id=updated["request_id"],
            payload={
                "mission_id": updated["id"],
                "mode": mode_token,
                "operator_id": str(operator_id or ""),
                "reason": str(reason or ""),
                "job_count": len(updated_jobs),
                "error_count": len(errors),
                "skipped_count": len(skipped),
            },
        )
        return {
            "status": updated.get("status") or ("waiting" if mode_token != "restart" else "waiting"),
            "mission": updated,
            "jobs": updated_jobs,
            "queue_messages": queue_messages,
            "errors": errors,
            "skipped": skipped,
        }

    def resume_mission(self, mission_id: str, *, operator_id: str = "", reason: str = "mission_resume_latest") -> dict:
        return self._recover_mission(
            mission_id,
            operator_id=operator_id,
            reason=reason,
            mode="resume_latest",
        )

    def resume_mission_from_checkpoint(
        self,
        mission_id: str,
        *,
        operator_id: str = "",
        reason: str = "mission_resume_checkpoint",
        checkpoint_artifact_id: str = "",
    ) -> dict:
        return self._recover_mission(
            mission_id,
            operator_id=operator_id,
            reason=reason,
            mode="resume_checkpoint",
            checkpoint_artifact_id=checkpoint_artifact_id,
        )

    def cancel_mission(self, mission_id: str, *, operator_id: str = "", reason: str = "mission_cancelled") -> dict:
        mission = self.get_mission(mission_id)
        self._record_mission_control_action(mission, action="cancel", operator_id=operator_id, reason=reason)
        results = []
        errors = []
        for job_id in _unique_tokens(mission.get("child_job_ids") or []):
            try:
                results.append(self.cancel_job(job_id, reason=f"{reason}:{operator_id or 'operator'}"))
            except Exception as exc:
                errors.append({"job_id": job_id, "error": str(exc)})
        updated = self.get_mission(mission_id)
        return {"status": updated.get("status") or "cancelled", "mission": updated, "jobs": results, "errors": errors}

    def restart_mission(self, mission_id: str, *, operator_id: str = "", reason: str = "mission_restart") -> dict:
        return self._recover_mission(
            mission_id,
            operator_id=operator_id,
            reason=reason,
            mode="restart",
        )

    # --------------------------- helper enlistment --------------------------

    def mesh_pressure(self) -> dict:
        """Summarise local mesh compute pressure for helper-enlistment planning."""
        metrics = self.queue_metrics()
        queued = int((metrics.get("counts") or {}).get("queued") or 0)
        inflight = int((metrics.get("counts") or {}).get("inflight") or 0)
        workers = dict(metrics.get("workers") or {})
        total_slots = int(workers.get("total_slots") or 0)
        available_slots = int(workers.get("available_slots") or 0)
        oldest_queued_at = metrics.get("oldest_queued_at") or ""
        pressure = str(metrics.get("pressure") or "idle").strip().lower()
        backlog_ratio = metrics.get("backlog_ratio")
        reasons: list[str] = []
        if pressure == "saturated":
            reasons.append("queue_saturated")
        elif pressure == "elevated":
            reasons.append("queue_elevated")
        if total_slots <= 0 and (queued > 0 or inflight > 0):
            reasons.append("no_local_slots")
        if available_slots <= 0 and queued > 0:
            reasons.append("no_available_slots")
        if queued > max(1, total_slots) * 2 and total_slots > 0:
            reasons.append("backlog_gt_2x_slots")
        compute_profile = dict(self.device_profile.get("compute_profile") or {})
        # surface GPU-weighted saturation signal for workload_class=gpu_*
        if self.device_profile.get("battery_powered") and pressure in {"saturated", "elevated"}:
            reasons.append("battery_under_load")
        return {
            "peer_id": self.node_id,
            "pressure": pressure,
            "queued": queued,
            "inflight": inflight,
            "total_slots": total_slots,
            "available_slots": available_slots,
            "backlog_ratio": backlog_ratio,
            "oldest_queued_at": oldest_queued_at,
            "gpu_capable": bool(compute_profile.get("gpu_capable")),
            "gpu_count": int(compute_profile.get("gpu_count") or 0),
            "reasons": reasons,
            "needs_help": bool(reasons and pressure in {"elevated", "saturated"}),
            "observed_at": _utcnow(),
        }

    def _peer_enlistment_state(self, peer: dict) -> dict:
        metadata = dict((peer or {}).get("metadata") or {})
        enlistment = dict(metadata.get("enlistment") or {})
        device_profile = dict(peer.get("device_profile") or {})
        return {
            "peer_id": peer.get("peer_id") or "",
            "display_name": peer.get("display_name") or peer.get("peer_id") or "",
            "trust_tier": peer.get("trust_tier") or "trusted",
            "mode": str(enlistment.get("mode") or "idle").strip().lower(),
            "state": str(enlistment.get("state") or "unenlisted").strip().lower(),
            "role": str(enlistment.get("role") or "").strip().lower(),
            "enlisted_at": enlistment.get("enlisted_at") or "",
            "last_action_at": enlistment.get("last_action_at") or "",
            "last_reason": enlistment.get("last_reason") or "",
            "source": enlistment.get("source") or "",
            "drain_reason": enlistment.get("drain_reason") or "",
            "device_class": device_profile.get("device_class") or "full",
            "execution_tier": device_profile.get("execution_tier") or "standard",
            "helper_state": device_profile.get("helper_state") or "active",
            "compute_profile": dict(device_profile.get("compute_profile") or {}),
            "history": list(enlistment.get("history") or [])[-8:],
        }

    def list_helpers(self, *, limit: int = 100) -> dict:
        peers = self.list_peers(limit=limit).get("peers", [])
        helpers = [self._peer_enlistment_state(peer) for peer in peers]
        active = [item for item in helpers if item["state"] in {"enlisted", "draining"}]
        return {
            "peer_id": self.node_id,
            "count": len(helpers),
            "active_count": len(active),
            "pressure": self.mesh_pressure(),
            "helpers": helpers,
        }

    def _record_enlistment_action(
        self,
        peer_id: str,
        *,
        mode: Optional[str] = None,
        state: Optional[str] = None,
        role: Optional[str] = None,
        reason: str = "",
        source: str = "operator",
        drain_reason: str = "",
    ) -> dict:
        existing_row = self._get_peer_row(peer_id)
        if existing_row is None:
            raise MeshPolicyError("peer not found for enlistment")
        existing_metadata = dict(_loads_json(existing_row["metadata"], {}))
        enlistment = dict(existing_metadata.get("enlistment") or {})
        now = _utcnow()
        history = list(enlistment.get("history") or [])
        event_entry = {
            "at": now,
            "mode": mode if mode is not None else enlistment.get("mode"),
            "state": state if state is not None else enlistment.get("state"),
            "role": role if role is not None else enlistment.get("role"),
            "reason": reason,
            "source": source,
        }
        history.append(event_entry)
        enlistment_update = dict(enlistment)
        if mode is not None:
            enlistment_update["mode"] = mode
        if state is not None:
            enlistment_update["state"] = state
            if state == "enlisted" and not enlistment_update.get("enlisted_at"):
                enlistment_update["enlisted_at"] = now
            if state == "unenlisted":
                enlistment_update["enlisted_at"] = ""
        if role is not None:
            enlistment_update["role"] = role
        if source:
            enlistment_update["source"] = source
        if reason:
            enlistment_update["last_reason"] = reason
        if drain_reason:
            enlistment_update["drain_reason"] = drain_reason
        enlistment_update["last_action_at"] = now
        enlistment_update["history"] = history[-20:]
        existing_metadata["enlistment"] = enlistment_update
        updated_peer = self._update_peer_record(peer_id, metadata=existing_metadata)
        self._record_event(
            "mesh.helper.action",
            peer_id=peer_id,
            payload={
                "peer_id": peer_id,
                "mode": enlistment_update.get("mode"),
                "state": enlistment_update.get("state"),
                "role": enlistment_update.get("role"),
                "reason": reason,
                "source": source,
                "drain_reason": drain_reason,
            },
        )
        return self._peer_enlistment_state(updated_peer)

    def enlist_helper(
        self,
        peer_id: str,
        *,
        mode: str = "on_demand",
        role: str = "helper",
        reason: str = "operator_enlist",
        source: str = "operator",
    ) -> dict:
        peer_token = str(peer_id or "").strip()
        if not peer_token:
            raise MeshPolicyError("peer_id is required")
        mode_token = str(mode or "on_demand").strip().lower()
        if mode_token not in {"on_demand", "always", "scheduled", "burst"}:
            mode_token = "on_demand"
        role_token = str(role or "helper").strip().lower()
        if role_token not in {"helper", "relay", "gpu_helper", "drain"}:
            role_token = "helper"
        peer_row = self._get_peer_row(peer_token)
        if peer_row is None:
            raise MeshPolicyError("peer not found for enlistment")
        trust = _normalize_trust_tier(peer_row["trust_tier"])
        if trust in {"blocked", "public"}:
            raise MeshPolicyError(f"cannot enlist peer with trust_tier={trust}")
        return self._record_enlistment_action(
            peer_token,
            mode=mode_token,
            state="enlisted",
            role=role_token,
            reason=reason,
            source=source,
        )

    def drain_helper(
        self,
        peer_id: str,
        *,
        drain_reason: str = "operator_drain",
        source: str = "operator",
    ) -> dict:
        peer_token = str(peer_id or "").strip()
        if not peer_token:
            raise MeshPolicyError("peer_id is required")
        return self._record_enlistment_action(
            peer_token,
            state="draining",
            reason=drain_reason,
            drain_reason=drain_reason,
            source=source,
        )

    def retire_helper(
        self,
        peer_id: str,
        *,
        reason: str = "operator_retire",
        source: str = "operator",
    ) -> dict:
        peer_token = str(peer_id or "").strip()
        if not peer_token:
            raise MeshPolicyError("peer_id is required")
        return self._record_enlistment_action(
            peer_token,
            mode="idle",
            state="unenlisted",
            role="",
            reason=reason,
            source=source,
        )

    def plan_helper_enlistment(
        self,
        *,
        job: Optional[dict] = None,
        pressure: Optional[dict] = None,
        limit: int = 6,
    ) -> dict:
        mesh_pressure = dict(pressure or self.mesh_pressure())
        job_input = dict(job or {})
        if job_input:
            placement = self._normalized_placement(job_input)
        else:
            placement = {
                "workload_class": "default",
                "gpu_required": bool(mesh_pressure.get("pressure") == "saturated" and mesh_pressure.get("gpu_capable")),
                "queue_class": "default",
            }
        peers = self.list_peers(limit=200).get("peers", [])
        candidates = []
        synthetic_job = {"requirements": {"placement": placement}} if not job_input else job_input
        for peer in peers:
            enlistment = self._peer_enlistment_state(peer)
            trust = _normalize_trust_tier(peer.get("trust_tier"))
            if trust in {"blocked", "public"}:
                continue
            score, reasons = self._peer_candidate_score(peer, synthetic_job)
            if score <= -10000:
                continue
            device_profile = dict(peer.get("device_profile") or {})
            compute_profile = dict(device_profile.get("compute_profile") or {})
            current_state = enlistment["state"]
            score_bonus = 0
            plan_reasons = list(reasons)
            if current_state == "enlisted":
                score_bonus += 80
                plan_reasons.append("already_enlisted")
            elif current_state == "draining":
                score_bonus -= 180
                plan_reasons.append("draining")
            if compute_profile.get("gpu_capable"):
                score_bonus += 60
                plan_reasons.append("gpu_capable")
            if device_profile.get("device_class") in {"full", "relay"}:
                score_bonus += 40
            if device_profile.get("battery_powered"):
                score_bonus -= 30
                plan_reasons.append("battery_powered")
            candidates.append(
                {
                    "peer_id": peer.get("peer_id"),
                    "display_name": peer.get("display_name") or peer.get("peer_id"),
                    "trust_tier": trust,
                    "score": score + score_bonus,
                    "raw_placement_score": score,
                    "enlistment": enlistment,
                    "device_class": device_profile.get("device_class") or "full",
                    "execution_tier": device_profile.get("execution_tier") or "standard",
                    "compute_profile": compute_profile,
                    "reasons": plan_reasons,
                    "recommended_action": (
                        "reuse" if current_state == "enlisted"
                        else ("resume" if current_state == "draining" else "enlist")
                    ),
                }
            )
        candidates.sort(key=lambda item: item["score"], reverse=True)
        picks = candidates[: max(1, int(limit or 6))]
        return {
            "peer_id": self.node_id,
            "pressure": mesh_pressure,
            "placement": placement,
            "candidate_count": len(candidates),
            "candidates": picks,
            "generated_at": _utcnow(),
        }

    def auto_seek_help(
        self,
        *,
        job: Optional[dict] = None,
        max_enlist: int = 2,
        mode: str = "on_demand",
        reason: str = "auto_pressure",
        allow_remote_seek: bool = False,
        seek_hosts: Optional[list[str]] = None,
    ) -> dict:
        pressure = self.mesh_pressure()
        plan = self.plan_helper_enlistment(job=job, pressure=pressure, limit=max(1, int(max_enlist or 2)) * 2)
        enlisted = []
        skipped = []
        for candidate in plan.get("candidates") or []:
            if len(enlisted) >= max(1, int(max_enlist or 2)):
                break
            peer_id = candidate.get("peer_id") or ""
            action = candidate.get("recommended_action") or "enlist"
            if action == "reuse":
                skipped.append({"peer_id": peer_id, "reason": "already_enlisted"})
                continue
            try:
                role = "gpu_helper" if candidate.get("compute_profile", {}).get("gpu_capable") else "helper"
                state = self.enlist_helper(
                    peer_id,
                    mode=mode,
                    role=role,
                    reason=reason,
                    source="auto",
                )
                enlisted.append({"peer_id": peer_id, "state": state})
            except Exception as exc:
                skipped.append({"peer_id": peer_id, "reason": str(exc)})
        discovery: dict[str, Any] = {}
        if allow_remote_seek and (not enlisted or pressure.get("pressure") == "saturated"):
            try:
                discovery = self.seek_peers(
                    hosts=list(seek_hosts or []) or None,
                    trust_tier="trusted",
                    auto_connect=True,
                    refresh_known=True,
                )
            except Exception as exc:
                discovery = {"error": str(exc)}
        self._record_event(
            "mesh.helper.auto_seek",
            peer_id=self.node_id,
            payload={
                "pressure": pressure,
                "enlisted": [entry["peer_id"] for entry in enlisted],
                "skipped": [entry["peer_id"] for entry in skipped],
                "mode": mode,
                "reason": reason,
            },
        )
        return {
            "peer_id": self.node_id,
            "pressure": pressure,
            "plan": plan,
            "enlisted": enlisted,
            "skipped": skipped,
            "discovery": discovery,
            "generated_at": _utcnow(),
        }

    def _row_to_offload_preference(self, row) -> dict:
        if row is None:
            return {}
        return {
            "peer_id": str(row["peer_id"] or "").strip(),
            "workload_class": _normalize_workload_class(row["workload_class"] or "default"),
            "preference": _normalize_preference_token(row["preference"] or "allow"),
            "source": str(row["source"] or "").strip(),
            "metadata": _loads_json(row["metadata"], {}),
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
        }

    def set_offload_preference(
        self,
        peer_id: str,
        *,
        workload_class: str = "default",
        preference: str = "allow",
        source: str = "operator",
        metadata: Optional[dict] = None,
    ) -> dict:
        peer_token = str(peer_id or "").strip()
        if not peer_token:
            raise MeshPolicyError("peer_id is required")
        workload_token = _normalize_workload_class(workload_class or "default")
        preference_token = _normalize_preference_token(preference)
        if self._get_peer_row(peer_token) is None:
            raise MeshPolicyError("peer not found for offload preference")
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_offload_preferences
                (peer_id, workload_class, preference, source, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(peer_id, workload_class) DO UPDATE SET
                    preference=excluded.preference,
                    source=excluded.source,
                    metadata=excluded.metadata,
                    updated_at=excluded.updated_at
                """,
                (
                    peer_token,
                    workload_token,
                    preference_token,
                    str(source or "operator").strip(),
                    json.dumps(dict(metadata or {})),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM mesh_offload_preferences WHERE peer_id=? AND workload_class=?",
                (peer_token, workload_token),
            ).fetchone()
            conn.commit()
        result = self._row_to_offload_preference(row)
        self._record_event(
            "mesh.offload.preference",
            peer_id=peer_token,
            payload={
                "peer_id": peer_token,
                "workload_class": workload_token,
                "preference": preference_token,
                "source": str(source or "operator").strip(),
            },
        )
        return result

    def list_offload_preferences(
        self,
        *,
        limit: int = 100,
        peer_id: str = "",
        workload_class: str = "",
    ) -> dict:
        query = ["SELECT * FROM mesh_offload_preferences WHERE 1=1"]
        args: list[Any] = []
        if str(peer_id or "").strip():
            query.append("AND peer_id=?")
            args.append(str(peer_id or "").strip())
        if str(workload_class or "").strip():
            query.append("AND workload_class=?")
            args.append(_normalize_workload_class(workload_class))
        query.append("ORDER BY updated_at DESC LIMIT ?")
        args.append(max(1, int(limit or 100)))
        with self._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(args)).fetchall()
        items = [self._row_to_offload_preference(row) for row in rows]
        return {"peer_id": self.node_id, "count": len(items), "preferences": items}

    def _offload_preferences_map(self, workload_class: str) -> dict[str, dict]:
        workload_token = _normalize_workload_class(workload_class or "default")
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_offload_preferences
                WHERE workload_class IN (?, 'default')
                ORDER BY CASE WHEN workload_class=? THEN 0 ELSE 1 END, updated_at DESC
                """,
                (workload_token, workload_token),
            ).fetchall()
        mapped: dict[str, dict] = {}
        for row in rows:
            item = self._row_to_offload_preference(row)
            peer_token = item.get("peer_id") or ""
            if peer_token and peer_token not in mapped:
                mapped[peer_token] = item
        return mapped

    def evaluate_autonomous_offload(self, *, job: Optional[dict] = None) -> dict:
        policy = _normalize_offload_policy(
            (self.device_profile or {}).get("offload_policy") or {},
            self.device_profile,
        )
        pressure = self.mesh_pressure()
        job_input = dict(job or {})
        if job_input:
            placement = self._normalized_placement(job_input)
        else:
            placement = {
                "workload_class": "default",
                "gpu_required": False,
                "queue_class": "default",
            }
        threshold_ok = _pressure_rank(pressure.get("pressure")) >= _pressure_rank(policy.get("pressure_threshold"))
        result = {
            "peer_id": self.node_id,
            "policy": policy,
            "pressure": pressure,
            "placement": placement,
            "decision": "noop",
            "action": "idle",
            "approval_required": False,
            "reasons": [],
            "candidate_count": 0,
            "eligible_candidate_count": 0,
            "candidates": [],
            "eligible_candidates": [],
            "generated_at": _utcnow(),
        }
        if not policy.get("enabled"):
            result["reasons"].append("policy_disabled")
            return result
        if not pressure.get("needs_help"):
            result["reasons"].append("pressure_does_not_need_help")
            return result
        if not threshold_ok:
            result["reasons"].append("pressure_below_threshold")
            return result
        workload_class = _normalize_workload_class(placement.get("workload_class") or "default")
        allowed_workloads = set(policy.get("allowed_workload_classes") or [])
        if allowed_workloads and workload_class not in allowed_workloads:
            result["reasons"].append("workload_not_allowed_by_policy")
            return result
        plan = self.plan_helper_enlistment(
            job=job_input,
            pressure=pressure,
            limit=max(2, int(policy.get("max_auto_enlist") or 2) * 3),
        )
        candidates = list(plan.get("candidates") or [])
        preference_map = self._offload_preferences_map(workload_class)
        eligible = []
        approval_reasons: list[str] = []
        for candidate in candidates:
            trust = _normalize_trust_tier(candidate.get("trust_tier"))
            device_class = str(candidate.get("device_class") or "full").strip().lower()
            compute_profile = dict(candidate.get("compute_profile") or {})
            score = int(candidate.get("score") or 0)
            peer_pref = dict(preference_map.get(candidate.get("peer_id") or "") or {})
            pref_token = _normalize_preference_token(peer_pref.get("preference") or "allow")
            candidate["offload_preference"] = peer_pref
            if trust not in set(policy.get("allowed_trust_tiers") or []):
                continue
            if device_class not in set(policy.get("allowed_device_classes") or []):
                continue
            if not policy.get("allow_battery_helpers") and "battery_powered" in set(candidate.get("reasons") or []):
                continue
            if pref_token == "deny":
                candidate["reasons"] = list(candidate.get("reasons") or []) + ["preference_deny"]
                continue
            if score < int(policy.get("min_candidate_score") or 0):
                continue
            if pref_token == "avoid":
                candidate["reasons"] = list(candidate.get("reasons") or []) + ["preference_avoid"]
                continue
            if pref_token == "prefer":
                candidate["score"] = score + 120
                candidate["reasons"] = list(candidate.get("reasons") or []) + ["preference_prefer"]
            eligible.append(candidate)
            if trust in set(policy.get("approval_trust_tiers") or []):
                approval_reasons.append(f"trust_tier_requires_approval:{trust}")
            if device_class in set(policy.get("approval_device_classes") or []):
                approval_reasons.append(f"device_class_requires_approval:{device_class}")
            if policy.get("approval_for_gpu_helpers") and compute_profile.get("gpu_capable"):
                approval_reasons.append("gpu_helper_requires_approval")
            if workload_class in set(policy.get("approval_workload_classes") or []):
                approval_reasons.append(f"workload_requires_approval:{workload_class}")
            if pref_token == "approval":
                approval_reasons.append("preference_requires_approval")
        eligible.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        result["candidate_count"] = len(candidates)
        result["eligible_candidate_count"] = len(eligible)
        result["candidates"] = candidates
        result["eligible_candidates"] = eligible[: max(1, int(policy.get("max_auto_enlist") or 2))]
        if not eligible:
            result["reasons"].append("no_eligible_helpers")
            return result
        if policy.get("mode") == "manual":
            result["decision"] = "suggest"
            result["action"] = "manual"
            result["reasons"].append("manual_mode")
            return result
        if policy.get("mode") == "approval" or approval_reasons:
            result["decision"] = "request_approval"
            result["action"] = "approval"
            result["approval_required"] = True
            result["reasons"].extend(_unique_tokens(approval_reasons))
            return result
        result["decision"] = "auto_enlist"
        result["action"] = "auto"
        result["reasons"].append("policy_allows_auto_enlist")
        return result

    def _autonomous_offload_request_id(self, evaluation: dict) -> str:
        eligible = list(evaluation.get("eligible_candidates") or [])
        peer_ids = sorted(str(item.get("peer_id") or "") for item in eligible if str(item.get("peer_id") or "").strip())
        placement = dict(evaluation.get("placement") or {})
        pressure = dict(evaluation.get("pressure") or {})
        basis = json.dumps(
            {
                "node_id": self.node_id,
                "peers": peer_ids,
                "pressure": pressure.get("pressure"),
                "workload_class": placement.get("workload_class"),
                "gpu_required": placement.get("gpu_required"),
            },
            sort_keys=True,
        )
        return "autonomy-offload-" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]

    def _apply_autonomous_offload_approval(
        self,
        approval: dict,
        *,
        decision: str = "approved",
        operator_peer_id: str = "",
        operator_agent_id: str = "",
        reason: str = "",
    ) -> dict:
        metadata = dict(approval.get("metadata") or {})
        autonomy = dict(metadata.get("autonomous_offload") or {})
        if not autonomy:
            return {"status": "ignored", "reason": "not_autonomous_offload"}
        peer_ids = [str(item).strip() for item in (autonomy.get("peer_ids") or []) if str(item).strip()]
        if not peer_ids:
            return {"status": "ignored", "reason": "no_helper_peers"}
        workload_class = _normalize_workload_class(autonomy.get("workload_class") or "default")
        decision_token = str(decision or "approved").strip().lower()
        if decision_token in {"rejected", "deferred"}:
            learned = []
            learned_pref = "deny" if decision_token == "rejected" else "approval"
            for peer_id in peer_ids:
                learned.append(
                    self.set_offload_preference(
                        peer_id,
                        workload_class=workload_class,
                        preference=learned_pref,
                        source="approval_memory",
                        metadata={"operator_peer_id": operator_peer_id, "operator_agent_id": operator_agent_id},
                    )
                )
            return {"status": "learned", "preference": learned_pref, "preferences": learned}
        max_enlist = max(1, int(autonomy.get("max_auto_enlist") or 1))
        enlisted = []
        skipped = []
        for peer_id in peer_ids:
            if len(enlisted) >= max_enlist:
                break
            try:
                state = self.enlist_helper(
                    peer_id,
                    mode=str(autonomy.get("mode") or "on_demand"),
                    role=str(autonomy.get("role") or "helper"),
                    reason=str(reason or "approved_autonomous_offload"),
                    source="approval",
                )
                enlisted.append({"peer_id": peer_id, "state": state})
                self.set_offload_preference(
                    peer_id,
                    workload_class=workload_class,
                    preference="allow",
                    source="approval_memory",
                    metadata={"operator_peer_id": operator_peer_id, "operator_agent_id": operator_agent_id},
                )
            except Exception as exc:
                skipped.append({"peer_id": peer_id, "reason": str(exc)})
        if enlisted:
            self.publish_notification(
                notification_type="helper.autonomy.applied",
                priority="high",
                title="Autonomous offload applied",
                body=f"Enlisted {len(enlisted)} helper peer(s) after approval.",
                target_peer_id=self.node_id,
                target_device_classes=["full", "light", "micro"],
                related_approval_id=approval.get("id") or "",
                metadata={
                    "peer_ids": [item["peer_id"] for item in enlisted],
                    "operator_peer_id": operator_peer_id,
                    "operator_agent_id": operator_agent_id,
                },
            )
        return {
            "status": "applied" if enlisted else "noop",
            "enlisted": enlisted,
            "skipped": skipped,
        }

    def run_autonomous_offload(
        self,
        *,
        job: Optional[dict] = None,
        actor_agent_id: str = "ocp-autonomy",
    ) -> dict:
        evaluation = self.evaluate_autonomous_offload(job=job)
        decision = str(evaluation.get("decision") or "noop")
        policy = dict(evaluation.get("policy") or {})
        result: dict[str, Any] = {
            "peer_id": self.node_id,
            "evaluation": evaluation,
            "status": decision,
            "generated_at": _utcnow(),
        }
        if decision in {"noop", "suggest"}:
            return result
        if decision == "request_approval":
            eligible = list(evaluation.get("eligible_candidates") or [])
            request = self.create_approval_request(
                title="Approve autonomous helper offload",
                summary=f"Pressure is {evaluation.get('pressure', {}).get('pressure') or 'unknown'} and OCP wants to enlist {len(eligible)} helper peer(s).",
                action_type="autonomous.offload",
                severity="high" if str(evaluation.get("pressure", {}).get("pressure") or "") == "saturated" else "normal",
                request_id=self._autonomous_offload_request_id(evaluation),
                requested_by_peer_id=self.node_id,
                requested_by_agent_id=actor_agent_id,
                target_peer_id=self.node_id,
                target_device_classes=policy.get("target_device_classes") or ["full", "light", "micro"],
                metadata={
                    "autonomous_offload": {
                        "peer_ids": [item.get("peer_id") for item in eligible],
                        "mode": "on_demand",
                        "role": "gpu_helper" if any(dict(item.get("compute_profile") or {}).get("gpu_capable") for item in eligible) else "helper",
                        "pressure": evaluation.get("pressure"),
                        "placement": evaluation.get("placement"),
                        "workload_class": _normalize_workload_class(dict(evaluation.get("placement") or {}).get("workload_class") or "default"),
                        "max_auto_enlist": int(policy.get("max_auto_enlist") or 2),
                    }
                },
            )
            result["approval"] = request
            result["status"] = "approval_requested"
            return result
        auto_result = self.auto_seek_help(
            job=job,
            max_enlist=int(policy.get("max_auto_enlist") or 2),
            mode="on_demand",
            reason="policy_auto_offload",
            allow_remote_seek=bool(policy.get("allow_remote_seek")),
        )
        result["auto_seek"] = auto_result
        result["status"] = "auto_enlisted"
        if policy.get("notify_on_action") and (auto_result.get("enlisted") or []):
            self.publish_notification(
                notification_type="helper.autonomy.enlisted",
                priority="high",
                title="Autonomous helper offload active",
                body=f"Enlisted {len(auto_result.get('enlisted') or [])} helper peer(s) for local pressure relief.",
                target_peer_id=self.node_id,
                target_device_classes=policy.get("target_device_classes") or ["full", "light", "micro"],
                metadata={"peer_ids": [item.get("peer_id") for item in auto_result.get("enlisted") or []]},
            )
        return result

    def stream_snapshot(self, *, since_seq: int = 0, limit: int = 50) -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_events
                WHERE seq > ?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (max(0, int(since_seq)), max(1, int(limit))),
            ).fetchall()
        events = [self._row_to_event(row) for row in rows]
        return {
            "organism_id": self.node_id,
            "device_profile": dict(self.device_profile),
            "sync_policy": self._device_profile_sync_policy(self.device_profile),
            "transport": {
                "route": "/mesh/stream",
                "mode": "snapshot",
                "websocket_bootstrap": True,
            },
            "events": events,
            "next_cursor": (events[-1]["seq"] if events else since_seq),
            "agent_presence": self.export_agent_presence(limit=50),
            "beacons": self.export_beacons(limit=12),
            "workers": self.list_workers(limit=20)["workers"],
            "peers": self.list_peers(limit=25)["peers"],
            "queue_metrics": self.queue_metrics(),
            "generated_at": _utcnow(),
        }

    def _upsert_registry_lock(self, resource: str, peer_id: str, *, ttl_seconds: int, reason: str, lock_token: str) -> dict:
        if self.registry is None:
            return {"status": "skipped"}
        return self.registry.acquire_lock(
            f"mesh:{resource}",
            agent_id=peer_id,
            agent_name=peer_id,
            session_id=lock_token,
            reason=reason,
            ttl_seconds=ttl_seconds,
            lock_type="mesh_lease",
            metadata={"lock_token": lock_token},
        )

    def acquire_lease(
        self,
        *,
        peer_id: str,
        resource: str,
        agent_id: str = "",
        job_id: str = "",
        ttl_seconds: int = 300,
        metadata: Optional[dict] = None,
    ) -> dict:
        ttl = max(60, int(ttl_seconds))
        now = _utcnow_dt()
        lease_id = str(uuid.uuid4())
        lock_token = uuid.uuid4().hex
        expires_at = (now + dt.timedelta(seconds=ttl)).isoformat().replace("+00:00", "Z")
        registry_lock = self._upsert_registry_lock(
            resource,
            peer_id,
            ttl_seconds=ttl,
            reason="mesh lease",
            lock_token=lock_token,
        )
        payload = dict(metadata or {})
        if registry_lock.get("lock"):
            payload["registry_lock"] = registry_lock["lock"]
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_leases
                (id, resource, peer_id, agent_id, job_id, status, ttl_seconds, lock_token, metadata, created_at, heartbeat_at, expires_at)
                VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease_id,
                    resource,
                    peer_id,
                    agent_id,
                    job_id,
                    ttl,
                    lock_token,
                    json.dumps(payload),
                    now.isoformat().replace("+00:00", "Z"),
                    now.isoformat().replace("+00:00", "Z"),
                    expires_at,
                ),
            )
            row = conn.execute("SELECT * FROM mesh_leases WHERE id=?", (lease_id,)).fetchone()
            conn.commit()
        lease = self._row_to_lease(row)
        self._record_event(
            "mesh.lease.acquired",
            peer_id=peer_id,
            payload={"lease_id": lease_id, "resource": resource, "ttl_seconds": ttl},
        )
        return lease

    def _lease_row(self, lease_id: str):
        with self._conn() as conn:
            return conn.execute("SELECT * FROM mesh_leases WHERE id=?", ((lease_id or "").strip(),)).fetchone()

    def heartbeat_lease(self, lease_id: str, *, ttl_seconds: int = 300) -> dict:
        row = self._lease_row(lease_id)
        if row is None:
            raise MeshPolicyError("lease not found")
        ttl = max(60, int(ttl_seconds))
        expires_at = (_utcnow_dt() + dt.timedelta(seconds=ttl)).isoformat().replace("+00:00", "Z")
        metadata = _loads_json(row["metadata"], {})
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_leases
                SET heartbeat_at=?, expires_at=?, ttl_seconds=?, metadata=?
                WHERE id=?
                """,
                (_utcnow(), expires_at, ttl, json.dumps(metadata), lease_id),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_leases WHERE id=?", (lease_id,)).fetchone()
        if self.registry is not None:
            try:
                self.registry.heartbeat_lock(
                    f"mesh:{row['resource']}",
                    agent_id=row["peer_id"],
                    lock_token=row["lock_token"],
                    ttl_seconds=ttl,
                )
            except Exception:
                logger.debug("mesh registry heartbeat failed", exc_info=True)
        lease = self._row_to_lease(fresh)
        self._record_event(
            "mesh.lease.heartbeat",
            peer_id=lease["peer_id"],
            payload={"lease_id": lease_id, "resource": lease["resource"], "ttl_seconds": ttl},
        )
        return lease

    def release_lease(self, lease_id: str, *, status: str = "released") -> dict:
        row = self._lease_row(lease_id)
        if row is None:
            raise MeshPolicyError("lease not found")
        released_at = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_leases
                SET status=?, released_at=?, heartbeat_at=?
                WHERE id=?
                """,
                ((status or "released").strip().lower(), released_at, released_at, lease_id),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_leases WHERE id=?", (lease_id,)).fetchone()
        if self.registry is not None:
            try:
                self.registry.release_lock(
                    f"mesh:{row['resource']}",
                    agent_id=row["peer_id"],
                    lock_token=row["lock_token"],
                    force=True,
                )
            except Exception:
                logger.debug("mesh registry release failed", exc_info=True)
        lease = self._row_to_lease(fresh)
        self._record_event(
            "mesh.lease.released",
            peer_id=lease["peer_id"],
            payload={"lease_id": lease_id, "resource": lease["resource"], "status": lease["status"]},
        )
        return lease

    def accept_lease_request(self, envelope: dict, *, route: str) -> dict:
        peer_id, request_meta, body, peer = self._verify_envelope(envelope, route=route)
        resource = (body.get("resource") or "").strip()
        if not resource:
            raise MeshPolicyError("resource is required")
        ttl_seconds = int(body.get("ttl_seconds") or body.get("ttl") or 300)
        if route == "/mesh/lease/acquire":
            lease = self.acquire_lease(
                peer_id=peer_id,
                resource=resource,
                agent_id=(body.get("agent_id") or "").strip(),
                job_id=(body.get("job_id") or "").strip(),
                ttl_seconds=ttl_seconds,
                metadata={"request_id": request_meta.get("request_id"), "peer": peer or {}},
            )
            return {"status": "ok", "lease": lease}
        if route == "/mesh/lease/heartbeat":
            lease = self.heartbeat_lease((body.get("lease_id") or "").strip(), ttl_seconds=ttl_seconds)
            return {"status": "ok", "lease": lease}
        lease = self.release_lease((body.get("lease_id") or "").strip())
        return {"status": "ok", "lease": lease}

    def _artifact_path(self, artifact_id: str) -> Path:
        return self.artifact_root / f"{artifact_id}.blob"

    def _artifact_retention_policy(self, *, policy: Optional[dict], metadata: Optional[dict]) -> dict:
        artifact_metadata = dict(metadata or {})
        if self._artifact_is_pinned({"metadata": artifact_metadata}):
            return {
                "retention_class": "durable",
                "retention_seconds": 0,
                "retention_deadline_at": "",
            }
        artifact_kind = str(artifact_metadata.get("artifact_kind") or "").strip().lower()
        raw_retention = artifact_metadata.get("retention_class") or artifact_metadata.get("retention")
        if not raw_retention:
            raw_retention = (dict(policy or {}).get("retention") or "")
        if raw_retention:
            retention_class = _normalize_retention_class(str(raw_retention))
        elif artifact_kind == "log":
            retention_class = "session"
        else:
            retention_class = "durable"
        raw_seconds = artifact_metadata.get("retention_seconds")
        if raw_seconds in (None, ""):
            raw_seconds = dict(policy or {}).get("retention_seconds")
        if raw_seconds in (None, "") and isinstance(dict(policy or {}).get("retention"), (int, float)):
            raw_seconds = dict(policy or {}).get("retention")
        try:
            retention_seconds = max(0, int(raw_seconds))
        except Exception:
            retention_seconds = ARTIFACT_RETENTION_DEFAULTS.get(retention_class, 0)
        return {
            "retention_class": retention_class,
            "retention_seconds": retention_seconds,
            "retention_deadline_at": _utc_after(retention_seconds) if retention_seconds > 0 else "",
        }

    def _delete_artifact_row(self, row, *, reason: str = "retention_expired") -> None:
        artifact_id = row["id"]
        artifact_path = Path(row["path"])
        try:
            artifact_path.unlink(missing_ok=True)
        except TypeError:
            if artifact_path.exists():
                artifact_path.unlink()
        with self._conn() as conn:
            conn.execute("DELETE FROM mesh_artifacts WHERE id=?", (artifact_id,))
            conn.commit()
        self._record_event(
            "mesh.artifact.purged",
            peer_id=row["owner_peer_id"] or self.node_id,
            payload={
                "artifact_id": artifact_id,
                "digest": row["digest"] or "",
                "reason": reason,
                "retention_class": row["retention_class"] or "durable",
            },
        )

    def _artifact_metadata_dict(self, value: Any) -> dict:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value.get("metadata") or value) if "metadata" in value else dict(value)
        try:
            return _loads_json(value["metadata"], {})
        except Exception:
            return {}

    def _artifact_is_pinned(self, artifact_like: Any) -> bool:
        metadata = self._artifact_metadata_dict(artifact_like)
        artifact_sync = dict(metadata.get("artifact_sync") or {})
        return _coerce_bool(metadata.get("pinned")) or _coerce_bool(artifact_sync.get("pinned"))

    def _artifact_row(self, artifact_id: str):
        with self._conn() as conn:
            return conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", ((artifact_id or "").strip(),)).fetchone()

    def _update_artifact_record(
        self,
        artifact_id: str,
        *,
        metadata: Optional[dict] = None,
        retention_class: Optional[str] = None,
        retention_deadline_at: Optional[str] = None,
    ) -> dict:
        row = self._artifact_row(artifact_id)
        if row is None:
            raise MeshArtifactAccessError("artifact not found")
        merged_metadata = dict(_loads_json(row["metadata"], {}))
        merged_metadata.update(dict(metadata or {}))
        next_retention_class = retention_class if retention_class is not None else (row["retention_class"] or "durable")
        next_retention_deadline = retention_deadline_at if retention_deadline_at is not None else (row["retention_deadline_at"] or "")
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_artifacts
                SET metadata=?, retention_class=?, retention_deadline_at=?
                WHERE id=?
                """,
                (
                    json.dumps(merged_metadata),
                    next_retention_class,
                    next_retention_deadline,
                    (artifact_id or "").strip(),
                ),
            )
            conn.commit()
            updated_row = conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", ((artifact_id or "").strip(),)).fetchone()
        return self._row_to_artifact(updated_row)

    def _purge_expired_artifacts(self, *, limit: int = 100) -> int:
        now = _utcnow()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_artifacts
                WHERE retention_deadline_at != ''
                  AND retention_deadline_at <= ?
                ORDER BY retention_deadline_at ASC
                LIMIT ?
                """,
                (now, max(1, int(limit or 100))),
            ).fetchall()
        purged = 0
        for row in rows:
            if self._artifact_is_pinned(row):
                continue
            self._delete_artifact_row(row, reason="retention_expired")
            purged += 1
        return purged

    def _normalize_job_metadata(self, raw: Optional[dict]) -> dict:
        metadata = dict(raw or {})
        retry_policy = dict(metadata.get("retry_policy") or {})
        max_attempts = int(
            retry_policy.get("max_attempts")
            or metadata.get("max_attempts")
            or metadata.get("retries")
            or 1
        )
        retry_policy["max_attempts"] = max(1, max_attempts)
        metadata["retry_policy"] = retry_policy
        resumability = dict(metadata.get("resumability") or {})
        resumable = _coerce_bool(
            resumability.get("enabled")
            if "enabled" in resumability
            else metadata.get("resumable")
        )
        resumability["enabled"] = resumable
        resumability["mode"] = str(resumability.get("mode") or ("checkpoint" if resumable else "stateless")).strip().lower()
        resumability["max_resume_attempts"] = max(
            0,
            int(
                resumability.get("max_resume_attempts")
                or metadata.get("max_resume_attempts")
                or max(0, retry_policy["max_attempts"] - 1)
            ),
        )
        metadata["resumability"] = resumability
        checkpoint_policy = dict(metadata.get("checkpoint_policy") or {})
        checkpoint_policy["enabled"] = _coerce_bool(
            checkpoint_policy.get("enabled")
            if "enabled" in checkpoint_policy
            else resumable
        )
        checkpoint_policy["mode"] = str(checkpoint_policy.get("mode") or ("manual" if checkpoint_policy["enabled"] else "none")).strip().lower()
        checkpoint_policy["retention_class"] = _normalize_retention_class(
            checkpoint_policy.get("retention_class") or "durable"
        )
        checkpoint_policy["on_retry"] = _coerce_bool(
            checkpoint_policy.get("on_retry")
            if "on_retry" in checkpoint_policy
            else resumable
        )
        metadata["checkpoint_policy"] = checkpoint_policy
        if metadata.get("dispatch_mode"):
            metadata["dispatch_mode"] = str(metadata["dispatch_mode"]).strip().lower()
        return metadata

    def _normalize_runtime_environment(
        self,
        *,
        payload: Optional[dict] = None,
        runtime: Optional[dict] = None,
        metadata: Optional[dict] = None,
        policy: Optional[dict] = None,
        resources: Optional[dict] = None,
    ) -> dict:
        payload_data = dict(payload or {})
        runtime_data = dict(runtime or {})
        metadata_data = dict(metadata or {})
        policy_data = _normalize_policy(policy or {})
        resources_data = dict(resources or {})
        env_policy_raw = dict(
            runtime_data.get("env_policy")
            or payload_data.get("env_policy")
            or metadata_data.get("env_policy")
            or {}
        )
        env_policy = {
            "inherit_host_env": _coerce_bool(env_policy_raw.get("inherit_host_env") if "inherit_host_env" in env_policy_raw else True),
            "allow_env_override": _coerce_bool(env_policy_raw.get("allow_env_override") if "allow_env_override" in env_policy_raw else True),
        }
        filesystem_raw = dict(
            runtime_data.get("filesystem")
            or payload_data.get("filesystem")
            or metadata_data.get("filesystem")
            or {}
        )
        filesystem_profile = str(filesystem_raw.get("profile") or metadata_data.get("filesystem_profile") or "workspace").strip().lower()
        if filesystem_profile not in {"workspace", "isolated", "custom"}:
            filesystem_profile = "workspace"
        writable_paths = [str(item).strip() for item in (filesystem_raw.get("writable_paths") or []) if str(item).strip()]
        cwd = str(
            payload_data.get("cwd")
            or payload_data.get("working_dir")
            or runtime_data.get("cwd")
            or runtime_data.get("working_dir")
            or ""
        ).strip()
        secret_scopes = _normalize_secret_scopes(policy_data.get("secret_scopes") or metadata_data.get("secret_scopes") or [])
        raw_secrets = runtime_data.get("secrets")
        if raw_secrets is None:
            raw_secrets = payload_data.get("secrets")
        default_scope = secret_scopes[0] if len(secret_scopes) == 1 else ""
        allowed_secret_scopes = set(secret_scopes)
        secret_bindings = []
        if isinstance(raw_secrets, dict):
            for env_var, secret_value in raw_secrets.items():
                binding_env = _normalize_env_var_name(env_var)
                if not binding_env:
                    continue
                if binding_env.startswith("OCP_RESUME_"):
                    raise MeshPolicyError(f"secret binding cannot override reserved runtime env: {binding_env}")
                scope = default_scope
                required = True
                source = "inline"
                provider_name = ""
                provider_path = ""
                if isinstance(secret_value, dict):
                    source = _normalize_secret_source(secret_value.get("source") or "inline")
                    scope = str(secret_value.get("scope") or default_scope).strip()
                    required = _coerce_bool(secret_value.get("required") if "required" in secret_value else True)
                    provider_name = str(secret_value.get("name") or secret_value.get("env") or "").strip()
                    provider_path = str(secret_value.get("path") or secret_value.get("file") or "").strip()
                if secret_scopes and scope and scope not in allowed_secret_scopes:
                    raise MeshPolicyError(f"secret scope not allowed by policy: {scope}")
                if not secret_scopes:
                    raise MeshPolicyError("payload.secrets requires explicit policy.secret_scopes")
                binding = {
                    "env_var": binding_env,
                    "scope": scope,
                    "required": required,
                    "source": source,
                    "provider_ref": "inline",
                }
                if source == "env":
                    resolved_provider_name = _normalize_env_var_name(provider_name or binding_env)
                    binding["name"] = resolved_provider_name
                    binding["provider_ref"] = f"env:{resolved_provider_name}"
                elif source == "store":
                    resolved_provider_name = str(provider_name or binding_env.lower()).strip()
                    if not resolved_provider_name:
                        raise MeshPolicyError(f"store secret binding missing name: {binding_env}")
                    if not scope:
                        raise MeshPolicyError(f"store secret binding requires scope: {binding_env}")
                    binding["name"] = resolved_provider_name
                    binding["provider_ref"] = f"store:{scope}/{resolved_provider_name}"
                elif source == "file":
                    if not provider_path:
                        raise MeshPolicyError(f"file secret binding requires path: {binding_env}")
                    binding["path"] = provider_path
                    binding["provider_ref"] = f"file:{provider_path}"
                secret_bindings.append(binding)
        network_mode = str(
            (payload_data.get("network") or {}).get("mode")
            if isinstance(payload_data.get("network"), dict)
            else payload_data.get("network")
            or (
            (runtime_data.get("network") or {}).get("mode")
            if isinstance(runtime_data.get("network"), dict)
            else runtime_data.get("network")
            )
            or resources_data.get("network")
            or "default"
        ).strip().lower() or "default"
        return {
            "cwd": cwd,
            "env_policy": env_policy,
            "filesystem": {
                "profile": filesystem_profile,
                "workspace_root_required": filesystem_profile in {"workspace", "isolated"},
                "writable_paths": writable_paths,
            },
            "network": {"mode": network_mode},
            "secrets": {
                "delivery": "env" if secret_bindings else "none",
                "bindings": secret_bindings,
                "scope_count": len(secret_scopes),
                "provider_count": len(secret_bindings),
                "sources": sorted({str(binding.get("source") or "inline") for binding in secret_bindings}),
                "redacted": bool(secret_bindings),
            },
        }

    def _normalize_job_spec(
        self,
        job_body: dict,
        *,
        requirements: Optional[dict] = None,
        policy: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ) -> dict:
        job = dict(job_body or {})
        metadata = self._normalize_job_metadata(metadata if metadata is not None else job.get("metadata") or {})
        requirements = dict(requirements or job.get("requirements") or {})
        policy = _normalize_policy(policy if policy is not None else job.get("policy") or {})
        kind = str(job.get("kind") or "").strip().lower()
        dispatch_mode = self._job_dispatch_mode(kind, {**job, "metadata": metadata})
        payload = dict(job.get("payload") or {})
        runtime = dict(job.get("runtime") or {})
        env = {
            _normalize_env_var_name(k): str(v)
            for k, v in dict(payload.get("env") or runtime.get("env") or {}).items()
            if _normalize_env_var_name(k)
        }
        args = [str(item) for item in (payload.get("args") or runtime.get("args") or [])]
        artifact_inputs = list(job.get("artifact_inputs") or [])
        artifact_outputs = list((job.get("artifact_outputs") or metadata.get("artifact_outputs") or []))
        resources = _normalize_resources(
            requirements.get("resources")
            or metadata.get("resources")
            or runtime.get("resources")
            or payload.get("resources")
            or {}
        )
        timeout_seconds = int(
            payload.get("timeout_seconds")
            or runtime.get("timeout_seconds")
            or metadata.get("timeout_seconds")
            or 300
        )
        runtime_environment = self._normalize_runtime_environment(
            payload=payload,
            runtime=runtime,
            metadata=metadata,
            policy=policy,
            resources=resources,
        )
        runtime_type = ""
        execution: dict[str, Any]
        if kind == "shell.command":
            runtime_type = "shell"
            command = payload.get("command")
            if isinstance(command, list):
                argv = [str(part) for part in command]
            elif isinstance(command, str) and command.strip():
                argv = ["/bin/sh", "-lc", str(command)]
            else:
                argv = []
            execution = {
                "runtime_type": runtime_type,
                "command": argv,
                "cwd": runtime_environment["cwd"],
                "env": env,
                "timeout_seconds": max(1, timeout_seconds),
            }
        elif kind == "python.inline":
            runtime_type = "python"
            execution = {
                "runtime_type": runtime_type,
                "inline_code": str(payload.get("code") or runtime.get("inline_code") or "").strip(),
                "args": args,
                "cwd": runtime_environment["cwd"],
                "env": env,
                "timeout_seconds": max(1, timeout_seconds),
                "python_version": str(runtime.get("python_version") or metadata.get("python_version") or "").strip(),
                "dependencies": [str(item).strip() for item in (runtime.get("dependencies") or metadata.get("dependencies") or []) if str(item).strip()],
            }
        elif kind == "docker.container":
            command_raw = payload.get("command") or runtime.get("command") or []
            if isinstance(command_raw, list):
                command = [str(item) for item in command_raw]
            elif isinstance(command_raw, str) and command_raw.strip():
                command = ["/bin/sh", "-lc", str(command_raw)]
            else:
                command = []
            runtime_type = "container"
            execution = {
                "runtime_type": runtime_type,
                "image": str(payload.get("image") or runtime.get("image") or "").strip(),
                "command": command,
                "args": args,
                "env": env,
                "working_dir": runtime_environment["cwd"],
                "timeout_seconds": max(1, timeout_seconds),
            }
        elif kind == "wasm.component":
            runtime_type = "wasm"
            execution = {
                "runtime_type": runtime_type,
                "component_ref": dict(payload.get("component_ref") or runtime.get("component_ref") or {}),
                "entrypoint": str(payload.get("entrypoint") or runtime.get("entrypoint") or "").strip(),
                "args": args,
                "env": env,
                "working_dir": runtime_environment["cwd"],
                "timeout_seconds": max(1, timeout_seconds),
            }
        else:
            runtime_type = str(runtime.get("runtime_type") or "custom").strip().lower() or "custom"
            execution = {
                "runtime_type": runtime_type,
                "executor_kind": kind,
                "payload": payload,
                "timeout_seconds": max(1, timeout_seconds),
            }
        needs = {str(item).strip() for item in (requirements.get("capabilities") or []) if str(item).strip()}
        default_caps = {
            "shell.command": {"shell"},
            "python.inline": {"python"},
            "docker.container": {"docker"},
            "wasm.component": {"wasm"},
        }.get(kind, set())
        capabilities = sorted(needs | default_caps)
        normalized_requirements = {
            "capabilities": capabilities,
            "resources": resources,
            "placement": dict(job.get("placement") or metadata.get("placement") or {}),
        }
        retries = {
            "max_attempts": int(((metadata.get("retry_policy") or {}).get("max_attempts") or 1)),
        }
        resumability = dict(metadata.get("resumability") or {})
        checkpoint_policy = dict(metadata.get("checkpoint_policy") or {})
        status_model = self._job_status_model()
        provenance = {
            "origin_peer_id": str(job.get("origin") or "").strip(),
            "request_id": str(job.get("request_id") or "").strip(),
            "submitted_at": str(job.get("created_at") or "").strip(),
            "kind": kind,
        }
        return {
            "kind": kind,
            "dispatch_mode": dispatch_mode,
            "execution": execution,
            "requirements": normalized_requirements,
            "policy": {
                **policy,
                "secret_scopes": _normalize_secret_scopes(policy.get("secret_scopes") or metadata.get("secret_scopes") or []),
            },
            "runtime_environment": runtime_environment,
            "retries": retries,
            "artifacts": {
                "inputs": artifact_inputs,
                "outputs": artifact_outputs,
            },
            "checkpoints": {
                "enabled": bool(checkpoint_policy.get("enabled")),
                "mode": str(checkpoint_policy.get("mode") or "none"),
                "retention_class": checkpoint_policy.get("retention_class") or "durable",
                "on_retry": bool(checkpoint_policy.get("on_retry")),
            },
            "resumability": {
                "enabled": bool(resumability.get("enabled")),
                "mode": str(resumability.get("mode") or "stateless"),
                "max_resume_attempts": int(resumability.get("max_resume_attempts") or 0),
            },
            "provenance": provenance,
            "status_model": status_model,
        }

    def _validate_normalized_job_spec(self, spec: dict) -> None:
        kind = str(spec.get("kind") or "").strip().lower()
        execution = dict(spec.get("execution") or {})
        runtime_type = str(execution.get("runtime_type") or "").strip().lower()
        runtime_environment = dict(spec.get("runtime_environment") or {})
        resumability = dict(spec.get("resumability") or {})
        checkpoints = dict(spec.get("checkpoints") or {})
        if kind == "shell.command" and not list(execution.get("command") or []):
            raise MeshPolicyError("shell.command requires payload.command")
        if kind == "python.inline" and not str(execution.get("inline_code") or "").strip():
            raise MeshPolicyError("python.inline requires payload.code")
        if kind == "docker.container" and not str(execution.get("image") or "").strip():
            raise MeshPolicyError("docker.container requires payload.image")
        if kind == "wasm.component":
            component_ref = dict(execution.get("component_ref") or {})
            if not component_ref.get("id") and not component_ref.get("digest") and not component_ref.get("path"):
                raise MeshPolicyError("wasm.component requires payload.component_ref")
        if runtime_type not in {"shell", "python", "container", "wasm", "custom"}:
            raise MeshPolicyError("unsupported runtime_type")
        env_policy = dict(runtime_environment.get("env_policy") or {})
        if set(env_policy.keys()) - {"inherit_host_env", "allow_env_override"}:
            raise MeshPolicyError("unsupported env_policy field")
        filesystem = dict(runtime_environment.get("filesystem") or {})
        if str(filesystem.get("profile") or "workspace") not in {"workspace", "isolated", "custom"}:
            raise MeshPolicyError("unsupported filesystem profile")
        for binding in list((runtime_environment.get("secrets") or {}).get("bindings") or []):
            env_var = _normalize_env_var_name(binding.get("env_var"))
            if env_var.startswith("OCP_RESUME_"):
                raise MeshPolicyError(f"secret binding cannot override reserved runtime env: {env_var}")
            source = _normalize_secret_source(binding.get("source") or "inline")
            if source == "env":
                _normalize_env_var_name(binding.get("name") or env_var)
            elif source == "store":
                if not str(binding.get("scope") or "").strip():
                    raise MeshPolicyError(f"store secret binding requires scope: {env_var}")
                if not str(binding.get("name") or "").strip():
                    raise MeshPolicyError(f"store secret binding requires name: {env_var}")
            elif source == "file":
                if not str(binding.get("path") or "").strip():
                    raise MeshPolicyError(f"file secret binding requires path: {env_var}")
        if bool(resumability.get("enabled")) and str(resumability.get("mode") or "checkpoint") not in {"checkpoint"}:
            raise MeshPolicyError("unsupported resumability mode")
        if bool(checkpoints.get("enabled")) and str(checkpoints.get("mode") or "manual") not in {"manual", "automatic"}:
            raise MeshPolicyError("unsupported checkpoint mode")

    def _job_status_model(self) -> dict[str, list[str]]:
        return {
            "states": [
                "queued",
                "running",
                "resuming",
                "checkpointed",
                "retry_wait",
                "completed",
                "failed",
                "cancelled",
                "rejected",
            ],
            "active_states": ["running", "resuming"],
            "terminal_states": ["completed", "failed", "cancelled", "rejected"],
            "retryable_states": ["queued", "retry_wait", "running", "resuming"],
            "recovery_states": ["checkpointed", "retry_wait", "resuming"],
            "failure_states": ["checkpointed", "failed"],
        }

    def _artifact_ref(self, artifact: dict) -> dict:
        return {
            "id": artifact.get("id") or "",
            "digest": artifact.get("digest") or "",
            "media_type": artifact.get("media_type") or "application/octet-stream",
            "size_bytes": int(artifact.get("size_bytes") or 0),
            "path": artifact.get("path") or "",
            "download_url": artifact.get("download_url") or "",
        }

    def _oci_descriptor(
        self,
        ref: dict,
        *,
        annotations: Optional[dict] = None,
        media_type: str = "",
    ) -> dict:
        descriptor = {
            "mediaType": media_type or ref.get("media_type") or "application/octet-stream",
            "digest": _oci_digest(ref.get("digest") or ""),
            "size": int(ref.get("size_bytes") or 0),
            "annotations": dict(annotations or {}),
        }
        if ref.get("download_url"):
            descriptor["urls"] = [str(ref.get("download_url") or "")]
        return descriptor

    def _artifact_descriptor_from_input(self, item: dict) -> dict:
        ref = dict(item or {})
        artifact_id = str(ref.get("id") or "").strip()
        artifact = {}
        if artifact_id:
            try:
                artifact = self.get_artifact(artifact_id, include_content=False)
            except Exception:
                artifact = {}
        merged = {
            "id": artifact.get("id") or artifact_id,
            "digest": artifact.get("digest") or ref.get("digest") or "",
            "media_type": artifact.get("media_type") or ref.get("media_type") or "application/octet-stream",
            "size_bytes": int(artifact.get("size_bytes") or ref.get("size_bytes") or 0),
            "download_url": artifact.get("download_url") or ref.get("download_url") or "",
        }
        role = str(ref.get("role") or ref.get("name") or "").strip()
        annotations = dict(ref.get("annotations") or {})
        return self._artifact_descriptor(merged, role=role, annotations=annotations)

    def _job_recovery_contract(
        self,
        job: dict,
        *,
        metadata: Optional[dict] = None,
        spec: Optional[dict] = None,
    ) -> dict:
        status_model = self._job_status_model()
        job_metadata = self._normalize_job_metadata(metadata if metadata is not None else job.get("metadata") or {})
        job_spec = dict(spec or job.get("spec") or job_metadata.get("job_spec") or {})
        resumability = dict(job_spec.get("resumability") or job_metadata.get("resumability") or {})
        sync_resilience = self._job_sync_resilience(job, metadata=job_metadata, spec=job_spec)
        latest_checkpoint_ref = dict(job_metadata.get("latest_checkpoint_ref") or {})
        selected_resume_checkpoint_ref = dict(job_metadata.get("resume_checkpoint_ref") or {})
        status = str((job.get("status") if isinstance(job, dict) else "") or "").strip().lower() or "queued"
        checkpoint_available = bool(latest_checkpoint_ref.get("id"))
        resumable = bool(resumability.get("enabled")) and checkpoint_available
        return {
            "state": status,
            "states": list(status_model["states"]),
            "terminal": status in set(status_model["terminal_states"]),
            "resumable": resumable,
            "resumability_enabled": bool(resumability.get("enabled")),
            "checkpoint_enabled": bool(sync_resilience["checkpoint_enabled"]),
            "checkpoint_on_retry": bool(sync_resilience["checkpoint_on_retry"]),
            "checkpoint_available": checkpoint_available,
            "latest_checkpoint_ref": latest_checkpoint_ref,
            "selected_resume_checkpoint_ref": selected_resume_checkpoint_ref,
            "resume_count": int(job_metadata.get("resume_count") or 0),
            "checkpointed_at": str(job_metadata.get("checkpointed_at") or ""),
            "last_resumed_at": str(job_metadata.get("last_resumed_at") or ""),
            "last_resumed_by": str(job_metadata.get("last_resumed_by") or ""),
            "last_resume_reason": str(job_metadata.get("last_resume_reason") or ""),
            "last_resume_requested_at": str(job_metadata.get("last_resume_requested_at") or ""),
            "last_resume_requested_by": str(job_metadata.get("last_resume_requested_by") or ""),
            "last_resume_requested_reason": str(job_metadata.get("last_resume_requested_reason") or ""),
            "last_restart_at": str(job_metadata.get("last_restart_at") or ""),
            "last_restart_by": str(job_metadata.get("last_restart_by") or ""),
            "last_restart_reason": str(job_metadata.get("last_restart_reason") or ""),
            "last_recovery_action": str(job_metadata.get("last_recovery_action") or ""),
            "last_recovery_at": str(job_metadata.get("last_recovery_at") or ""),
            "last_recovery_by": str(job_metadata.get("last_recovery_by") or ""),
            "last_recovery_reason": str(job_metadata.get("last_recovery_reason") or ""),
            "recovery_hint": dict(job_metadata.get("recovery_hint") or {}),
        }

    def _job_dispatch_mode(self, kind: str, job_body: dict) -> str:
        metadata = self._normalize_job_metadata(job_body.get("metadata") or {})
        dispatch_mode = (
            job_body.get("dispatch_mode")
            or metadata.get("dispatch_mode")
            or ("queued" if kind in {"shell.command", "python.inline"} else "inline")
        )
        token = str(dispatch_mode or "inline").strip().lower()
        if token not in {"inline", "queued"}:
            token = "inline"
        return token

    def _job_resume_checkpoint_ref(self, job: dict) -> dict:
        metadata = self._normalize_job_metadata(job.get("metadata") or {})
        spec = dict(job.get("spec") or metadata.get("job_spec") or {})
        resumability = dict(spec.get("resumability") or metadata.get("resumability") or {})
        if not resumability.get("enabled"):
            return {}
        if _coerce_bool(metadata.get("restart_from_scratch")):
            return {}
        checkpoint_ref = dict(metadata.get("resume_checkpoint_ref") or metadata.get("latest_checkpoint_ref") or {})
        if not checkpoint_ref.get("id"):
            return {}
        return checkpoint_ref

    def _publish_attempt_checkpoint(self, job: dict, *, attempt_id: str, checkpoint_payload: Any, metadata: Optional[dict] = None) -> dict:
        checkpoint_metadata = {
            "artifact_kind": "checkpoint",
            "job_id": job["id"],
            "attempt_id": attempt_id,
            "retention_class": (
                (self._normalize_job_metadata(job.get("metadata") or {}).get("checkpoint_policy") or {}).get("retention_class")
                or "durable"
            ),
            **dict(metadata or {}),
        }
        return self.publish_local_artifact(
            checkpoint_payload,
            media_type="application/json",
            policy=job["policy"],
            metadata=checkpoint_metadata,
        )

    def _attempt_row(self, attempt_id: str):
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM mesh_job_attempts WHERE id=?",
                ((attempt_id or "").strip(),),
            ).fetchone()

    def _list_attempt_rows(self, job_id: str):
        with self._conn() as conn:
            return conn.execute(
                """
                SELECT * FROM mesh_job_attempts
                WHERE job_id=?
                ORDER BY attempt_number ASC
                """,
                ((job_id or "").strip(),),
            ).fetchall()

    def _next_attempt_number(self, job_id: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(attempt_number), 0) AS max_attempt FROM mesh_job_attempts WHERE job_id=?",
                ((job_id or "").strip(),),
            ).fetchone()
        return int((row["max_attempt"] if row is not None else 0) or 0) + 1

    def _get_worker_row(self, worker_id: str):
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM mesh_workers WHERE id=?",
                ((worker_id or "").strip(),),
            ).fetchone()

    def _queue_policy_for_job(self, job_body: dict, metadata: dict, queue_name: str) -> dict:
        raw = dict(metadata.get("queue_policy") or {})
        ack_deadline_seconds = int(
            raw.get("ack_deadline_seconds")
            or metadata.get("ack_deadline_seconds")
            or job_body.get("ack_deadline_seconds")
            or 300
        )
        retention_seconds = int(
            raw.get("retention_seconds")
            or metadata.get("retention_seconds")
            or job_body.get("retention_seconds")
            or 604800
        )
        replay_window_seconds = int(
            raw.get("replay_window_seconds")
            or metadata.get("replay_window_seconds")
            or job_body.get("replay_window_seconds")
            or 86400
        )
        dead_letter_queue = str(
            raw.get("dead_letter_queue")
            or metadata.get("dead_letter_queue")
            or job_body.get("dead_letter_queue")
            or f"{queue_name}.dlq"
        ).strip() or f"{queue_name}.dlq"
        return {
            "ack_deadline_seconds": max(60, ack_deadline_seconds),
            "retention_seconds": max(300, retention_seconds),
            "replay_window_seconds": max(0, replay_window_seconds),
            "dead_letter_queue": dead_letter_queue,
        }

    def _queue_policy_for_message(self, queue_message: Optional[dict]) -> dict:
        message = dict(queue_message or {})
        metadata = dict(message.get("metadata") or {})
        raw = dict(metadata.get("queue_policy") or {})
        queue_name = str(metadata.get("origin_queue_name") or message.get("queue_name") or "default").strip() or "default"
        return {
            "ack_deadline_seconds": max(
                60,
                int(message.get("ack_deadline_seconds") or raw.get("ack_deadline_seconds") or 300),
            ),
            "retention_seconds": max(
                300,
                int(raw.get("retention_seconds") or metadata.get("retention_seconds") or 604800),
            ),
            "replay_window_seconds": max(
                0,
                int(raw.get("replay_window_seconds") or metadata.get("replay_window_seconds") or 86400),
            ),
            "dead_letter_queue": str(
                message.get("dead_letter_queue")
                or raw.get("dead_letter_queue")
                or metadata.get("dead_letter_queue")
                or f"{queue_name}.dlq"
            ).strip()
            or f"{queue_name}.dlq",
        }

    def _queue_name_for_job(self, job_body: dict, metadata: dict) -> str:
        queue_name = (
            job_body.get("queue")
            or metadata.get("queue_name")
            or ((job_body.get("placement") or {}).get("queue_class"))
            or "default"
        )
        token = str(queue_name or "default").strip().lower()
        return token or "default"

    def _dedupe_key_for_job(self, job_body: dict, metadata: dict) -> str:
        return str(job_body.get("dedupe_key") or metadata.get("dedupe_key") or "").strip()

    def _queue_row_for_job(self, job_id: str):
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM mesh_queue_messages WHERE job_id=?",
                ((job_id or "").strip(),),
            ).fetchone()

    def _find_queued_job_by_dedupe_key(self, dedupe_key: str, *, queue_name: str = "default") -> Optional[dict]:
        token = str(dedupe_key or "").strip()
        if not token:
            return None
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT job_id
                FROM mesh_queue_messages
                WHERE dedupe_key=? AND queue_name=? AND status NOT IN ('cancelled', 'dead_letter')
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """,
                (token, queue_name),
            ).fetchone()
        if row is None:
            return None
        try:
            return self.get_job(row["job_id"])
        except Exception:
            return None

    def _create_queue_message(
        self,
        *,
        job_id: str,
        queue_name: str,
        dedupe_key: str = "",
        queue_policy: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ) -> dict:
        now = _utcnow()
        queue_message_id = str(uuid.uuid4())
        policy = dict(queue_policy or {})
        queue_metadata = dict(metadata or {})
        queue_metadata.setdefault("origin_queue_name", queue_name)
        queue_metadata["queue_policy"] = {
            "ack_deadline_seconds": int(policy.get("ack_deadline_seconds") or 300),
            "retention_seconds": int(policy.get("retention_seconds") or 604800),
            "replay_window_seconds": int(policy.get("replay_window_seconds") or 86400),
            "dead_letter_queue": str(policy.get("dead_letter_queue") or f"{queue_name}.dlq"),
        }
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_queue_messages
                (id, job_id, queue_name, status, dedupe_key, ack_deadline_seconds, dead_letter_queue, delivery_attempts,
                 visibility_timeout_at, available_at, claimed_at, acked_at, replay_deadline_at, retention_deadline_at,
                 lease_id, worker_id, current_attempt_id, last_error, metadata, created_at, updated_at)
                VALUES (?, ?, ?, 'queued', ?, ?, ?, 0, '', ?, '', '', '', '', '', '', '', '', ?, ?, ?)
                """,
                (
                    queue_message_id,
                    job_id,
                    queue_name,
                    (dedupe_key or "").strip(),
                    int(policy.get("ack_deadline_seconds") or 300),
                    str(policy.get("dead_letter_queue") or f"{queue_name}.dlq"),
                    now,
                    json.dumps(queue_metadata),
                    now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_queue_messages WHERE id=?", (queue_message_id,)).fetchone()
        return self._row_to_queue_message(row)

    def _queue_message_for_job(self, job_id: str) -> dict:
        row = self._queue_row_for_job(job_id)
        return self._row_to_queue_message(row) if row is not None else {}

    def _queue_message_for_attempt(self, attempt_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mesh_queue_messages WHERE current_attempt_id=?",
                ((attempt_id or "").strip(),),
            ).fetchone()
        return self._row_to_queue_message(row) if row is not None else {}

    def _ensure_queue_message_for_job(self, job: dict) -> dict:
        queue_message = self._queue_message_for_job(job["id"])
        if queue_message:
            return queue_message
        metadata = self._normalize_job_metadata(job.get("metadata") or {})
        queue_name = str(metadata.get("queue_name") or "default").strip().lower() or "default"
        queue_policy = self._queue_policy_for_job({}, metadata, queue_name)
        return self._create_queue_message(
            job_id=job["id"],
            queue_name=queue_name,
            dedupe_key=str(metadata.get("dedupe_key") or "").strip(),
            queue_policy=queue_policy,
            metadata={
                "request_id": job.get("request_id") or "",
                "kind": job.get("kind") or "",
                "origin_peer_id": job.get("origin") or "",
            },
        )

    def _resolve_checkpoint_artifact(self, job: dict, *, checkpoint_artifact_id: str = "") -> dict:
        artifact_id = (checkpoint_artifact_id or "").strip()
        if not artifact_id:
            artifact_id = str((job.get("latest_checkpoint_ref") or {}).get("id") or "").strip()
        if not artifact_id:
            raise MeshPolicyError("job has no checkpoint artifact")
        try:
            artifact = self.get_artifact(artifact_id, include_content=False)
        except Exception as exc:
            raise MeshPolicyError("checkpoint artifact not found") from exc
        if (artifact.get("artifact_kind") or "").strip().lower() != "checkpoint":
            raise MeshPolicyError("artifact is not a checkpoint")
        artifact_metadata = dict(artifact.get("metadata") or {})
        if (artifact_metadata.get("job_id") or "").strip() != job["id"]:
            raise MeshPolicyError("checkpoint artifact does not belong to job")
        return artifact

    def _recover_job(
        self,
        job_id: str,
        *,
        checkpoint_artifact_id: str = "",
        operator_id: str = "",
        reason: str,
        restart: bool = False,
    ) -> dict:
        job = self.get_job(job_id)
        if job["status"] in {"running", "resuming"}:
            raise MeshPolicyError("running jobs cannot be recovered")
        if job["status"] in {"completed", "rejected"}:
            raise MeshPolicyError("completed or rejected jobs cannot be recovered")
        metadata = dict(job.get("metadata") or {})
        recovery = self._job_recovery_contract(job, metadata=metadata, spec=job.get("spec") or {})
        selected_checkpoint_ref = {}
        action = "restart" if restart else "resume_latest"
        if restart:
            metadata.pop("resume_checkpoint_ref", None)
            metadata["restart_from_scratch"] = True
        else:
            if not recovery["resumability_enabled"]:
                raise MeshPolicyError("job is not resumable")
            checkpoint_artifact = self._resolve_checkpoint_artifact(job, checkpoint_artifact_id=checkpoint_artifact_id)
            selected_checkpoint_ref = self._artifact_ref(checkpoint_artifact)
            action = "resume_checkpoint" if checkpoint_artifact_id else "resume_latest"
            metadata["resume_checkpoint_ref"] = selected_checkpoint_ref
            metadata["restart_from_scratch"] = False
            metadata["last_resume_requested_at"] = _utcnow()
            metadata["last_resume_requested_by"] = str(operator_id or "")
            metadata["last_resume_requested_reason"] = str(reason)
        queue_message = self._ensure_queue_message_for_job(job)
        if queue_message.get("status") == "inflight":
            raise MeshPolicyError("inflight jobs cannot be recovered")
        queue_metadata = dict(queue_message.get("metadata") or {})
        queue_name = str(queue_metadata.get("origin_queue_name") or metadata.get("queue_name") or queue_message.get("queue_name") or "default").strip() or "default"
        queue_policy = self._queue_policy_for_job({}, metadata, queue_name)
        now = _utcnow()
        queue_metadata["recovery_count"] = int(queue_metadata.get("recovery_count") or 0) + 1
        queue_metadata["last_recovery_action"] = action
        queue_metadata["last_recovery_reason"] = str(reason)
        metadata["current_attempt_id"] = ""
        metadata["last_recovery_action"] = action
        metadata["last_recovery_at"] = now
        metadata["last_recovery_by"] = str(operator_id or "")
        metadata["last_recovery_reason"] = str(reason)
        metadata["retry_scheduled_at"] = now
        if restart:
            metadata["last_restart_at"] = now
            metadata["last_restart_by"] = str(operator_id or "")
            metadata["last_restart_reason"] = str(reason)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_jobs
                SET status='retry_wait', lease_id='', executor='', result_ref='{}', metadata=?, updated_at=?
                WHERE id=?
                """,
                (json.dumps(metadata), now, job["id"]),
            )
            conn.execute(
                """
                UPDATE mesh_queue_messages
                SET status='queued', queue_name=?, dead_letter_queue=?, visibility_timeout_at='', available_at=?,
                    claimed_at='', acked_at='', replay_deadline_at='', retention_deadline_at='', lease_id='',
                    worker_id='', current_attempt_id='', last_error='', metadata=?, updated_at=?
                WHERE id=?
                """,
                (
                    queue_name,
                    str(queue_policy["dead_letter_queue"]),
                    now,
                    json.dumps(queue_metadata),
                    now,
                    queue_message["id"],
                ),
            )
            conn.commit()
        recovered_job = self.get_job(job["id"])
        recovered_queue = self._queue_message_for_job(job["id"])
        event_type = "mesh.job.restarted" if restart else "mesh.job.resume_requested"
        self._record_event(
            event_type,
            peer_id=self.node_id,
            request_id=recovered_job["request_id"],
            payload={
                "job_id": recovered_job["id"],
                "queue_message_id": recovered_queue.get("id", ""),
                "checkpoint_artifact_id": selected_checkpoint_ref.get("id", ""),
                "operator_id": str(operator_id or ""),
                "reason": str(reason),
                "action": action,
            },
        )
        self._record_event(
            "mesh.queue.recovered",
            peer_id=self.node_id,
            request_id=recovered_job["request_id"],
            payload={
                "job_id": recovered_job["id"],
                "queue_message_id": recovered_queue.get("id", ""),
                "action": action,
            },
        )
        return {"status": "retry_wait", "job": recovered_job, "queue_message": recovered_queue}

    def resume_job(self, job_id: str, *, operator_id: str = "", reason: str = "operator_resume_latest") -> dict:
        return self._recover_job(job_id, operator_id=operator_id, reason=reason, restart=False)

    def resume_job_from_checkpoint(
        self,
        job_id: str,
        *,
        checkpoint_artifact_id: str,
        operator_id: str = "",
        reason: str = "operator_resume_checkpoint",
    ) -> dict:
        return self._recover_job(
            job_id,
            checkpoint_artifact_id=checkpoint_artifact_id,
            operator_id=operator_id,
            reason=reason,
            restart=False,
        )

    def restart_job(self, job_id: str, *, operator_id: str = "", reason: str = "operator_restart") -> dict:
        return self._recover_job(job_id, operator_id=operator_id, reason=reason, restart=True)

    def _purge_retained_queue_messages(self, *, limit: int = 100) -> int:
        now = _utcnow()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_queue_messages
                WHERE status IN ('acked', 'dead_letter', 'cancelled')
                  AND retention_deadline_at != ''
                  AND retention_deadline_at <= ?
                ORDER BY retention_deadline_at ASC
                LIMIT ?
                """,
                (now, max(1, int(limit))),
            ).fetchall()
        purged = 0
        for row in rows:
            queue_message = self._row_to_queue_message(row)
            with self._conn() as conn:
                conn.execute("DELETE FROM mesh_queue_messages WHERE id=?", (queue_message["id"],))
                conn.commit()
            self._record_event(
                "mesh.queue.retention_purged",
                peer_id=self.node_id,
                request_id="",
                payload={
                    "job_id": queue_message["job_id"],
                    "queue_message_id": queue_message["id"],
                    "status": queue_message["status"],
                },
            )
            purged += 1
        return purged

    def _requeue_expired_queue_messages(self, *, limit: int = 50) -> int:
        now = _utcnow()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_queue_messages
                WHERE status='inflight' AND visibility_timeout_at != '' AND visibility_timeout_at <= ?
                ORDER BY visibility_timeout_at ASC
                LIMIT ?
                """,
                (now, max(1, int(limit))),
            ).fetchall()
        processed = 0
        for row in rows:
            queue_message = self._row_to_queue_message(row)
            job_id = queue_message["job_id"]
            attempt_id = queue_message.get("current_attempt_id") or ""
            lease_id = queue_message.get("lease_id") or ""
            if lease_id:
                try:
                    self.release_lease(lease_id, status="expired")
                except Exception:
                    logger.debug("mesh queue lease expiry release failed", exc_info=True)
            with self._conn() as conn:
                if attempt_id:
                    attempt_row = conn.execute(
                        "SELECT metadata FROM mesh_job_attempts WHERE id=?",
                        (attempt_id,),
                    ).fetchone()
                    conn.execute(
                        """
                        UPDATE mesh_job_attempts
                        SET status='expired', error=?, heartbeat_at=?, finished_at=?, metadata=?
                        WHERE id=? AND status IN ('claimed', 'running')
                        """,
                        (
                            "visibility timeout expired",
                            now,
                            now,
                            json.dumps(
                                {
                                    **_loads_json(attempt_row["metadata"] if attempt_row is not None else "{}", {}),
                                    "queue_timeout": True,
                                }
                            ),
                            attempt_id,
                        ),
                    )
                job_row = conn.execute("SELECT metadata, status FROM mesh_jobs WHERE id=?", (job_id,)).fetchone()
                if job_row is not None and (job_row["status"] or "").strip() not in {"completed", "failed", "cancelled", "rejected"}:
                    job_metadata = _loads_json(job_row["metadata"], {})
                    job_metadata["queue_requeued_at"] = now
                    job_metadata["last_error"] = "visibility timeout expired"
                    job_metadata["current_attempt_id"] = ""
                    requeued_status = "retry_wait" if dict(job_metadata.get("resume_checkpoint_ref") or {}) else "queued"
                    conn.execute(
                        """
                        UPDATE mesh_jobs
                        SET status=?, lease_id='', metadata=?, updated_at=?
                        WHERE id=?
                        """,
                        (requeued_status, json.dumps(job_metadata), now, job_id),
                    )
                conn.execute(
                    """
                    UPDATE mesh_queue_messages
                    SET status='queued', visibility_timeout_at='', available_at=?, claimed_at='', lease_id='',
                        worker_id='', current_attempt_id='', last_error=?, updated_at=?
                    WHERE id=?
                    """,
                    (now, "visibility timeout expired", now, queue_message["id"]),
                )
                conn.commit()
            self._record_event(
                "mesh.queue.redelivered",
                peer_id=self.node_id,
                request_id="",
                payload={"job_id": job_id, "attempt_id": attempt_id, "queue_message_id": queue_message["id"]},
            )
            processed += 1
        return processed

    def list_queue_messages(self, *, limit: int = 25, status: str = "") -> dict:
        self._purge_retained_queue_messages(limit=max(10, int(limit or 25)))
        self._requeue_expired_queue_messages(limit=max(5, int(limit or 25)))
        query = [
            "SELECT * FROM mesh_queue_messages",
        ]
        params: list[Any] = []
        status_token = str(status or "").strip().lower()
        if status_token:
            query.append("WHERE status=?")
            params.append(status_token)
        query.append("ORDER BY updated_at DESC, created_at DESC LIMIT ?")
        params.append(max(1, int(limit or 25)))
        with self._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(params)).fetchall()
        messages = [self._row_to_queue_message(row) for row in rows]
        return {"peer_id": self.node_id, "count": len(messages), "messages": messages}

    def list_queue_events(
        self,
        *,
        since_seq: int = 0,
        limit: int = 50,
        queue_message_id: str = "",
        job_id: str = "",
    ) -> dict:
        self._purge_retained_queue_messages(limit=max(20, int(limit or 50)))
        self._requeue_expired_queue_messages(limit=max(10, int(limit or 50)))
        scan_limit = max(max(1, int(limit or 50)) * 20, 100)
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_events
                WHERE seq > ? AND event_type LIKE 'mesh.queue.%'
                ORDER BY seq ASC
                LIMIT ?
                """,
                (max(0, int(since_seq or 0)), scan_limit),
            ).fetchall()
        events = []
        queue_message_token = (queue_message_id or "").strip()
        job_token = (job_id or "").strip()
        for row in rows:
            event = self._row_to_event(row)
            payload = dict(event.get("payload") or {})
            if queue_message_token and (payload.get("queue_message_id") or "") != queue_message_token:
                continue
            if job_token and (payload.get("job_id") or "") != job_token:
                continue
            events.append(event)
            if len(events) >= max(1, int(limit or 50)):
                break
        next_cursor = int(events[-1]["seq"]) if events else max(0, int(since_seq or 0))
        return {
            "peer_id": self.node_id,
            "count": len(events),
            "events": events,
            "next_cursor": next_cursor,
            "filters": {"queue_message_id": queue_message_token, "job_id": job_token},
        }

    def queue_metrics(self) -> dict:
        self._purge_retained_queue_messages(limit=200)
        self._requeue_expired_queue_messages(limit=100)
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT queue_name, status, COUNT(*) AS item_count,
                       SUM(CASE WHEN delivery_attempts > 1 THEN 1 ELSE 0 END) AS redelivery_count
                FROM mesh_queue_messages
                GROUP BY queue_name, status
                ORDER BY queue_name ASC, status ASC
                """
            ).fetchall()
            oldest_row = conn.execute(
                """
                SELECT MIN(available_at) AS oldest_available_at
                FROM mesh_queue_messages
                WHERE status='queued'
                """
            ).fetchone()
        counts = {
            "queued": 0,
            "inflight": 0,
            "acked": 0,
            "dead_letter": 0,
            "cancelled": 0,
        }
        queues: dict[str, dict[str, Any]] = {}
        redelivery_total = 0
        for row in rows:
            queue_name = row["queue_name"] or "default"
            status = row["status"] or "queued"
            item_count = int(row["item_count"] or 0)
            redelivery_count = int(row["redelivery_count"] or 0)
            counts[status] = counts.get(status, 0) + item_count
            redelivery_total += redelivery_count
            bucket = queues.setdefault(
                queue_name,
                {
                    "queue_name": queue_name,
                    "total": 0,
                    "queued": 0,
                    "inflight": 0,
                    "acked": 0,
                    "dead_letter": 0,
                    "cancelled": 0,
                },
            )
            bucket["total"] += item_count
            bucket[status] = bucket.get(status, 0) + item_count
        workers = self.list_workers(limit=200)["workers"]
        total_slots = sum(max(1, int(worker.get("max_concurrent_jobs") or 1)) for worker in workers if worker.get("status") in {"active", "ready"})
        active_attempts = sum(int(worker.get("active_attempts") or 0) for worker in workers if worker.get("status") in {"active", "ready"})
        available_slots = max(0, total_slots - active_attempts)
        queued = counts.get("queued", 0)
        inflight = counts.get("inflight", 0)
        if queued <= 0 and inflight <= 0:
            pressure = "idle"
            scheduler_penalty = 0
        elif total_slots <= 0 and queued > 0:
            pressure = "saturated"
            scheduler_penalty = 180
        elif queued > max(1, total_slots * 3):
            pressure = "saturated"
            scheduler_penalty = 180
        elif queued > max(1, available_slots):
            pressure = "elevated"
            scheduler_penalty = 90
        else:
            pressure = "nominal"
            scheduler_penalty = 0
        backlog_ratio = round(queued / max(1, total_slots), 2) if total_slots > 0 else None
        return {
            "peer_id": self.node_id,
            "counts": counts,
            "queues": list(queues.values()),
            "workers": {
                "registered": len(workers),
                "total_slots": total_slots,
                "active_attempts": active_attempts,
                "available_slots": available_slots,
            },
            "pressure": pressure,
            "backlog_ratio": backlog_ratio,
            "scheduler_penalty": scheduler_penalty,
            "oldest_queued_at": (oldest_row["oldest_available_at"] if oldest_row is not None else "") or "",
            "redelivery_count": redelivery_total,
        }

    def replay_queue_message(
        self,
        *,
        queue_message_id: str = "",
        job_id: str = "",
        reason: str = "operator_replay",
    ) -> dict:
        queue_message_token = (queue_message_id or "").strip()
        job_token = (job_id or "").strip()
        if not queue_message_token and not job_token:
            raise MeshPolicyError("queue_message_id or job_id is required")
        with self._conn() as conn:
            if queue_message_token:
                row = conn.execute("SELECT * FROM mesh_queue_messages WHERE id=?", (queue_message_token,)).fetchone()
            else:
                row = conn.execute("SELECT * FROM mesh_queue_messages WHERE job_id=?", (job_token,)).fetchone()
        if row is None:
            raise MeshPolicyError("queue message not found")
        queue_message = self._row_to_queue_message(row)
        if queue_message["status"] not in {"dead_letter", "cancelled"}:
            raise MeshPolicyError("only dead_letter or cancelled queue messages may be replayed")
        now = _utcnow()
        replay_deadline_at = (queue_message.get("replay_deadline_at") or "").strip()
        if replay_deadline_at and replay_deadline_at <= now:
            raise MeshPolicyError("queue replay window has expired")
        job = self.get_job(queue_message["job_id"])
        if job["status"] == "checkpointed":
            raise MeshPolicyError("checkpointed jobs require /resume or /restart recovery controls")
        if job["status"] in {"completed", "rejected"}:
            raise MeshPolicyError("completed or rejected jobs cannot be replayed")
        job_metadata = dict(job.get("metadata") or {})
        job_metadata["replayed_at"] = now
        job_metadata["replay_reason"] = str(reason)
        job_metadata["current_attempt_id"] = ""
        queue_metadata = dict(queue_message.get("metadata") or {})
        queue_metadata["replayed_at"] = now
        queue_metadata["replay_reason"] = str(reason)
        queue_metadata["replay_count"] = int(queue_metadata.get("replay_count") or 0) + 1
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_jobs
                SET status='queued', lease_id='', result_ref='{}', metadata=?, updated_at=?
                WHERE id=?
                """,
                (json.dumps(job_metadata), now, job["id"]),
            )
            conn.execute(
                """
                UPDATE mesh_queue_messages
                SET status='queued', available_at=?, visibility_timeout_at='', claimed_at='', acked_at='',
                    replay_deadline_at='', retention_deadline_at='', queue_name=?, dead_letter_queue=?,
                    lease_id='', worker_id='', current_attempt_id='', last_error='', metadata=?, updated_at=?
                WHERE id=?
                """,
                (
                    now,
                    str(queue_metadata.get("origin_queue_name") or job.get("metadata", {}).get("queue_name") or "default"),
                    str((queue_metadata.get("queue_policy") or {}).get("dead_letter_queue") or queue_message.get("dead_letter_queue") or ""),
                    json.dumps(queue_metadata),
                    now,
                    queue_message["id"],
                ),
            )
            conn.commit()
        replayed_job = self.get_job(job["id"])
        replayed_queue = self._queue_message_for_job(job["id"])
        self._record_event(
            "mesh.queue.replayed",
            peer_id=self.node_id,
            request_id=replayed_job["request_id"],
            payload={
                "job_id": replayed_job["id"],
                "queue_message_id": replayed_queue.get("id", ""),
                "reason": str(reason),
            },
        )
        return {"status": "queued", "job": replayed_job, "queue_message": replayed_queue}

    def set_queue_ack_deadline(
        self,
        *,
        queue_message_id: str = "",
        attempt_id: str = "",
        ttl_seconds: int = 0,
        reason: str = "operator_ack_deadline_update",
    ) -> dict:
        queue_message_token = (queue_message_id or "").strip()
        attempt_token = (attempt_id or "").strip()
        if not queue_message_token and not attempt_token:
            raise MeshPolicyError("queue_message_id or attempt_id is required")
        with self._conn() as conn:
            if queue_message_token:
                row = conn.execute("SELECT * FROM mesh_queue_messages WHERE id=?", (queue_message_token,)).fetchone()
            else:
                row = conn.execute("SELECT * FROM mesh_queue_messages WHERE current_attempt_id=?", (attempt_token,)).fetchone()
        if row is None:
            raise MeshPolicyError("queue message not found")
        queue_message = self._row_to_queue_message(row)
        if queue_message["status"] != "inflight":
            raise MeshPolicyError("ack deadline can only be updated for inflight queue messages")
        if not queue_message.get("lease_id"):
            raise MeshPolicyError("queue message has no active lease")
        requested_ttl = int(ttl_seconds or queue_message.get("ack_deadline_seconds") or 300)
        refreshed_lease = self.heartbeat_lease(queue_message["lease_id"], ttl_seconds=max(60, requested_ttl))
        updated_metadata = dict(queue_message.get("metadata") or {})
        updated_metadata["last_ack_deadline_update_at"] = _utcnow()
        updated_metadata["last_ack_deadline_reason"] = str(reason)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_queue_messages
                SET ack_deadline_seconds=?, visibility_timeout_at=?, metadata=?, updated_at=?
                WHERE id=?
                """,
                (
                    int(refreshed_lease["ttl_seconds"]),
                    refreshed_lease["expires_at"],
                    json.dumps(updated_metadata),
                    _utcnow(),
                    queue_message["id"],
                ),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_queue_messages WHERE id=?", (queue_message["id"],)).fetchone()
        refreshed_queue = self._row_to_queue_message(fresh)
        self._record_event(
            "mesh.queue.ack_deadline_updated",
            peer_id=self.node_id,
            request_id="",
            payload={
                "job_id": refreshed_queue["job_id"],
                "queue_message_id": refreshed_queue["id"],
                "attempt_id": refreshed_queue.get("current_attempt_id") or "",
                "ttl_seconds": int(refreshed_lease["ttl_seconds"]),
                "reason": str(reason),
            },
        )
        return {"status": "ok", "queue_message": refreshed_queue, "lease": refreshed_lease}

    def register_worker(
        self,
        *,
        worker_id: str,
        agent_id: str = "",
        capabilities: Optional[list[str]] = None,
        resources: Optional[dict] = None,
        labels: Optional[list[str]] = None,
        max_concurrent_jobs: int = 1,
        metadata: Optional[dict] = None,
        status: str = "active",
    ) -> dict:
        now = _utcnow()
        capabilities = [str(item).strip() for item in (capabilities or []) if str(item).strip()]
        labels = [str(item).strip() for item in (labels or []) if str(item).strip()]
        worker_metadata = dict(metadata or {})
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_workers
                (id, peer_id, agent_id, status, capabilities, resources, labels, max_concurrent_jobs, metadata,
                 created_at, updated_at, last_heartbeat_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    agent_id=excluded.agent_id,
                    status=excluded.status,
                    capabilities=excluded.capabilities,
                    resources=excluded.resources,
                    labels=excluded.labels,
                    max_concurrent_jobs=excluded.max_concurrent_jobs,
                    metadata=excluded.metadata,
                    updated_at=excluded.updated_at,
                    last_heartbeat_at=excluded.last_heartbeat_at
                """,
                (
                    worker_id,
                    self.node_id,
                    agent_id,
                    (status or "active").strip().lower(),
                    json.dumps(capabilities),
                    json.dumps(dict(resources or {})),
                    json.dumps(labels),
                    max(1, int(max_concurrent_jobs)),
                    json.dumps(worker_metadata),
                    now,
                    now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_workers WHERE id=?", (worker_id,)).fetchone()
        worker = self._row_to_worker(row)
        self._record_event(
            "mesh.worker.registered",
            peer_id=self.node_id,
            payload={"worker_id": worker_id, "capabilities": worker["capabilities"]},
        )
        return worker

    def heartbeat_worker(self, worker_id: str, *, status: str = "", metadata: Optional[dict] = None) -> dict:
        row = self._get_worker_row(worker_id)
        if row is None:
            raise MeshPolicyError("worker not found")
        now = _utcnow()
        merged_metadata = _loads_json(row["metadata"], {})
        merged_metadata.update(dict(metadata or {}))
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_workers
                SET status=?, metadata=?, updated_at=?, last_heartbeat_at=?
                WHERE id=?
                """,
                (
                    (status or row["status"] or "active").strip().lower(),
                    json.dumps(merged_metadata),
                    now,
                    now,
                    worker_id,
                ),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_workers WHERE id=?", (worker_id,)).fetchone()
        return self._row_to_worker(fresh)

    def list_workers(self, *, limit: int = 25) -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_workers
                ORDER BY last_heartbeat_at DESC, updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        workers = [self._row_to_worker(row) for row in rows]
        return {"peer_id": self.node_id, "count": len(workers), "workers": workers}

    def _worker_active_attempts(self, worker_id: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS attempt_count
                FROM mesh_job_attempts
                WHERE worker_id=? AND status IN ('claimed', 'running')
                """,
                ((worker_id or "").strip(),),
            ).fetchone()
        return int((row["attempt_count"] if row is not None else 0) or 0)

    def _requirements_satisfied_for_worker(self, requirements: dict, worker: dict) -> bool:
        needed = {str(item).strip() for item in (requirements.get("capabilities") or []) if str(item).strip()}
        available = set(worker.get("capabilities") or [])
        available.update(card["name"] for card in self.capability_cards() if card.get("available"))
        return needed.issubset(available)

    def poll_jobs(self, worker_id: str, *, limit: int = 10) -> dict:
        worker = self._row_to_worker(self._get_worker_row(worker_id))
        if worker is None:
            raise MeshPolicyError("worker not found")
        self._requeue_expired_queue_messages(limit=max(5, int(limit or 10)))
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT mesh_jobs.*
                FROM mesh_queue_messages
                JOIN mesh_jobs ON mesh_jobs.id = mesh_queue_messages.job_id
                WHERE mesh_jobs.target_peer_id=?
                  AND mesh_queue_messages.status='queued'
                  AND mesh_queue_messages.available_at <= ?
                  AND mesh_jobs.status IN ('queued', 'retry_wait')
                ORDER BY mesh_queue_messages.available_at ASC, mesh_queue_messages.updated_at ASC
                LIMIT ?
                """,
                (self.node_id, _utcnow(), max(1, int(limit)) * 4),
            ).fetchall()
        jobs = []
        for row in rows:
            job = self._row_to_job(row)
            if self._requirements_satisfied_for_worker(job.get("requirements") or {}, worker):
                jobs.append(job)
            if len(jobs) >= max(1, int(limit)):
                break
        return {"status": "ok", "worker": worker, "jobs": jobs}

    def claim_next_job(self, worker_id: str, *, job_id: str = "", ttl_seconds: int = 0) -> dict:
        self._requeue_expired_queue_messages(limit=25)
        worker = self.heartbeat_worker(worker_id)
        if worker["status"] not in {"active", "ready"}:
            return {"status": "idle", "reason": "worker_inactive", "worker": worker}
        if self._worker_active_attempts(worker_id) >= int(worker.get("max_concurrent_jobs") or 1):
            return {"status": "idle", "reason": "worker_busy", "worker": worker}
        candidates = []
        if job_id:
            candidates = [self.get_job(job_id)]
        else:
            candidates = self.poll_jobs(worker_id, limit=1)["jobs"]
        if not candidates:
            return {"status": "idle", "reason": "no_jobs", "worker": worker}
        job = candidates[0]
        queue_message = self._queue_message_for_job(job["id"])
        if job["status"] not in {"queued", "retry_wait"} or queue_message.get("status") != "queued":
            return {"status": "idle", "reason": "job_unavailable", "worker": worker, "job": job}
        ack_deadline_seconds = max(60, int(ttl_seconds or queue_message.get("ack_deadline_seconds") or 300))
        attempt_number = self._next_attempt_number(job["id"])
        attempt_id = str(uuid.uuid4())
        resume_checkpoint_ref = self._job_resume_checkpoint_ref(job) if attempt_number > 1 else {}
        lease = self.acquire_lease(
            peer_id=self.node_id,
            resource=f"job:{job['id']}:attempt:{attempt_number}",
            agent_id=worker_id,
            job_id=job["id"],
            ttl_seconds=ack_deadline_seconds,
            metadata={"worker_id": worker_id, "attempt_number": attempt_number},
        )
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_job_attempts
                (id, job_id, attempt_number, worker_id, status, lease_id, executor, result_ref, error, metadata,
                 started_at, heartbeat_at, finished_at)
                VALUES (?, ?, ?, ?, 'claimed', ?, ?, '{}', '', ?, ?, ?, '')
                """,
                (
                    attempt_id,
                    job["id"],
                    attempt_number,
                    worker_id,
                    lease["id"],
                    worker_id,
                    json.dumps(
                        {
                            "claimed_from_status": job["status"],
                            "queue_message_id": queue_message.get("id") or "",
                            "resumed_from_checkpoint_ref": resume_checkpoint_ref,
                            "executor_device_profile": dict(self.device_profile),
                            "executor_sync_policy": self._device_profile_sync_policy(self.device_profile),
                        }
                    ),
                    now,
                    now,
                ),
            )
            metadata = dict(job.get("metadata") or {})
            metadata["current_attempt_id"] = attempt_id
            metadata["claimed_by_worker_id"] = worker_id
            metadata["attempt_count"] = attempt_number
            if resume_checkpoint_ref:
                metadata["resume_checkpoint_ref"] = resume_checkpoint_ref
                metadata["restart_from_scratch"] = False
                metadata["resume_attempted_at"] = now
                metadata["resume_count"] = int(metadata.get("resume_count") or 0) + 1
                metadata["last_resumed_at"] = now
                metadata["last_resumed_by"] = str(metadata.get("last_resume_requested_by") or worker_id)
                metadata["last_resume_reason"] = str(
                    metadata.get("last_resume_requested_reason")
                    or metadata.get("last_recovery_reason")
                    or "automatic_retry_resume"
                )
            job_status = "resuming" if resume_checkpoint_ref else "running"
            conn.execute(
                """
                UPDATE mesh_jobs
                SET status=?, lease_id=?, executor=?, metadata=?, updated_at=?
                WHERE id=?
                """,
                (job_status, lease["id"], worker_id, json.dumps(metadata), now, job["id"]),
            )
            conn.execute(
                """
                UPDATE mesh_queue_messages
                SET status='inflight', delivery_attempts=delivery_attempts+1, visibility_timeout_at=?, claimed_at=?,
                    lease_id=?, worker_id=?, current_attempt_id=?, updated_at=?
                WHERE job_id=?
                """,
                (lease["expires_at"], now, lease["id"], worker_id, attempt_id, now, job["id"]),
            )
            conn.commit()
        claimed_job = self.get_job(job["id"])
        attempt = self._row_to_attempt(self._attempt_row(attempt_id))
        self._record_event(
            "mesh.job.claimed",
            peer_id=self.node_id,
            request_id=claimed_job["request_id"],
            payload={
                "job_id": claimed_job["id"],
                "attempt_id": attempt_id,
                "worker_id": worker_id,
                "resume_artifact_id": resume_checkpoint_ref.get("id", ""),
            },
        )
        claimed_queue = self._queue_message_for_job(job["id"])
        self._record_event(
            "mesh.queue.claimed",
            peer_id=self.node_id,
            request_id=claimed_job["request_id"],
            payload={"job_id": claimed_job["id"], "attempt_id": attempt_id, "queue_message_id": claimed_queue.get("id", "")},
        )
        return {"status": "claimed", "worker": worker, "job": claimed_job, "attempt": attempt, "queue_message": claimed_queue}

    def heartbeat_job_attempt(self, attempt_id: str, *, ttl_seconds: int = 300, metadata: Optional[dict] = None) -> dict:
        row = self._attempt_row(attempt_id)
        if row is None:
            raise MeshPolicyError("job attempt not found")
        now = _utcnow()
        merged_metadata = _loads_json(row["metadata"], {})
        merged_metadata.update(dict(metadata or {}))
        lease_id = (row["lease_id"] or "").strip()
        if lease_id:
            lease = self.heartbeat_lease(lease_id, ttl_seconds=ttl_seconds)
        else:
            lease = {}
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_job_attempts
                SET status='running', heartbeat_at=?, metadata=?
                WHERE id=?
                """,
                (now, json.dumps(merged_metadata), attempt_id),
            )
            conn.execute(
                """
                UPDATE mesh_queue_messages
                SET visibility_timeout_at=?, updated_at=?
                WHERE current_attempt_id=? AND status='inflight'
                """,
                ((lease.get("expires_at") or now), now, attempt_id),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_job_attempts WHERE id=?", (attempt_id,)).fetchone()
        return self._row_to_attempt(fresh)

    def complete_job_attempt(
        self,
        attempt_id: str,
        result: Any,
        *,
        media_type: str = "application/json",
        executor: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        row = self._attempt_row(attempt_id)
        if row is None:
            raise MeshPolicyError("job attempt not found")
        attempt = self._row_to_attempt(row)
        job = self.get_job(attempt["job_id"])
        attempt_metadata = _loads_json(row["metadata"], {})
        result_package = self._publish_job_result_package(
            job,
            result=result,
            media_type=media_type,
            executor=executor or row["executor"] or attempt["worker_id"],
            attempt_id=attempt_id,
            metadata=dict(metadata or {}),
        )
        result_artifact = result_package["result_ref"]
        finished_at = _utcnow()
        lease_id = (row["lease_id"] or "").strip()
        queue_message = self._queue_message_for_job(job["id"])
        queue_policy = self._queue_policy_for_message(queue_message)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_job_attempts
                SET status='completed', executor=?, result_ref=?, metadata=?, heartbeat_at=?, finished_at=?
                WHERE id=?
                """,
                (
                    executor or row["executor"] or "",
                    json.dumps(result_artifact),
                    json.dumps({**attempt_metadata, **dict(metadata or {})}),
                    finished_at,
                    finished_at,
                    attempt_id,
                ),
            )
            job_metadata = dict(job.get("metadata") or {})
            job_metadata["completed_by_worker_id"] = attempt["worker_id"]
            job_metadata["result_bundle_ref"] = result_package["bundle_ref"]
            job_metadata["result_config_ref"] = result_package["config_ref"]
            job_metadata["result_attestation_ref"] = result_package["attestation_ref"]
            job_metadata["result_artifacts"] = result_package["related_artifacts"]
            job_metadata["secret_delivery"] = list(result_package.get("secret_delivery") or [])
            job_metadata["current_attempt_id"] = ""
            checkpoint_ref = dict(result_package["related_artifacts"].get("checkpoint") or {})
            if checkpoint_ref:
                job_metadata["latest_checkpoint_ref"] = checkpoint_ref
            job_metadata.pop("resume_checkpoint_ref", None)
            conn.execute(
                """
                UPDATE mesh_jobs
                SET status='completed', result_ref=?, executor=?, metadata=?, updated_at=?
                WHERE id=?
                """,
                (
                    json.dumps(result_artifact),
                    executor or row["executor"] or attempt["worker_id"],
                    json.dumps(job_metadata),
                    finished_at,
                    job["id"],
                ),
            )
            conn.execute(
                """
                UPDATE mesh_queue_messages
                SET status='acked', acked_at=?, visibility_timeout_at='', lease_id='', worker_id='',
                    current_attempt_id='', replay_deadline_at='', retention_deadline_at=?, updated_at=?
                WHERE job_id=?
                """,
                (finished_at, _utc_after(queue_policy["retention_seconds"]), finished_at, job["id"]),
            )
            conn.commit()
        if lease_id:
            self.release_lease(lease_id, status="completed")
        completed_job = self.get_job(job["id"])
        completed_attempt = self._row_to_attempt(self._attempt_row(attempt_id))
        completed_queue = self._queue_message_for_job(job["id"])
        self._record_event(
            "mesh.job.completed",
            peer_id=self.node_id,
            request_id=completed_job["request_id"],
            payload={
                "job_id": completed_job["id"],
                "attempt_id": attempt_id,
                "worker_id": attempt["worker_id"],
                "result_artifact_id": result_artifact["id"],
                "bundle_artifact_id": result_package["bundle_ref"]["id"],
            },
        )
        self._record_event(
            "mesh.queue.acked",
            peer_id=self.node_id,
            request_id=completed_job["request_id"],
            payload={"job_id": completed_job["id"], "attempt_id": attempt_id, "queue_message_id": completed_queue.get("id", "")},
        )
        return {"status": "completed", "job": completed_job, "attempt": completed_attempt, "queue_message": completed_queue}

    def fail_job_attempt(
        self,
        attempt_id: str,
        *,
        error: str,
        retryable: bool = True,
        metadata: Optional[dict] = None,
    ) -> dict:
        row = self._attempt_row(attempt_id)
        if row is None:
            raise MeshPolicyError("job attempt not found")
        attempt = self._row_to_attempt(row)
        job = self.get_job(attempt["job_id"])
        finished_at = _utcnow()
        lease_id = (row["lease_id"] or "").strip()
        queue_message = self._queue_message_for_job(job["id"])
        queue_policy = self._queue_policy_for_message(queue_message)
        failure_metadata = dict(metadata or {})
        checkpoint_ref = dict(failure_metadata.get("checkpoint_ref") or {})
        checkpoint_payload = failure_metadata.pop("checkpoint", None)
        if checkpoint_payload is not None:
            checkpoint_ref = self._publish_attempt_checkpoint(
                job,
                attempt_id=attempt_id,
                checkpoint_payload=checkpoint_payload,
                metadata={"failure_error": str(error)},
            )
        if checkpoint_ref:
            failure_metadata["checkpoint_ref"] = checkpoint_ref
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_job_attempts
                SET status='failed', error=?, metadata=?, heartbeat_at=?, finished_at=?
                WHERE id=?
                """,
                (
                    str(error),
                    json.dumps({**_loads_json(row["metadata"], {}), **failure_metadata}),
                    finished_at,
                    finished_at,
                    attempt_id,
                ),
            )
            conn.commit()
        if lease_id:
            self.release_lease(lease_id, status="failed")
        normalized_metadata = self._normalize_job_metadata(job.get("metadata") or {})
        job_spec = dict(job.get("spec") or normalized_metadata.get("job_spec") or {})
        max_attempts = int((((normalized_metadata.get("retry_policy") or {}).get("max_attempts")) or 1))
        resumability = dict(job_spec.get("resumability") or normalized_metadata.get("resumability") or {})
        checkpoint_policy = dict(job_spec.get("checkpoints") or normalized_metadata.get("checkpoint_policy") or {})
        sync_resilience = self._job_sync_resilience(job, metadata=normalized_metadata, spec=job_spec)
        can_resume = bool(resumability.get("enabled")) and bool(checkpoint_ref.get("id"))
        has_retry_budget = bool(retryable) and int(attempt["attempt_number"]) < max_attempts
        next_status = "failed"
        job_metadata = dict(job.get("metadata") or {})
        job_metadata["current_attempt_id"] = ""
        job_metadata["last_error"] = str(error)
        job_metadata["last_failure_at"] = finished_at
        job_metadata["last_failure_error"] = str(error)
        if checkpoint_ref:
            job_metadata["latest_checkpoint_ref"] = checkpoint_ref
            job_metadata["checkpointed_at"] = finished_at
        job_metadata.pop("resume_checkpoint_ref", None)
        job_metadata.pop("recovery_hint", None)
        if has_retry_budget:
            next_status = "retry_wait"
            job_metadata["retry_scheduled_at"] = finished_at
            if checkpoint_ref and bool(checkpoint_policy.get("on_retry")):
                job_metadata["resume_checkpoint_ref"] = checkpoint_ref
                job_metadata["restart_from_scratch"] = False
        elif can_resume:
            next_status = "checkpointed"
            job_metadata["resume_checkpoint_ref"] = checkpoint_ref
            job_metadata["restart_from_scratch"] = False
        if next_status in {"retry_wait", "checkpointed"}:
            recovery_hint = self._intermittent_recovery_hint(job, sync_resilience=sync_resilience)
            if recovery_hint:
                recovery_hint["created_at"] = finished_at
                recovery_hint["checkpoint_artifact_id"] = checkpoint_ref.get("id", "")
                job_metadata["recovery_hint"] = recovery_hint
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_jobs
                SET status=?, lease_id='', metadata=?, updated_at=?
                WHERE id=?
                """,
                (next_status, json.dumps(job_metadata), finished_at, job["id"]),
            )
            queue_status = "queued" if next_status == "retry_wait" else "dead_letter"
            dead_letter_queue = queue_policy["dead_letter_queue"]
            replay_deadline_at = _utc_after(queue_policy["replay_window_seconds"]) if next_status == "failed" else ""
            retention_deadline_at = _utc_after(queue_policy["retention_seconds"]) if next_status in {"failed", "checkpointed"} else ""
            queue_name = (queue_message.get("metadata") or {}).get("origin_queue_name") or queue_message.get("queue_name") or "default"
            if next_status != "retry_wait":
                queue_name = dead_letter_queue
            conn.execute(
                """
                UPDATE mesh_queue_messages
                SET status=?, queue_name=?, dead_letter_queue=?, visibility_timeout_at='', available_at=?, lease_id='',
                    worker_id='', current_attempt_id='', last_error=?, replay_deadline_at=?, retention_deadline_at=?, updated_at=?
                WHERE job_id=?
                """,
                (
                    queue_status,
                    queue_name,
                    dead_letter_queue,
                    finished_at,
                    str(error),
                    replay_deadline_at,
                    retention_deadline_at,
                    finished_at,
                    job["id"],
                ),
            )
            conn.commit()
        failed_job = self.get_job(job["id"])
        failed_attempt = self._row_to_attempt(self._attempt_row(attempt_id))
        failed_queue = self._queue_message_for_job(job["id"])
        event_type = {
            "retry_wait": "mesh.job.retry_scheduled",
            "checkpointed": "mesh.job.checkpointed",
        }.get(next_status, "mesh.job.failed")
        self._record_event(
            event_type,
            peer_id=self.node_id,
            request_id=failed_job["request_id"],
            payload={
                "job_id": failed_job["id"],
                "attempt_id": attempt_id,
                "error": str(error),
                "checkpoint_artifact_id": checkpoint_ref.get("id", ""),
            },
        )
        self._record_event(
            {
                "retry_wait": "mesh.queue.nacked",
                "checkpointed": "mesh.queue.checkpointed",
            }.get(next_status, "mesh.queue.dead_lettered"),
            peer_id=self.node_id,
            request_id=failed_job["request_id"],
            payload={"job_id": failed_job["id"], "attempt_id": attempt_id, "queue_message_id": failed_queue.get("id", "")},
        )
        return {"status": next_status, "job": failed_job, "attempt": failed_attempt, "queue_message": failed_queue}

    def run_worker_once(self, worker_id: str) -> dict:
        claimed = self.claim_next_job(worker_id)
        if claimed.get("status") != "claimed":
            return claimed
        attempt = claimed["attempt"]
        job = claimed["job"]
        self.heartbeat_job_attempt(attempt["id"])
        payload = self._resolve_job_payload(
            {
                "payload": job.get("payload_inline") or {},
                "payload_ref": job.get("payload_ref") or {},
                "origin": job.get("origin") or "",
            }
        )
        resume_checkpoint_ref = dict((attempt.get("metadata") or {}).get("resumed_from_checkpoint_ref") or {})
        if resume_checkpoint_ref:
            payload["_ocp_resume"] = {
                "checkpoint_ref": resume_checkpoint_ref,
                "attempt_number": int(attempt.get("attempt_number") or 1),
            }
        try:
            executor, result, completion_metadata = self._execute_job(job, payload=payload)
            return self.complete_job_attempt(
                attempt["id"],
                result,
                executor=executor,
                metadata=dict(completion_metadata or {}),
            )
        except Exception as exc:
            return self.fail_job_attempt(attempt["id"], error=str(exc), retryable=True)

    def publish_local_artifact(
        self,
        content: Any,
        *,
        media_type: str = "application/json",
        policy: Optional[dict] = None,
        metadata: Optional[dict] = None,
        owner_peer_id: Optional[str] = None,
    ) -> dict:
        if isinstance(content, bytes):
            payload_bytes = content
        elif isinstance(content, str):
            payload_bytes = content.encode("utf-8")
        else:
            payload_bytes = json.dumps(content, sort_keys=True, default=str).encode("utf-8")
        artifact_id = str(uuid.uuid4())
        digest = _sha256_bytes(payload_bytes)
        path = self._artifact_path(artifact_id)
        path.write_bytes(payload_bytes)
        retention = self._artifact_retention_policy(policy=policy, metadata=metadata)
        artifact_metadata = dict(metadata or {})
        artifact_metadata.setdefault("content_sha256", digest)
        artifact_metadata.setdefault("oci_digest", _oci_digest(digest))
        artifact_metadata.setdefault("size_bytes", len(payload_bytes))
        artifact_metadata.setdefault("media_type", media_type)
        ref = ArtifactRef(
            id=artifact_id,
            digest=digest,
            media_type=media_type,
            size_bytes=len(payload_bytes),
            owner_peer_id=(owner_peer_id or self.node_id),
            policy=_normalize_policy(policy or {"classification": "trusted", "mode": "batch"}),
            path=str(path),
            created_at=_utcnow(),
            metadata=artifact_metadata,
            retention_class=retention["retention_class"],
            retention_deadline_at=retention["retention_deadline_at"],
            download_url=f"{self.base_url}/mesh/artifacts/{artifact_id}",
        )
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_artifacts
                (id, digest, media_type, size_bytes, owner_peer_id, policy, path, metadata, retention_class, retention_deadline_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ref.id,
                    ref.digest,
                    ref.media_type,
                    ref.size_bytes,
                    ref.owner_peer_id,
                    json.dumps(ref.policy),
                    ref.path,
                    json.dumps(ref.metadata),
                    ref.retention_class,
                    ref.retention_deadline_at,
                    ref.created_at,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", (ref.id,)).fetchone()
        self._record_event(
            "mesh.artifact.published",
            peer_id=ref.owner_peer_id,
            payload={
                "artifact_id": ref.id,
                "digest": ref.digest,
                "media_type": ref.media_type,
                "retention_class": ref.retention_class,
            },
        )
        return self._row_to_artifact(row)

    def accept_artifact_publish(self, envelope: dict) -> dict:
        peer_id, request_meta, body, _ = self._verify_envelope(envelope, route="/mesh/artifacts/publish")
        artifact = dict(body.get("artifact") or {})
        descriptor = dict(artifact.get("descriptor") or {})
        if artifact.get("json") is not None:
            content = artifact["json"]
            media_type = artifact.get("media_type") or "application/json"
        elif artifact.get("content_base64"):
            content = _b64decode(artifact["content_base64"])
            media_type = artifact.get("media_type") or "application/octet-stream"
        else:
            content = artifact.get("content") or ""
            media_type = artifact.get("media_type") or "text/plain; charset=utf-8"
        published = self.publish_local_artifact(
            content,
            media_type=media_type,
            policy=artifact.get("policy") or {"classification": "trusted", "mode": "batch"},
            metadata={
                "request_id": request_meta.get("request_id"),
                "source_peer_id": peer_id,
                "provided_descriptor": descriptor,
                **dict(artifact.get("metadata") or {}),
            },
            owner_peer_id=peer_id,
        )
        expected_digest = (artifact.get("digest") or "").strip()
        expected_size = int(artifact.get("size_bytes") or descriptor.get("size") or 0)
        expected_media_type = str(descriptor.get("mediaType") or "").strip()
        expected_descriptor_digest = str(descriptor.get("digest") or "").strip()
        if expected_size and int(published["size_bytes"] or 0) != expected_size:
            artifact_path = Path(published["path"])
            try:
                artifact_path.unlink(missing_ok=True)
            except TypeError:
                if artifact_path.exists():
                    artifact_path.unlink()
            with self._conn() as conn:
                conn.execute("DELETE FROM mesh_artifacts WHERE id=?", (published["id"],))
                conn.commit()
            raise MeshPolicyError("artifact size mismatch")
        if expected_media_type and expected_media_type != published["media_type"]:
            artifact_path = Path(published["path"])
            try:
                artifact_path.unlink(missing_ok=True)
            except TypeError:
                if artifact_path.exists():
                    artifact_path.unlink()
            with self._conn() as conn:
                conn.execute("DELETE FROM mesh_artifacts WHERE id=?", (published["id"],))
                conn.commit()
            raise MeshPolicyError("artifact media type mismatch")
        if expected_digest and expected_digest != published["digest"]:
            artifact_path = Path(published["path"])
            try:
                artifact_path.unlink(missing_ok=True)
            except TypeError:
                if artifact_path.exists():
                    artifact_path.unlink()
            with self._conn() as conn:
                conn.execute("DELETE FROM mesh_artifacts WHERE id=?", (published["id"],))
                conn.commit()
            raise MeshPolicyError("artifact digest mismatch")
        if expected_descriptor_digest and expected_descriptor_digest != _oci_digest(published["digest"]):
            artifact_path = Path(published["path"])
            try:
                artifact_path.unlink(missing_ok=True)
            except TypeError:
                if artifact_path.exists():
                    artifact_path.unlink()
            with self._conn() as conn:
                conn.execute("DELETE FROM mesh_artifacts WHERE id=?", (published["id"],))
                conn.commit()
            raise MeshPolicyError("artifact descriptor digest mismatch")
        return {"status": "published", "artifact": published}

    def get_artifact(self, artifact_id: str, *, requester_peer_id: str = "", include_content: bool = True) -> dict:
        self._purge_expired_artifacts(limit=20)
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", ((artifact_id or "").strip(),)).fetchone()
        if row is None:
            raise MeshArtifactAccessError("artifact not found")
        if (
            not self._artifact_is_pinned(row)
            and (row["retention_deadline_at"] or "").strip()
            and (row["retention_deadline_at"] or "") <= _utcnow()
        ):
            self._delete_artifact_row(row, reason="retention_expired")
            raise MeshArtifactAccessError("artifact expired")
        artifact = self._row_to_artifact(row)
        peer = self._row_to_peer(self._get_peer_row(requester_peer_id)) if requester_peer_id else None
        if requester_peer_id and not self._policy_allows_peer(artifact["policy"], peer):
            raise MeshArtifactAccessError("artifact policy denies access for peer")
        if include_content:
            payload_bytes = Path(artifact["path"]).read_bytes()
            artifact["content_base64"] = _b64encode(payload_bytes)
        return artifact

    def list_artifacts(
        self,
        *,
        limit: int = 25,
        artifact_kind: str = "",
        digest: str = "",
        job_id: str = "",
        attempt_id: str = "",
        parent_artifact_id: str = "",
        owner_peer_id: str = "",
        media_type: str = "",
        retention_class: str = "",
    ) -> dict:
        self._purge_expired_artifacts(limit=max(20, int(limit or 25)))
        clauses: list[str] = []
        params: list[Any] = []
        owner_peer_token = (owner_peer_id or "").strip()
        if owner_peer_token:
            clauses.append("owner_peer_id=?")
            params.append(owner_peer_token)
        media_type_token = (media_type or "").strip()
        if media_type_token:
            clauses.append("media_type=?")
            params.append(media_type_token)
        digest_token = str(digest or "").strip().lower()
        if digest_token.startswith("sha256:"):
            digest_token = digest_token.split(":", 1)[1]
        if digest_token:
            clauses.append("digest=?")
            params.append(digest_token)
        retention_class_token = _normalize_retention_class(retention_class) if retention_class else ""
        if retention_class_token:
            clauses.append("retention_class=?")
            params.append(retention_class_token)
        query = ["SELECT * FROM mesh_artifacts"]
        if clauses:
            query.append("WHERE " + " AND ".join(clauses))
        query.append("ORDER BY created_at DESC LIMIT ?")
        params.append(max(max(1, int(limit or 25)) * 12, 100))
        with self._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(params)).fetchall()
        artifact_kind_token = (artifact_kind or "").strip().lower()
        job_id_token = (job_id or "").strip()
        attempt_id_token = (attempt_id or "").strip()
        parent_artifact_id_token = (parent_artifact_id or "").strip()
        artifacts = []
        for row in rows:
            artifact = self._row_to_artifact(row)
            metadata = dict(artifact.get("metadata") or {})
            if artifact_kind_token and (artifact.get("artifact_kind") or "").strip().lower() != artifact_kind_token:
                continue
            if job_id_token and (metadata.get("job_id") or "").strip() != job_id_token:
                continue
            if attempt_id_token and (metadata.get("attempt_id") or "").strip() != attempt_id_token:
                continue
            if parent_artifact_id_token and (metadata.get("parent_artifact_id") or "").strip() != parent_artifact_id_token:
                continue
            artifacts.append(artifact)
            if len(artifacts) >= max(1, int(limit or 25)):
                break
        return {
            "peer_id": self.node_id,
            "count": len(artifacts),
            "artifacts": artifacts,
            "filters": {
                "artifact_kind": artifact_kind_token,
                "digest": digest_token,
                "job_id": job_id_token,
                "attempt_id": attempt_id_token,
                "parent_artifact_id": parent_artifact_id_token,
                "owner_peer_id": owner_peer_token,
                "media_type": media_type_token,
                "retention_class": retention_class_token,
            },
        }

    def _artifact_row_by_digest(self, digest: str):
        token = str(digest or "").strip().lower()
        if token.startswith("sha256:"):
            token = token.split(":", 1)[1]
        if not token:
            return None
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM mesh_artifacts WHERE digest=? ORDER BY created_at DESC LIMIT 1",
                (token,),
            ).fetchone()

    def find_local_artifact_by_digest(self, digest: str) -> Optional[dict]:
        row = self._artifact_row_by_digest(digest)
        return self._row_to_artifact(row) if row is not None else None

    def _resolve_remote_artifact(
        self,
        peer_id: str,
        *,
        artifact_id: str = "",
        digest: str = "",
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
        include_content: bool = True,
    ) -> tuple[MeshPeerClient, dict, str]:
        peer_token = (peer_id or "").strip()
        artifact_token = (artifact_id or "").strip()
        digest_token = str(digest or "").strip().lower()
        if digest_token.startswith("sha256:"):
            digest_token = digest_token.split(":", 1)[1]
        if not artifact_token and not digest_token:
            raise MeshPolicyError("artifact_id or digest is required")
        remote_client, _ = self._resolve_peer_client(peer_token, client=client, base_url=base_url)
        if artifact_token:
            remote_artifact = remote_client.get_artifact(artifact_token, peer_id=self.node_id, include_content=include_content)
            return remote_client, remote_artifact, artifact_token
        listing = remote_client.list_artifacts(limit=1, digest=digest_token)
        if not list(listing.get("artifacts") or []):
            raise MeshArtifactAccessError("remote artifact not found")
        remote_ref = dict(listing["artifacts"][0] or {})
        artifact_token = str(remote_ref.get("id") or "").strip()
        remote_artifact = remote_client.get_artifact(artifact_token, peer_id=self.node_id, include_content=include_content)
        return remote_client, remote_artifact, artifact_token

    def _artifact_json_payload(self, artifact: dict) -> dict:
        payload_bytes = b""
        if str(artifact.get("content_base64") or "").strip():
            payload_bytes = _b64decode(artifact.get("content_base64") or "")
        elif artifact.get("path"):
            try:
                payload_bytes = Path(str(artifact.get("path") or "")).read_bytes()
            except Exception:
                payload_bytes = b""
        if not payload_bytes:
            return {}
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
            return dict(payload or {}) if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _artifact_graph_targets(self, artifact: dict) -> list[dict]:
        metadata = dict(artifact.get("metadata") or {})
        payload = self._artifact_json_payload(artifact)
        refs: list[dict] = []

        def add_ref(*, artifact_id: str = "", digest: str = "", reason: str = "", role: str = "") -> None:
            artifact_token = str(artifact_id or "").strip()
            digest_token = str(digest or "").strip().lower()
            if digest_token.startswith("sha256:"):
                digest_token = digest_token.split(":", 1)[1]
            if not artifact_token and not digest_token:
                return
            refs.append(
                {
                    "artifact_id": artifact_token,
                    "digest": digest_token,
                    "reason": str(reason or "").strip(),
                    "role": str(role or "").strip(),
                }
            )

        parent_artifact_id = str(metadata.get("parent_artifact_id") or "").strip()
        if parent_artifact_id:
            add_ref(artifact_id=parent_artifact_id, reason="parent_artifact")

        for key in ("primary_artifact_id", "subject_artifact_id", "config_artifact_id", "attestation_artifact_id"):
            if str(metadata.get(key) or "").strip():
                add_ref(artifact_id=str(metadata.get(key) or "").strip(), reason=f"metadata:{key}")

        kind = str(payload.get("kind") or "").strip().lower()
        if kind == "ocp.artifact.bundle":
            primary = dict(payload.get("primary") or {})
            add_ref(artifact_id=primary.get("id") or "", digest=primary.get("digest") or "", reason="bundle_primary", role=primary.get("role") or "")
            config = dict(payload.get("config") or {})
            add_ref(digest=config.get("digest") or "", reason="bundle_config")
            subject = dict(payload.get("subject") or {})
            add_ref(digest=subject.get("digest") or "", reason="bundle_subject")
            for descriptor in list(payload.get("descriptors") or []):
                descriptor_item = dict(descriptor or {})
                add_ref(
                    artifact_id=descriptor_item.get("id") or "",
                    digest=descriptor_item.get("digest") or "",
                    reason="bundle_descriptor",
                    role=descriptor_item.get("role") or "",
                )
        elif kind == "ocp.execution.attestation":
            subject = dict(payload.get("subject") or {})
            add_ref(
                artifact_id=subject.get("artifact_id") or "",
                digest=subject.get("digest") or "",
                reason="attestation_subject",
            )
        elif kind == "ocp.artifact.config":
            result = dict(payload.get("result") or {})
            add_ref(
                artifact_id=result.get("artifact_id") or "",
                digest=result.get("digest") or "",
                reason="config_result",
            )

        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for item in refs:
            key = (str(item.get("artifact_id") or "").strip(), str(item.get("digest") or "").strip().lower())
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _artifact_attempt_graph_targets(
        self,
        peer_id: str,
        *,
        remote_client: MeshPeerClient,
        artifact: dict,
        max_items: int = 20,
    ) -> list[dict]:
        metadata = dict(artifact.get("metadata") or {})
        job_id = str(metadata.get("job_id") or "").strip()
        attempt_id = str(metadata.get("attempt_id") or "").strip()
        if not job_id:
            return []
        listing = remote_client.list_artifacts(
            limit=max(1, int(max_items or 20)),
            job_id=job_id,
            attempt_id=attempt_id,
        )
        refs: list[dict] = []
        for item in list(listing.get("artifacts") or []):
            artifact_item = dict(item or {})
            refs.append(
                {
                    "artifact_id": str(artifact_item.get("id") or "").strip(),
                    "digest": str(artifact_item.get("digest") or "").strip().lower(),
                    "reason": "attempt_artifact_set",
                    "role": str(artifact_item.get("artifact_kind") or "").strip(),
                }
            )
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for item in refs:
            key = (item["artifact_id"], item["digest"])
            if key in seen or not any(key):
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def replicate_artifact_from_peer(
        self,
        peer_id: str,
        *,
        artifact_id: str = "",
        digest: str = "",
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
        request_id: Optional[str] = None,
        pin: bool = False,
    ) -> dict:
        peer_token = (peer_id or "").strip()
        if not peer_token:
            raise MeshPolicyError("peer_id is required")
        artifact_token = (artifact_id or "").strip()
        digest_token = str(digest or "").strip().lower()
        if digest_token.startswith("sha256:"):
            digest_token = digest_token.split(":", 1)[1]
        if not artifact_token and not digest_token:
            raise MeshPolicyError("artifact_id or digest is required")

        local_hit = self.find_local_artifact_by_digest(digest_token) if digest_token else None
        if local_hit is not None:
            self._record_event(
                "mesh.artifact.sync.local_hit",
                peer_id=peer_token,
                request_id=request_id or "",
                payload={"artifact_id": local_hit["id"], "digest": local_hit["digest"]},
            )
            return {
                "status": "already_present",
                "artifact": local_hit,
                "source": {"peer_id": peer_token, "artifact_id": artifact_token, "digest": digest_token},
            }

        remote_client, remote_artifact, artifact_token = self._resolve_remote_artifact(
            peer_token,
            artifact_id=artifact_token,
            digest=digest_token,
            client=client,
            base_url=base_url,
            include_content=True,
        )

        remote_digest = str(remote_artifact.get("digest") or "").strip().lower()
        if not remote_digest:
            raise MeshPolicyError("remote artifact missing digest")
        if digest_token and remote_digest != digest_token:
            raise MeshPolicyError("remote artifact digest mismatch")

        local_hit = self.find_local_artifact_by_digest(remote_digest)
        if local_hit is not None:
            self._record_event(
                "mesh.artifact.sync.local_hit",
                peer_id=peer_token,
                request_id=request_id or "",
                payload={"artifact_id": local_hit["id"], "digest": local_hit["digest"]},
            )
            return {
                "status": "already_present",
                "artifact": local_hit,
                "source": {"peer_id": peer_token, "artifact_id": artifact_token, "digest": remote_digest},
            }

        content_base64 = str(remote_artifact.get("content_base64") or "").strip()
        if not content_base64:
            raise MeshPolicyError("remote artifact content missing")
        source_artifact_id = remote_artifact.get("id") or artifact_token
        verification = {
            "status": "verified",
            "verified": True,
            "reason": "replicated_from_peer",
            "checked_at": _utcnow(),
            "peer_id": peer_token,
            "source_artifact_id": source_artifact_id,
            "local_digest": remote_digest,
            "remote_digest": remote_digest,
            "size_match": int(remote_artifact.get("size_bytes") or 0) == len(_b64decode(content_base64)),
            "media_type_match": True,
            "descriptor_match": str((remote_artifact.get("oci_descriptor") or {}).get("digest") or "").strip() == _oci_digest(remote_digest),
        }
        replicated = self.publish_local_artifact(
            _b64decode(content_base64),
            media_type=remote_artifact.get("media_type") or "application/octet-stream",
            policy=remote_artifact.get("policy") or {"classification": "trusted", "mode": "batch"},
            metadata={
                **dict(remote_artifact.get("metadata") or {}),
                "replicated_from_peer_id": peer_token,
                "replicated_from_artifact_id": source_artifact_id,
                "replicated_from_download_url": remote_artifact.get("download_url") or "",
                "cas_origin_digest": remote_digest,
                "pinned": bool(pin),
                "mirror_verification": verification,
                "artifact_sync": {
                    "mode": "pull-through",
                    "pinned": bool(pin),
                    "source_peer_id": peer_token,
                    "source_artifact_id": source_artifact_id,
                    "source_digest": remote_digest,
                    "synced_at": _utcnow(),
                    "verified_at": verification["checked_at"],
                    "verification_status": verification["status"],
                },
            },
            owner_peer_id=remote_artifact.get("owner_peer_id") or peer_token,
        )
        if replicated["digest"] != remote_digest:
            raise MeshPolicyError("replicated artifact digest mismatch")
        self._record_event(
            "mesh.artifact.replicated",
            peer_id=peer_token,
            request_id=request_id or "",
            payload={
                "artifact_id": replicated["id"],
                "digest": replicated["digest"],
                "source_artifact_id": source_artifact_id,
                "pinned": bool(pin),
            },
        )
        return {
            "status": "replicated",
            "artifact": replicated,
            "source": {
                "peer_id": peer_token,
                "artifact_id": source_artifact_id,
                "digest": remote_digest,
            },
            "verification": verification,
        }

    def replicate_artifact_graph_from_peer(
        self,
        peer_id: str,
        *,
        artifact_id: str = "",
        digest: str = "",
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
        request_id: Optional[str] = None,
        pin: bool = False,
    ) -> dict:
        peer_token = (peer_id or "").strip()
        if not peer_token:
            raise MeshPolicyError("peer_id is required")
        remote_client, remote_root, resolved_artifact_id = self._resolve_remote_artifact(
            peer_token,
            artifact_id=artifact_id,
            digest=digest,
            client=client,
            base_url=base_url,
            include_content=True,
        )
        root = self.replicate_artifact_from_peer(
            peer_token,
            artifact_id=resolved_artifact_id,
            digest=str(remote_root.get("digest") or "").strip(),
            client=remote_client,
            request_id=request_id,
            pin=pin,
        )
        pending = self._artifact_graph_targets(remote_root)
        pending.extend(self._artifact_attempt_graph_targets(peer_token, remote_client=remote_client, artifact=remote_root))
        seen: set[tuple[str, str]] = {
            (
                str(root["artifact"].get("id") or "").strip(),
                str(root["artifact"].get("digest") or "").strip().lower(),
            )
        }
        replicated_children: list[dict] = []
        for ref in pending:
            ref_id = str(ref.get("artifact_id") or "").strip()
            ref_digest = str(ref.get("digest") or "").strip().lower()
            key = (ref_id, ref_digest)
            if key in seen:
                continue
            seen.add(key)
            if not ref_id and not ref_digest:
                continue
            child = self.replicate_artifact_from_peer(
                peer_token,
                artifact_id=ref_id,
                digest=ref_digest,
                client=remote_client,
                request_id=request_id,
                pin=pin,
            )
            replicated_children.append(
                {
                    "status": child.get("status") or "",
                    "artifact": child.get("artifact") or {},
                    "source": child.get("source") or {},
                    "reason": str(ref.get("reason") or "").strip(),
                    "role": str(ref.get("role") or "").strip(),
                }
            )
        self._record_event(
            "mesh.artifact.graph.replicated",
            peer_id=peer_token,
            request_id=request_id or "",
            payload={
                "root_artifact_id": (root.get("artifact") or {}).get("id", ""),
                "root_digest": (root.get("artifact") or {}).get("digest", ""),
                "replicated_count": len(replicated_children) + 1,
                "pinned": bool(pin),
            },
        )
        return {
            "status": "replicated",
            "root": root,
            "artifacts": [root["artifact"], *[item["artifact"] for item in replicated_children if item.get("artifact")]],
            "graph": {
                "root_artifact_id": (root.get("artifact") or {}).get("id", ""),
                "root_digest": (root.get("artifact") or {}).get("digest", ""),
                "count": len(replicated_children) + 1,
                "linked": replicated_children,
            },
        }

    def set_artifact_pin(self, artifact_id: str, *, pinned: bool = True, reason: str = "operator_pin") -> dict:
        row = self._artifact_row(artifact_id)
        if row is None:
            raise MeshArtifactAccessError("artifact not found")
        metadata = dict(_loads_json(row["metadata"], {}))
        artifact_sync = dict(metadata.get("artifact_sync") or {})
        metadata["pinned"] = bool(pinned)
        artifact_sync["pinned"] = bool(pinned)
        artifact_sync["pin_updated_at"] = _utcnow()
        artifact_sync["pin_reason"] = str(reason or "").strip()
        metadata["artifact_sync"] = artifact_sync
        if pinned:
            updated = self._update_artifact_record(
                artifact_id,
                metadata=metadata,
                retention_class="durable",
                retention_deadline_at="",
            )
        else:
            retention = self._artifact_retention_policy(policy=_loads_json(row["policy"], {}), metadata=metadata)
            updated = self._update_artifact_record(
                artifact_id,
                metadata=metadata,
                retention_class=retention["retention_class"],
                retention_deadline_at=retention["retention_deadline_at"],
            )
        self._record_event(
            "mesh.artifact.pin.updated",
            peer_id=updated.get("owner_peer_id") or self.node_id,
            payload={
                "artifact_id": updated["id"],
                "digest": updated["digest"],
                "pinned": bool(pinned),
                "reason": str(reason or "").strip(),
            },
        )
        return updated

    def verify_artifact_mirror(
        self,
        artifact_id: str,
        *,
        peer_id: str = "",
        source_artifact_id: str = "",
        digest: str = "",
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
    ) -> dict:
        local = self.get_artifact((artifact_id or "").strip(), include_content=False)
        metadata = dict(local.get("metadata") or {})
        artifact_sync = dict(metadata.get("artifact_sync") or {})
        peer_token = (peer_id or artifact_sync.get("source_peer_id") or metadata.get("replicated_from_peer_id") or "").strip()
        if not peer_token:
            raise MeshPolicyError("peer_id is required for mirror verification")
        source_artifact_token = (
            source_artifact_id
            or artifact_sync.get("source_artifact_id")
            or metadata.get("replicated_from_artifact_id")
            or ""
        ).strip()
        digest_token = str(digest or artifact_sync.get("source_digest") or local.get("digest") or "").strip().lower()
        if digest_token.startswith("sha256:"):
            digest_token = digest_token.split(":", 1)[1]
        remote_client, _ = self._resolve_peer_client(peer_token, client=client, base_url=base_url)
        verification = {
            "status": "unknown",
            "verified": False,
            "reason": "",
            "checked_at": _utcnow(),
            "peer_id": peer_token,
            "source_artifact_id": source_artifact_token,
            "local_digest": local.get("digest") or "",
            "remote_digest": "",
            "size_match": False,
            "media_type_match": False,
            "descriptor_match": False,
        }
        try:
            remote = {}
            listing = remote_client.list_artifacts(limit=10, digest=digest_token)
            remote_candidates = list(listing.get("artifacts") or [])
            if source_artifact_token:
                remote = next(
                    (dict(item or {}) for item in remote_candidates if str((item or {}).get("id") or "").strip() == source_artifact_token),
                    {},
                )
            elif remote_candidates:
                remote = dict(remote_candidates[0] or {})
                source_artifact_token = str(remote.get("id") or "").strip()
                verification["source_artifact_id"] = source_artifact_token
            if not remote:
                raise MeshArtifactAccessError("remote artifact not found")
            remote_digest = str(remote.get("digest") or "").strip().lower()
            verification["remote_digest"] = remote_digest
            verification["size_match"] = int(remote.get("size_bytes") or 0) == int(local.get("size_bytes") or 0)
            verification["media_type_match"] = str(remote.get("media_type") or "") == str(local.get("media_type") or "")
            verification["descriptor_match"] = str((remote.get("oci_descriptor") or {}).get("digest") or "").strip() == _oci_digest(remote_digest)
            verification["verified"] = (
                remote_digest == str(local.get("digest") or "").strip().lower()
                and verification["size_match"]
                and verification["media_type_match"]
                and verification["descriptor_match"]
            )
            verification["status"] = "verified" if verification["verified"] else "mismatch"
            verification["reason"] = "remote_descriptor_match" if verification["verified"] else "remote_descriptor_mismatch"
        except MeshArtifactAccessError:
            verification["status"] = "missing_remote"
            verification["reason"] = "remote_artifact_not_found"
        artifact_sync["verified_at"] = verification["checked_at"]
        artifact_sync["verification_status"] = verification["status"]
        metadata["artifact_sync"] = artifact_sync
        metadata["mirror_verification"] = verification
        updated = self._update_artifact_record(local["id"], metadata=metadata)
        self._record_event(
            "mesh.artifact.mirror.verified",
            peer_id=peer_token,
            payload={
                "artifact_id": updated["id"],
                "digest": updated["digest"],
                "status": verification["status"],
                "source_artifact_id": verification["source_artifact_id"],
            },
        )
        return {"status": verification["status"], "artifact": updated, "verification": verification}

    def purge_expired_artifacts(self, *, limit: int = 100) -> dict:
        purged = self._purge_expired_artifacts(limit=limit)
        return {"status": "ok", "peer_id": self.node_id, "purged": purged}

    def _dispatch_mode_requires_worker(self, kind: str, dispatch_mode: str) -> bool:
        return dispatch_mode == "queued" or kind in {"shell.command", "python.inline", "docker.container", "wasm.component"}

    def _peer_capabilities(self, peer: Optional[dict]) -> set[str]:
        cards = list((peer or {}).get("capability_cards") or [])
        return {str(card.get("name") or "").strip() for card in cards if card.get("available", True)}

    def _peer_worker_count(self, peer: Optional[dict]) -> int:
        metadata = dict((peer or {}).get("metadata") or {})
        remote_workers = list(metadata.get("remote_workers") or [])
        return len(remote_workers)

    def _peer_worker_slots(self, peer: Optional[dict]) -> int:
        metadata = dict((peer or {}).get("metadata") or {})
        remote_workers = list(metadata.get("remote_workers") or [])
        slots = 0
        for worker in remote_workers:
            if not isinstance(worker, dict):
                continue
            if worker.get("status") not in {"active", "ready"}:
                continue
            slots += max(
                0,
                int(worker.get("max_concurrent_jobs") or 1) - int(worker.get("active_attempts") or 0),
            )
        return slots

    def _peer_queue_metrics(self, peer: Optional[dict]) -> dict:
        metadata = dict((peer or {}).get("metadata") or {})
        return dict(metadata.get("remote_queue_metrics") or {})

    def _local_queue_depth(self) -> int:
        self._requeue_expired_queue_messages(limit=25)
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS job_count
                FROM mesh_queue_messages
                WHERE status IN ('queued', 'inflight')
                """,
            ).fetchone()
        return int((row["job_count"] if row is not None else 0) or 0)

    def _normalized_placement(self, job: dict) -> dict:
        placement = dict(job.get("placement") or {})
        queue_class = str(
            placement.get("queue_class")
            or job.get("queue_class")
            or (
                "latency_sensitive"
                if placement.get("latency_sensitive")
                else (job.get("policy") or {}).get("mode")
                or "default"
            )
        ).strip().lower() or "default"
        if queue_class not in {"default", "batch", "latency_sensitive"}:
            queue_class = "default"
        execution_class = str(
            placement.get("execution_class")
            or placement.get("execution_profile")
            or ("latency" if queue_class == "latency_sensitive" else "default")
        ).strip().lower() or "default"
        if execution_class not in {"default", "latency", "throughput", "isolation"}:
            execution_class = "default"
        preferred_peer_ids = [
            str(item).strip()
            for item in (
                placement.get("preferred_peer_ids")
                or placement.get("preferred_peers")
                or ([placement.get("preferred_peer_id")] if placement.get("preferred_peer_id") else [])
            )
            if str(item).strip()
        ]
        required_peer_ids = [
            str(item).strip()
            for item in (placement.get("required_peer_ids") or [])
            if str(item).strip()
        ]
        trust_floor_raw = str(
            placement.get("trust_floor")
            or placement.get("min_trust_tier")
            or placement.get("minimum_trust_tier")
            or ""
        ).strip()
        trust_floor = _normalize_trust_tier(trust_floor_raw) if trust_floor_raw else ""
        preferred_trust_tiers = []
        for item in (
            placement.get("preferred_trust_tiers")
            or ([placement.get("preferred_trust_tier")] if placement.get("preferred_trust_tier") else [])
        ):
            token = str(item or "").strip()
            if not token:
                continue
            normalized = _normalize_trust_tier(token)
            if normalized not in preferred_trust_tiers:
                preferred_trust_tiers.append(normalized)
        max_peer_raw = placement.get("max_peer_queue_depth")
        if max_peer_raw in (None, ""):
            max_peer_raw = placement.get("remote_queue_depth_max")
        max_peer_queue_depth = None if max_peer_raw in (None, "") else max(0, int(max_peer_raw))
        max_local_raw = placement.get("max_local_queue_depth")
        if max_local_raw in (None, ""):
            max_local_raw = placement.get("local_queue_depth_max")
        max_local_queue_depth = None if max_local_raw in (None, "") else max(0, int(max_local_raw))
        preferred_device_classes = _unique_tokens(
            placement.get("preferred_device_classes")
            or ([placement.get("preferred_device_class")] if placement.get("preferred_device_class") else [])
        )
        required_device_classes = _unique_tokens(
            placement.get("required_device_classes")
            or ([placement.get("required_device_class")] if placement.get("required_device_class") else [])
        )
        workload_class = _normalize_workload_class(
            placement.get("workload_class")
            or (job.get("metadata") or {}).get("workload_class")
            or "default"
        )
        resource_needs = _normalize_resources(
            placement.get("resource_needs")
            or (job.get("requirements") or {}).get("resources")
            or {}
        )
        gpu_required_raw = placement.get("gpu_required")
        if gpu_required_raw is None:
            gpu_required = (
                bool(resource_needs.get("gpus"))
                or workload_class in {"gpu_inference", "gpu_training"}
            )
        else:
            gpu_required = bool(gpu_required_raw)
        gpu_class_preferred = _normalize_gpu_class(
            placement.get("gpu_class_preferred")
            or placement.get("preferred_gpu_class")
            or ""
        )
        if gpu_class_preferred == "none":
            gpu_class_preferred = ""
        min_gpu_vram_mb_raw = placement.get("min_gpu_vram_mb") or placement.get("gpu_vram_mb")
        try:
            min_gpu_vram_mb = max(0, int(min_gpu_vram_mb_raw or 0))
        except Exception:
            min_gpu_vram_mb = 0
        min_memory_mb_raw = placement.get("min_memory_mb") or resource_needs.get("memory_mb")
        try:
            min_memory_mb = max(0, int(min_memory_mb_raw or 0))
        except Exception:
            min_memory_mb = 0
        min_cpu_raw = placement.get("min_cpu_cores") or resource_needs.get("cpu")
        try:
            min_cpu = max(0.0, float(min_cpu_raw or 0))
        except Exception:
            min_cpu = 0.0
        return {
            "queue_class": queue_class,
            "execution_class": execution_class,
            "stay_local": bool(placement.get("stay_local")),
            "avoid_public": bool(placement.get("avoid_public")),
            "latency_sensitive": bool(placement.get("latency_sensitive")) or queue_class == "latency_sensitive",
            "batch": bool(placement.get("batch")) or queue_class == "batch",
            "prefer_low_backlog": bool(placement.get("prefer_low_backlog")) or execution_class == "throughput",
            "trust_floor": trust_floor,
            "preferred_trust_tiers": preferred_trust_tiers,
            "max_peer_queue_depth": max_peer_queue_depth,
            "max_local_queue_depth": max_local_queue_depth,
            "preferred_peer_ids": preferred_peer_ids,
            "required_peer_ids": required_peer_ids,
            "preferred_device_classes": preferred_device_classes,
            "required_device_classes": required_device_classes,
            "require_stable_network": bool(placement.get("require_stable_network")),
            "avoid_battery": bool(placement.get("avoid_battery")),
            "require_artifact_mirror": bool(placement.get("require_artifact_mirror")),
            "workload_class": workload_class,
            "gpu_required": bool(gpu_required),
            "gpu_class_preferred": gpu_class_preferred,
            "min_gpu_vram_mb": int(min_gpu_vram_mb),
            "min_memory_mb": int(min_memory_mb),
            "min_cpu_cores": float(min_cpu),
            "resource_needs": resource_needs,
        }

    def _peer_is_public_lane(self, peer: Optional[dict]) -> bool:
        trust_tier = _normalize_trust_tier((peer or {}).get("trust_tier") or "trusted")
        if trust_tier in {"market", "public"}:
            return True
        metadata = dict((peer or {}).get("metadata") or {})
        if metadata.get("external_market"):
            return True
        for card in list((peer or {}).get("capability_cards") or []):
            if (
                isinstance(card, dict)
                and card.get("available", True)
                and bool((card.get("metadata") or {}).get("external_market"))
            ):
                return True
        return False

    def _device_profile_sync_policy(self, profile: dict) -> dict:
        normalized = _normalize_device_profile(profile)
        mode = "intermittent" if normalized.get("sleep_capable") or normalized.get("intermittent") else "continuous"
        return {
            "mode": mode,
            "sleep_capable": bool(normalized.get("sleep_capable")),
            "intermittent": bool(normalized.get("intermittent")),
            "preferred_sync_interval_seconds": int(normalized.get("preferred_sync_interval_seconds") or 60),
            "offline_grace_seconds": int(normalized.get("offline_grace_seconds") or 900),
            "delivery_mode": "poll" if mode == "intermittent" else "continuous",
            "relay_recommended": bool(normalized.get("sleep_capable") or normalized.get("intermittent")),
        }

    def _job_sync_resilience(
        self,
        job: dict,
        *,
        metadata: Optional[dict] = None,
        spec: Optional[dict] = None,
    ) -> dict:
        job_metadata = self._normalize_job_metadata(metadata if metadata is not None else job.get("metadata") or {})
        job_spec = dict(spec or job.get("spec") or job_metadata.get("job_spec") or {})
        resumability = dict(job_spec.get("resumability") or job_metadata.get("resumability") or {})
        checkpoints = dict(job_spec.get("checkpoints") or job_metadata.get("checkpoint_policy") or {})
        return {
            "resumability_enabled": bool(resumability.get("enabled")),
            "checkpoint_enabled": bool(checkpoints.get("enabled")),
            "checkpoint_on_retry": bool(checkpoints.get("on_retry")),
            "resume_capable": bool(resumability.get("enabled")) and bool(checkpoints.get("enabled")),
        }

    def _intermittent_recovery_hint(self, job: dict, *, sync_resilience: Optional[dict] = None) -> dict:
        profile = _normalize_device_profile(self.device_profile)
        resilience = dict(sync_resilience or self._job_sync_resilience(job))
        if not profile.get("sleep_capable") and not profile.get("intermittent"):
            return {}
        hint = {
            "strategy": "resume_on_stable_peer",
            "reason": "intermittent_executor",
            "preferred_target_device_classes": ["full", "relay"],
            "requires_artifact_mirror": True,
            "local_device_class": profile.get("device_class") or "full",
        }
        if resilience.get("resume_capable"):
            hint["resume_capable"] = True
            hint["recommended_action"] = "resume"
        else:
            hint["resume_capable"] = False
            hint["recommended_action"] = "restart"
        return hint

    def _device_profile_execution_limits(self, profile: dict) -> dict:
        normalized = _normalize_device_profile(profile)
        tier = str(normalized.get("execution_tier") or "standard").strip().lower()
        compute_profile = dict(normalized.get("compute_profile") or {})
        if tier == "light":
            baseline = {"cpu": 1.0, "memory_mb": 1024, "disk_mb": 2048, "gpus": 0}
        elif tier == "standard":
            baseline = {"cpu": 4.0, "memory_mb": 8192, "disk_mb": 16384, "gpus": 0}
        elif tier == "heavy":
            baseline = {"cpu": None, "memory_mb": None, "disk_mb": None, "gpus": None}
        else:
            baseline = {"cpu": 0.0, "memory_mb": 0, "disk_mb": 0, "gpus": 0}
        cpu_cores = int(compute_profile.get("cpu_cores") or 0)
        memory_mb = int(compute_profile.get("memory_mb") or 0)
        disk_mb = int(compute_profile.get("disk_mb") or 0)
        gpu_count = int(compute_profile.get("gpu_count") or 0)
        # Compute profile can override tier baseline only when the tier is
        # heavy (unbounded) or when the reported capacity is stricter than the
        # tier cap. This keeps the tier's intent as the authoritative cap and
        # avoids quietly loosening limits via device-profile defaults.
        if cpu_cores > 0:
            if baseline["cpu"] is None:
                baseline["cpu"] = float(cpu_cores)
            else:
                baseline["cpu"] = min(float(cpu_cores), baseline["cpu"])
        if memory_mb > 0:
            if baseline["memory_mb"] is None:
                baseline["memory_mb"] = memory_mb
            else:
                baseline["memory_mb"] = min(memory_mb, baseline["memory_mb"])
        if disk_mb > 0:
            if baseline["disk_mb"] is None:
                baseline["disk_mb"] = disk_mb
            else:
                baseline["disk_mb"] = min(disk_mb, baseline["disk_mb"])
        if gpu_count > 0:
            if baseline["gpus"] is None:
                baseline["gpus"] = gpu_count
            else:
                baseline["gpus"] = max(baseline["gpus"], gpu_count)
        return baseline

    def _device_profile_allows_job(self, profile: dict, job: dict, *, requires_worker: bool) -> tuple[bool, str]:
        normalized = _normalize_device_profile(profile)
        if requires_worker:
            if not normalized.get("compute_ready"):
                return False, "device_not_compute_ready"
            if not normalized.get("accepts_remote_jobs") and normalized.get("device_class") != "full":
                return False, "device_declines_remote_jobs"
        limits = self._device_profile_execution_limits(normalized)
        resources = _normalize_resources((job.get("requirements") or {}).get("resources") or {})
        if limits["cpu"] is not None and float(resources.get("cpu") or 0) > float(limits["cpu"]):
            return False, "device_cpu_limit"
        if limits["memory_mb"] is not None and int(resources.get("memory_mb") or 0) > int(limits["memory_mb"]):
            return False, "device_memory_limit"
        if limits["disk_mb"] is not None and int(resources.get("disk_mb") or 0) > int(limits["disk_mb"]):
            return False, "device_disk_limit"
        if limits["gpus"] is not None and int(resources.get("gpus") or 0) > int(limits["gpus"]):
            return False, "device_gpu_limit"
        return True, ""

    def _device_profile_schedule_reasons(self, profile: dict) -> list[str]:
        normalized = _normalize_device_profile(profile)
        return [
            f"device_class={normalized['device_class']}",
            f"execution_tier={normalized['execution_tier']}",
            f"network_profile={normalized['network_profile']}",
            f"mobility={normalized['mobility']}",
            f"sync_interval={int(normalized['preferred_sync_interval_seconds'])}",
        ]

    def _device_profile_schedule_score(
        self,
        profile: dict,
        placement: dict,
        *,
        requires_worker: bool,
        remote: bool,
        sync_resilience: Optional[dict] = None,
    ) -> tuple[int, list[str]]:
        normalized = _normalize_device_profile(profile)
        reasons = self._device_profile_schedule_reasons(normalized)
        compute_profile = dict(normalized.get("compute_profile") or {})
        workload_class = str(placement.get("workload_class") or "default").strip().lower()
        gpu_required = bool(placement.get("gpu_required"))
        gpu_class_preferred = str(placement.get("gpu_class_preferred") or "").strip().lower()
        gpu_capable = bool(compute_profile.get("gpu_capable"))
        gpu_class = str(compute_profile.get("gpu_class") or "none").strip().lower()
        gpu_vram = int(compute_profile.get("gpu_vram_mb") or 0)
        helper_state = str(normalized.get("helper_state") or "active").strip().lower()
        if helper_state == "retired" and requires_worker and remote:
            return -10000, reasons + ["helper_retired"]
        if helper_state == "draining" and requires_worker:
            reasons.append("helper_draining_penalty")
        if gpu_required and requires_worker and not gpu_capable:
            return -10000, reasons + ["gpu_required_not_available"]
        supports = set(compute_profile.get("supports_workload_classes") or [])
        if workload_class in {"gpu_training"} and requires_worker and "gpu_training" not in supports and gpu_vram < 16384:
            return -10000, reasons + ["workload_class_not_supported"]
        if placement.get("min_gpu_vram_mb") and requires_worker and gpu_capable and gpu_vram < int(placement.get("min_gpu_vram_mb") or 0):
            return -10000, reasons + ["gpu_vram_insufficient"]
        if placement.get("min_memory_mb"):
            memory_mb = int(compute_profile.get("memory_mb") or 0)
            if requires_worker and memory_mb > 0 and memory_mb < int(placement.get("min_memory_mb") or 0):
                return -10000, reasons + ["memory_insufficient"]
        if placement.get("min_cpu_cores"):
            cpu_cores = float(compute_profile.get("cpu_cores") or 0)
            if requires_worker and cpu_cores > 0 and cpu_cores < float(placement.get("min_cpu_cores") or 0):
                return -10000, reasons + ["cpu_cores_insufficient"]
        resilience = dict(sync_resilience or {})
        device_class = normalized["device_class"]
        score = {"full": 90, "light": -40, "micro": -260, "relay": -180}.get(device_class, 0)
        # Compute-profile driven nudges
        if requires_worker:
            cpu_cores = int(compute_profile.get("cpu_cores") or 0)
            memory_mb = int(compute_profile.get("memory_mb") or 0)
            if cpu_cores >= 32:
                score += 80
                reasons.append("cpu_many_cores_bonus")
            elif cpu_cores >= 8:
                score += 30
                reasons.append("cpu_cores_bonus")
            if memory_mb >= 65536:
                score += 40
                reasons.append("memory_large_bonus")
            elif memory_mb >= 16384:
                score += 15
                reasons.append("memory_mid_bonus")
        if workload_class in {"gpu_inference", "gpu_training", "mixed"}:
            if gpu_capable:
                score += 260 if workload_class == "gpu_training" else 180
                reasons.append(f"gpu_match_{workload_class}")
                if gpu_class_preferred and gpu_class_preferred == gpu_class:
                    score += 90
                    reasons.append("gpu_class_preferred_match")
                if placement.get("min_gpu_vram_mb") and gpu_vram >= int(placement.get("min_gpu_vram_mb") or 0):
                    score += 40
                    reasons.append("gpu_vram_sufficient")
            else:
                score -= 120
                reasons.append("gpu_missing_penalty")
        elif workload_class == "cpu_bound" and requires_worker:
            cpu_cores = int(compute_profile.get("cpu_cores") or 0)
            if cpu_cores >= 8:
                score += 60
                reasons.append("cpu_bound_match")
        if helper_state == "draining":
            score -= 220
        elif helper_state == "active" and str(normalized.get("helper_role") or "") == "helper":
            score += 45
            reasons.append("active_helper_bonus")
        if normalized.get("sleep_capable"):
            reasons.append("sleep_capable")
        if normalized.get("intermittent"):
            score -= 90
            reasons.append("intermittent_penalty")
            if requires_worker:
                if not resilience.get("resume_capable"):
                    return -10000, reasons + ["intermittent_requires_resumable_job"]
                score += 35
                reasons.append("intermittent_resume_capable")
                if normalized.get("artifact_mirror_capable"):
                    score += 25
                    reasons.append("artifact_mirror_ready")
        if placement["require_stable_network"] and normalized["network_profile"] in {"metered", "intermittent"}:
            return -10000, reasons + ["stable_network_required"]
        if placement["avoid_battery"] and normalized.get("battery_powered"):
            score -= 120
            reasons.append("battery_penalty")
        if placement["require_artifact_mirror"] and not normalized.get("artifact_mirror_capable"):
            return -10000, reasons + ["artifact_mirror_required"]
        if placement["required_device_classes"] and device_class not in set(placement["required_device_classes"]):
            return -10000, reasons + ["device_class_denied"]
        if placement["preferred_device_classes"] and device_class in set(placement["preferred_device_classes"]):
            score += 140
            reasons.append("preferred_device_class")
        if placement["batch"]:
            if normalized["mobility"] == "fixed":
                score += 40
                reasons.append("fixed_batch_bonus")
            if normalized["network_profile"] in {"wired", "broadband"}:
                score += 30
                reasons.append("stable_network_bonus")
            if normalized.get("sleep_capable"):
                score -= 45
                reasons.append("sleep_batch_penalty")
        if placement["latency_sensitive"] and normalized["network_profile"] in {"metered", "intermittent"}:
            score -= 80
            reasons.append("latency_network_penalty")
        if requires_worker and device_class == "full":
            score += 30
            reasons.append("full_compute_bonus")
            if resilience.get("resume_capable"):
                score += 30
                reasons.append("stable_recovery_bonus")
        if remote and normalized["mobility"] in {"mobile", "wearable"}:
            score -= 50
            reasons.append("mobile_remote_penalty")
        return score, reasons

    def _record_scheduler_decision(
        self,
        *,
        request_id: str = "",
        job_id: str = "",
        job_kind: str = "",
        decision: Optional[dict] = None,
    ) -> dict:
        decision = dict(decision or {})
        selected = dict(decision.get("selected") or {})
        decision_id = str(uuid.uuid4())
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_scheduler_decisions
                (id, request_id, job_id, job_kind, status, strategy, target_type, peer_id, score, placement, selected, candidates, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    (request_id or "").strip(),
                    (job_id or "").strip(),
                    (job_kind or "").strip(),
                    (decision.get("status") or "").strip() or "placed",
                    (decision.get("strategy") or "").strip(),
                    (selected.get("target_type") or "").strip(),
                    (selected.get("peer_id") or "").strip(),
                    int(selected.get("score") or 0),
                    json.dumps(dict(decision.get("placement") or {})),
                    json.dumps(selected),
                    json.dumps(list(decision.get("candidates") or [])),
                    _utcnow(),
                ),
            )
            conn.execute(
                """
                DELETE FROM mesh_scheduler_decisions
                WHERE id NOT IN (
                    SELECT id
                    FROM mesh_scheduler_decisions
                    ORDER BY created_at DESC, id DESC
                    LIMIT 500
                )
                """
            )
            row = conn.execute("SELECT * FROM mesh_scheduler_decisions WHERE id=?", (decision_id,)).fetchone()
            conn.commit()
        return self._row_to_scheduler_decision(row)

    def _attach_job_id_to_scheduler_decision(self, decision_id: str, job_id: str) -> Optional[dict]:
        decision_id = (decision_id or "").strip()
        if not decision_id:
            return None
        with self._conn() as conn:
            conn.execute(
                "UPDATE mesh_scheduler_decisions SET job_id=? WHERE id=?",
                ((job_id or "").strip(), decision_id),
            )
            row = conn.execute("SELECT * FROM mesh_scheduler_decisions WHERE id=?", (decision_id,)).fetchone()
            conn.commit()
        return self._row_to_scheduler_decision(row) if row is not None else None

    def list_scheduler_decisions(self, *, limit: int = 25, status: str = "", target_type: str = "") -> dict:
        clauses = []
        params: list = []
        if status:
            clauses.append("status=?")
            params.append((status or "").strip())
        if target_type:
            clauses.append("target_type=?")
            params.append((target_type or "").strip())
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM mesh_scheduler_decisions
                {where}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                tuple(params + [max(1, int(limit or 25))]),
            ).fetchall()
        decisions = [self._row_to_scheduler_decision(row) for row in rows]
        return {"peer_id": self.node_id, "count": len(decisions), "decisions": decisions}

    def _trust_score(self, trust_tier: str) -> int:
        return {
            "self": 1000,
            "trusted": 800,
            "partner": 600,
            "market": 250,
            "public": 100,
            "blocked": -10000,
        }.get(_normalize_trust_tier(trust_tier), 0)

    def _trust_rank(self, trust_tier: str) -> int:
        return {
            "blocked": 0,
            "public": 1,
            "market": 2,
            "partner": 3,
            "trusted": 4,
            "self": 5,
        }.get(_normalize_trust_tier(trust_tier), 0)

    def _trust_meets_floor(self, trust_tier: str, floor: str) -> bool:
        if not str(floor or "").strip():
            return True
        return self._trust_rank(trust_tier) >= self._trust_rank(floor)

    def _local_load_summary(self) -> dict:
        queue_metrics = self.queue_metrics()
        workers = dict(queue_metrics.get("workers") or {})
        counts = dict(queue_metrics.get("counts") or {})
        queue_depth = int(counts.get("queued", 0) or 0) + int(counts.get("inflight", 0) or 0)
        total_slots = int(workers.get("total_slots", 0) or 0)
        active_attempts = int(workers.get("active_attempts", 0) or 0)
        available_slots = int(workers.get("available_slots", 0) or 0)
        utilization = round(active_attempts / max(1, total_slots), 2) if total_slots > 0 else None
        return {
            "queue_depth": queue_depth,
            "queued": int(counts.get("queued", 0) or 0),
            "inflight": int(counts.get("inflight", 0) or 0),
            "pressure": str(queue_metrics.get("pressure") or "idle"),
            "backlog_ratio": queue_metrics.get("backlog_ratio"),
            "scheduler_penalty": int(queue_metrics.get("scheduler_penalty") or 0),
            "total_slots": total_slots,
            "active_attempts": active_attempts,
            "available_slots": available_slots,
            "utilization": utilization,
        }

    def _peer_load_summary(self, peer: Optional[dict]) -> dict:
        metrics = self._peer_queue_metrics(peer)
        counts = dict(metrics.get("counts") or {})
        worker_metrics = dict(metrics.get("workers") or {})
        queue_depth = int(counts.get("queued", 0) or 0) + int(counts.get("inflight", 0) or 0)
        total_slots = int(worker_metrics.get("total_slots", 0) or 0)
        active_attempts = int(worker_metrics.get("active_attempts", 0) or 0)
        available_slots = int(worker_metrics.get("available_slots", 0) or 0)
        if total_slots <= 0:
            total_slots = self._peer_worker_count(peer)
        if available_slots <= 0:
            available_slots = self._peer_worker_slots(peer)
        utilization = round(active_attempts / max(1, total_slots), 2) if total_slots > 0 else None
        return {
            "queue_depth": queue_depth,
            "queued": int(counts.get("queued", 0) or 0),
            "inflight": int(counts.get("inflight", 0) or 0),
            "pressure": str(metrics.get("pressure") or "unknown"),
            "backlog_ratio": metrics.get("backlog_ratio"),
            "scheduler_penalty": int(metrics.get("scheduler_penalty") or 0),
            "total_slots": total_slots,
            "active_attempts": active_attempts,
            "available_slots": available_slots,
            "utilization": utilization,
        }

    def _local_reliability_summary(self, *, limit: int = 40) -> dict:
        with self._conn() as conn:
            job_rows = conn.execute(
                """
                SELECT status
                FROM mesh_jobs
                WHERE target_peer_id=? AND status IN ('completed', 'failed')
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (self.node_id, max(1, int(limit))),
            ).fetchall()
            retry_row = conn.execute(
                """
                SELECT COUNT(*) AS retry_count
                FROM mesh_job_attempts
                WHERE status='failed' AND job_id IN (
                    SELECT id
                    FROM mesh_jobs
                    WHERE target_peer_id=?
                )
                """,
                (self.node_id,),
            ).fetchone()
        completed = sum(1 for row in job_rows if (row["status"] or "").strip() == "completed")
        failed = sum(1 for row in job_rows if (row["status"] or "").strip() == "failed")
        retries = int((retry_row["retry_count"] if retry_row is not None else 0) or 0)
        total = completed + failed
        if total <= 0:
            success_rate = None
            score = 0
        else:
            success_rate = round(completed / total, 4)
            score = int(round(((completed / total) - 0.5) * 160)) - min(retries, 5) * 4
        return {
            "source": "local_jobs",
            "completed": completed,
            "failed": failed,
            "retried": retries,
            "total": total,
            "success_rate": success_rate,
            "score": max(-120, min(120, score)),
        }

    def _peer_reliability_summary(self, peer: Optional[dict], *, limit: int = 40) -> dict:
        peer_id = (peer or {}).get("peer_id") or self.node_id
        if peer_id == self.node_id:
            return self._local_reliability_summary(limit=limit)
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT event_type
                FROM mesh_remote_events
                WHERE peer_id=? AND event_type IN ('mesh.job.completed', 'mesh.job.failed', 'mesh.job.retry_scheduled')
                ORDER BY remote_seq DESC
                LIMIT ?
                """,
                ((peer_id or "").strip(), max(1, int(limit))),
            ).fetchall()
        completed = sum(1 for row in rows if (row["event_type"] or "").strip() == "mesh.job.completed")
        failed = sum(1 for row in rows if (row["event_type"] or "").strip() == "mesh.job.failed")
        retries = sum(1 for row in rows if (row["event_type"] or "").strip() == "mesh.job.retry_scheduled")
        total = completed + failed
        if total <= 0:
            success_rate = None
            score = 0
        else:
            success_rate = round(completed / total, 4)
            score = int(round(((completed / total) - 0.5) * 160)) - min(retries, 5) * 4
        return {
            "source": "remote_events",
            "completed": completed,
            "failed": failed,
            "retried": retries,
            "total": total,
            "success_rate": success_rate,
            "score": max(-120, min(120, score)),
        }

    def _local_candidate_score(self, job: dict) -> tuple[int, list[str]]:
        requirements = dict(job.get("requirements") or {})
        placement = self._normalized_placement(job)
        kind = (job.get("kind") or "").strip().lower()
        dispatch_mode = self._job_dispatch_mode(kind, job)
        requires_worker = self._dispatch_mode_requires_worker(kind, dispatch_mode)
        sync_resilience = self._job_sync_resilience(job)
        reasons = ["local-first", f"queue_class={placement['queue_class']}"]
        reliability = self._local_reliability_summary()
        load = self._local_load_summary()
        device_profile = dict(self.device_profile)
        reasons.extend(self._device_profile_schedule_reasons(device_profile))
        if placement["stay_local"]:
            reasons.append("stay_local")
        if placement["latency_sensitive"]:
            reasons.append("latency_sensitive")
        if placement["batch"]:
            reasons.append("batch")
        reasons.append(f"execution_class={placement['execution_class']}")
        reasons.append(f"backpressure={load['pressure']}")
        reasons.append(f"local_queue_depth={load['queue_depth']}")
        reasons.append(f"available_slots={load['available_slots']}")
        reasons.append(
            "reliability="
            + (
                f"{reliability['completed']}/{reliability['total']}"
                if reliability["total"] > 0
                else "unknown"
            )
        )
        reasons.append(f"resume_capable={str(sync_resilience['resume_capable']).lower()}")
        if not self._requirements_satisfied(requirements):
            return -10000, reasons + ["requirements_unmet"]
        device_ok, device_reason = self._device_profile_allows_job(device_profile, job, requires_worker=requires_worker)
        if not device_ok:
            return -10000, reasons + [device_reason]
        device_score, device_reasons = self._device_profile_schedule_score(
            device_profile,
            placement,
            requires_worker=requires_worker,
            remote=False,
            sync_resilience=sync_resilience,
        )
        if device_score <= -10000:
            return -10000, reasons + device_reasons
        reasons.extend(device_reasons)
        if placement["max_local_queue_depth"] is not None and load["queue_depth"] > placement["max_local_queue_depth"]:
            return -10000, reasons + ["local_backlog_limit_exceeded"]
        if requires_worker:
            workers = self.list_workers(limit=100)["workers"]
            matching_workers = [worker for worker in workers if self._requirements_satisfied_for_worker(requirements, worker)]
            if not matching_workers:
                return -10000, reasons + ["no_matching_local_worker"]
            available_slots = sum(
                max(0, int(worker.get("max_concurrent_jobs") or 1) - int(worker.get("active_attempts") or 0))
                for worker in matching_workers
                if worker.get("status") in {"active", "ready"}
            )
            reasons.append(f"matching_workers={len(matching_workers)}")
            reasons.append(f"available_slots={available_slots}")
            score = (
                self._trust_score("self")
                + 150
                + (available_slots * 20)
                - (self._local_queue_depth() * 10)
                + reliability["score"]
                + device_score
                - int(load.get("scheduler_penalty") or 0)
            )
            if placement["stay_local"]:
                score += 300
            if placement["latency_sensitive"]:
                score += 140
            if placement["batch"]:
                score -= 20
            if placement["prefer_low_backlog"]:
                score -= load["queue_depth"] * 18
                reasons.append("low_backlog_preferred")
            if placement["execution_class"] == "throughput":
                score += load["available_slots"] * 25
                score -= load["queue_depth"] * 8
                reasons.append("execution_class_throughput")
            elif placement["execution_class"] == "isolation":
                score -= 220
                reasons.append("execution_class_isolation_local_penalty")
            elif placement["execution_class"] == "latency":
                score += 90
                reasons.append("execution_class_latency")
            return score, reasons
        score = self._trust_score("self") + 120 + reliability["score"] + device_score - int(load.get("scheduler_penalty") or 0)
        if placement["stay_local"]:
            score += 300
        if placement["latency_sensitive"]:
            score += 140
        if placement["batch"]:
            score -= 20
        if placement["prefer_low_backlog"]:
            score -= load["queue_depth"] * 18
            reasons.append("low_backlog_preferred")
        if placement["execution_class"] == "throughput":
            score += load["available_slots"] * 25
            score -= load["queue_depth"] * 8
            reasons.append("execution_class_throughput")
        elif placement["execution_class"] == "isolation":
            score -= 220
            reasons.append("execution_class_isolation_local_penalty")
        elif placement["execution_class"] == "latency":
            score += 90
            reasons.append("execution_class_latency")
        return score, reasons + ["inline_capable"]

    def _peer_candidate_score(self, peer: dict, job: dict) -> tuple[int, list[str]]:
        requirements = dict(job.get("requirements") or {})
        policy = _normalize_policy(job.get("policy") or {})
        placement = self._normalized_placement(job)
        reliability = self._peer_reliability_summary(peer)
        load = self._peer_load_summary(peer)
        kind = (job.get("kind") or "").strip().lower()
        dispatch_mode = self._job_dispatch_mode(kind, job)
        requires_worker = self._dispatch_mode_requires_worker(kind, dispatch_mode)
        sync_resilience = self._job_sync_resilience(job)
        device_profile = self._peer_device_profile(peer)
        reasons = [f"trust_tier={peer.get('trust_tier') or 'trusted'}", f"queue_class={placement['queue_class']}"]
        reasons.append(f"execution_class={placement['execution_class']}")
        reasons.append(f"remote_queue_depth={load['queue_depth']}")
        reasons.append(f"remote_pressure={load['pressure']}")
        reasons.extend(self._device_profile_schedule_reasons(device_profile))
        reasons.append(
            "reliability="
            + (
                f"{reliability['completed']}/{reliability['total']}"
                if reliability["total"] > 0
                else "unknown"
            )
        )
        reasons.append(f"resume_capable={str(sync_resilience['resume_capable']).lower()}")
        if placement["stay_local"]:
            return -10000, reasons + ["stay_local"]
        if placement["trust_floor"] and not self._trust_meets_floor(peer.get("trust_tier") or "trusted", placement["trust_floor"]):
            return -10000, reasons + ["trust_floor_denied"]
        if placement["required_peer_ids"] and peer.get("peer_id") not in set(placement["required_peer_ids"]):
            return -10000, reasons + ["peer_not_required"]
        if placement["avoid_public"] and self._peer_is_public_lane(peer):
            return -10000, reasons + ["avoid_public"]
        if placement["max_peer_queue_depth"] is not None and load["queue_depth"] > placement["max_peer_queue_depth"]:
            return -10000, reasons + ["peer_backlog_limit_exceeded"]
        if not self._policy_allows_peer(policy, peer):
            return -10000, reasons + ["policy_denied"]
        if not set(str(item).strip() for item in (requirements.get("capabilities") or []) if str(item).strip()).issubset(
            self._peer_capabilities(peer)
        ):
            return -10000, reasons + ["requirements_unmet"]
        device_ok, device_reason = self._device_profile_allows_job(device_profile, job, requires_worker=requires_worker)
        if not device_ok:
            return -10000, reasons + [device_reason]
        device_score, device_reasons = self._device_profile_schedule_score(
            device_profile,
            placement,
            requires_worker=requires_worker,
            remote=True,
            sync_resilience=sync_resilience,
        )
        if device_score <= -10000:
            return -10000, reasons + device_reasons
        reasons.extend(device_reasons)
        if requires_worker:
            worker_count = self._peer_worker_count(peer)
            available_slots = self._peer_worker_slots(peer)
            reasons.append(f"remote_workers={worker_count}")
            reasons.append(f"available_slots={available_slots}")
            if worker_count <= 0:
                return -10000, reasons + ["no_remote_workers_advertised"]
        score = self._trust_score(peer.get("trust_tier") or "trusted") + reliability["score"] + device_score
        if peer.get("status") == "connected":
            score += 40
            reasons.append("connected")
        if peer.get("heartbeat", {}).get("status") == "active":
            score += 30
            reasons.append("active_heartbeat")
        if (peer.get("sync_state") or {}).get("last_sync_error"):
            score -= 80
            reasons.append("sync_error_penalty")
        if reliability["failed"] > reliability["completed"] and reliability["total"] > 0:
            reasons.append("reliability_penalty")
        elif reliability["completed"] > 0:
            reasons.append("reliability_bonus")
        if placement["latency_sensitive"]:
            score -= 80
            reasons.append("latency_sensitive_penalty")
        if placement["batch"]:
            score += 40
            reasons.append("batch_friendly")
        if placement["preferred_trust_tiers"] and _normalize_trust_tier(peer.get("trust_tier") or "trusted") in set(placement["preferred_trust_tiers"]):
            score += 140
            reasons.append("preferred_trust_tier")
        score -= load["queue_depth"] * 12
        if load["pressure"] == "saturated":
            score -= 120
            reasons.append("remote_saturated")
        elif load["pressure"] == "elevated":
            score -= 60
            reasons.append("remote_elevated")
        if placement["prefer_low_backlog"]:
            score += max(0, 120 - (load["queue_depth"] * 20))
            reasons.append("low_backlog_preferred")
        if placement["execution_class"] == "throughput":
            score += load["available_slots"] * 20
            score += 60 if load["queue_depth"] <= 1 else 0
            reasons.append("execution_class_throughput")
        elif placement["execution_class"] == "isolation":
            if _normalize_trust_tier(peer.get("trust_tier") or "trusted") in {"trusted", "partner"} and not self._peer_is_public_lane(peer):
                score += 220
                reasons.append("execution_class_isolation_preferred")
            elif self._peer_is_public_lane(peer):
                score -= 180
                reasons.append("execution_class_isolation_public_penalty")
        elif placement["execution_class"] == "latency":
            score -= 40
            reasons.append("execution_class_latency_penalty")
        if self._peer_is_public_lane(peer):
            reasons.append("public_lane")
            if placement["latency_sensitive"]:
                score -= 60
            if placement["batch"]:
                score += 20
        if placement["preferred_peer_ids"] and peer.get("peer_id") in set(placement["preferred_peer_ids"]):
            score += 250
            reasons.append("placement_preferred_peer")
        score += self._peer_worker_slots(peer) * 15
        enlistment_meta = dict((peer.get("metadata") or {}).get("enlistment") or {})
        enlistment_state = str(enlistment_meta.get("state") or "").strip().lower()
        enlistment_role = str(enlistment_meta.get("role") or "").strip().lower()
        if enlistment_state == "enlisted":
            score += 180
            reasons.append("helper_enlisted_bonus")
            if enlistment_role == "gpu_helper" and placement.get("workload_class") in {"gpu_inference", "gpu_training", "mixed"}:
                score += 120
                reasons.append("gpu_helper_preferred")
        elif enlistment_state == "draining":
            score -= 140
            reasons.append("helper_draining_penalty")
        return score, reasons

    def select_execution_target(
        self,
        job: dict,
        *,
        request_id: str = "",
        preferred_peer_id: str = "",
        allow_local: bool = True,
        allow_remote: bool = True,
    ) -> dict:
        normalized_job = dict(job or {})
        placement = self._normalized_placement(normalized_job)
        candidates = []
        if allow_local:
            score, reasons = self._local_candidate_score(normalized_job)
            candidates.append(
                {
                    "target_type": "local",
                    "peer_id": self.node_id,
                    "score": score,
                    "reasons": reasons,
                    "selected": False,
                }
            )
        if allow_remote:
            for peer in self.list_peers(limit=500).get("peers", []):
                score, reasons = self._peer_candidate_score(peer, normalized_job)
                if preferred_peer_id and peer["peer_id"] == preferred_peer_id:
                    score += 500
                    reasons = list(reasons) + ["preferred_peer"]
                candidates.append(
                    {
                        "target_type": "peer",
                        "peer_id": peer["peer_id"],
                        "score": score,
                        "reasons": reasons,
                        "selected": False,
                    }
                )
        candidates.sort(key=lambda item: (item["score"], item["target_type"] == "local"), reverse=True)
        selected = next((candidate for candidate in candidates if candidate["score"] > -10000), None)
        if selected is None:
            decision = {
                "status": "unplaced",
                "strategy": "trust-capability-load-v1.2",
                "placement": placement,
                "selected": {},
                "candidates": candidates,
            }
            persisted = self._record_scheduler_decision(
                request_id=request_id,
                job_kind=(normalized_job.get("kind") or "").strip(),
                decision=decision,
            )
            decision["decision_id"] = persisted["id"]
            self._record_event(
                "mesh.scheduler.unplaced",
                peer_id=self.node_id,
                payload={
                    "decision_id": decision["decision_id"],
                    "job_kind": normalized_job.get("kind") or "",
                    "placement": placement,
                    "candidates": candidates,
                },
            )
            return decision
        selected["selected"] = True
        decision = {
            "status": "placed",
            "strategy": "trust-capability-load-v1.2",
            "placement": placement,
            "selected": selected,
            "candidates": candidates,
        }
        persisted = self._record_scheduler_decision(
            request_id=request_id,
            job_kind=(normalized_job.get("kind") or "").strip(),
            decision=decision,
        )
        decision["decision_id"] = persisted["id"]
        self._record_event(
            "mesh.scheduler.decision",
            peer_id=selected["peer_id"],
            payload={
                "decision_id": decision["decision_id"],
                "job_kind": normalized_job.get("kind") or "",
                "target_type": selected["target_type"],
                "score": selected["score"],
                "reasons": selected["reasons"],
                "placement": placement,
            },
        )
        return decision

    def _resolve_job_payload(self, job_body: dict) -> dict:
        if job_body.get("payload") is not None:
            return dict(job_body.get("payload") or {})
        payload_ref = dict(job_body.get("payload_ref") or {})
        if not payload_ref:
            return {}
        artifact_id = (payload_ref.get("id") or "").strip()
        artifact = self.get_artifact(artifact_id, requester_peer_id=(job_body.get("origin") or ""))
        payload_bytes = _b64decode(artifact.get("content_base64") or "")
        try:
            return json.loads(payload_bytes.decode("utf-8"))
        except Exception:
            return {"raw_text": payload_bytes.decode("utf-8", errors="replace")}

    def _requirements_satisfied(self, requirements: dict) -> bool:
        needed = {str(item).strip() for item in (requirements.get("capabilities") or []) if str(item).strip()}
        available = {card["name"] for card in self.capability_cards() if card.get("available")}
        return needed.issubset(available)

    def _store_job_row(
        self,
        *,
        job_id: str,
        request_id: str,
        kind: str,
        origin_peer_id: str,
        target_peer_id: str,
        requirements: dict,
        policy: dict,
        payload_ref: dict,
        payload_inline: dict,
        artifact_inputs: list[dict],
        status: str,
        result_ref: Optional[dict] = None,
        lease_id: str = "",
        executor: str = "",
        metadata: Optional[dict] = None,
        created_at: Optional[str] = None,
    ) -> dict:
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_jobs
                (id, request_id, kind, origin_peer_id, target_peer_id, requirements, policy, payload_ref, payload_inline,
                 artifact_inputs, status, result_ref, lease_id, executor, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    status=excluded.status,
                    result_ref=excluded.result_ref,
                    lease_id=excluded.lease_id,
                    executor=excluded.executor,
                    metadata=excluded.metadata,
                    updated_at=excluded.updated_at
                """,
                (
                    job_id,
                    request_id,
                    kind,
                    origin_peer_id,
                    target_peer_id,
                    json.dumps(requirements),
                    json.dumps(policy),
                    json.dumps(payload_ref),
                    json.dumps(payload_inline),
                    json.dumps(artifact_inputs),
                    status,
                    json.dumps(result_ref or {}),
                    lease_id,
                    executor,
                    json.dumps(metadata or {}),
                    created_at or now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_jobs WHERE request_id=?", (request_id,)).fetchone()
        return self._row_to_job(row)

    def _existing_job_by_request(self, request_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_jobs WHERE request_id=?", ((request_id or "").strip(),)).fetchone()
        return self._row_to_job(row) if row is not None else None

    def _existing_handoff_by_request(self, request_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_handoffs WHERE request_id=?", ((request_id or "").strip(),)).fetchone()
        return self._row_to_handoff(row) if row is not None else None

    def _resolve_runtime_cwd(self, runtime_environment: dict, execution: dict) -> Path:
        requested_cwd = str(runtime_environment.get("cwd") or execution.get("cwd") or execution.get("working_dir") or "").strip()
        cwd_path = self.workspace_root
        if not requested_cwd:
            return cwd_path
        candidate = Path(requested_cwd)
        if not candidate.is_absolute():
            candidate = (self.workspace_root / candidate).resolve()
        else:
            candidate = candidate.resolve()
        if self.workspace_root != candidate and self.workspace_root not in candidate.parents:
            raise MeshPolicyError("runtime cwd must stay inside workspace_root")
        return candidate

    def _resolve_secret_binding_value(self, binding: dict, raw_secret: Any) -> tuple[Optional[str], dict]:
        env_name = _normalize_env_var_name(binding.get("env_var"))
        scope = str(binding.get("scope") or "").strip()
        required = bool(binding.get("required", True))
        source = _normalize_secret_source(binding.get("source") or "inline")
        provider_ref = str(binding.get("provider_ref") or source).strip() or source
        resolved_value: Optional[str] = None
        delivery_record = {
            "env_var": env_name,
            "scope": scope,
            "required": required,
            "source": source,
            "provider_ref": provider_ref,
            "delivery": "env",
            "resolved": False,
            "value_digest": "",
        }
        if source == "inline":
            rendered_value = raw_secret.get("value") if isinstance(raw_secret, dict) else raw_secret
            if rendered_value is not None:
                resolved_value = str(rendered_value)
        elif source == "env":
            provider_name = _normalize_env_var_name(binding.get("name") or env_name)
            delivery_record["name"] = provider_name
            resolved_value = os.environ.get(provider_name)
        elif source == "store":
            provider_name = str(binding.get("name") or "").strip()
            delivery_record["name"] = provider_name
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT * FROM mesh_secrets WHERE scope=? AND name=?",
                    (scope, provider_name),
                ).fetchone()
            if row is not None:
                resolved_value = str(row["value"] or "")
                delivery_record["secret_id"] = row["id"]
        elif source == "file":
            requested_path = str(binding.get("path") or "").strip()
            delivery_record["path"] = requested_path
            file_path = self._resolve_secret_file_path(requested_path)
            resolved_value = file_path.read_text(encoding="utf-8").rstrip("\r\n")
        if resolved_value is not None:
            delivery_record["resolved"] = True
            delivery_record["value_digest"] = _secret_value_digest(resolved_value)
        return resolved_value, delivery_record

    def _build_runtime_env(self, *, job: dict, payload: dict, spec: dict) -> tuple[dict[str, str], list[dict]]:
        execution = dict(spec.get("execution") or {})
        runtime_environment = dict(spec.get("runtime_environment") or {})
        env_policy = dict(runtime_environment.get("env_policy") or {})
        inherit_host_env = bool(env_policy.get("inherit_host_env", True))
        allow_env_override = bool(env_policy.get("allow_env_override", True))
        env: dict[str, str] = dict(os.environ) if inherit_host_env else {}
        delivery_records: list[dict] = []
        for key, value in dict(execution.get("env") or {}).items():
            env_name = _normalize_env_var_name(key)
            if not env_name:
                continue
            if allow_env_override or env_name not in env:
                env[env_name] = str(value)
        payload_secrets = dict(payload.get("secrets") or {})
        for binding in list((runtime_environment.get("secrets") or {}).get("bindings") or []):
            normalized_name = _normalize_env_var_name(binding.get("env_var"))
            if not normalized_name:
                continue
            if normalized_name.startswith("OCP_RESUME_"):
                raise MeshPolicyError(f"secret binding cannot override reserved runtime env: {normalized_name}")
            resolved_value, delivery_record = self._resolve_secret_binding_value(
                dict(binding),
                payload_secrets.get(normalized_name),
            )
            if resolved_value is None:
                delivery_records.append(delivery_record)
                if bool(binding.get("required", True)):
                    raise MeshPolicyError(f"required secret binding missing value: {normalized_name}")
                continue
            env[normalized_name] = str(resolved_value)
            delivery_records.append(delivery_record)
        return env, delivery_records

    def _container_runtime_paths(self, runtime_environment: dict, execution: dict) -> dict[str, Any]:
        filesystem = dict(runtime_environment.get("filesystem") or {})
        profile = str(filesystem.get("profile") or "workspace").strip().lower() or "workspace"
        if profile == "isolated":
            return {
                "mount_workspace": False,
                "host_workdir": None,
                "container_root": "",
                "container_workdir": "",
            }
        host_workdir = self._resolve_runtime_cwd(runtime_environment, execution)
        container_root = "/workspace"
        try:
            rel_path = host_workdir.relative_to(self.workspace_root)
            container_workdir = str((Path(container_root) / rel_path).as_posix())
        except Exception:
            container_workdir = container_root
        return {
            "mount_workspace": True,
            "host_workdir": host_workdir,
            "container_root": container_root,
            "container_workdir": container_workdir,
        }

    def _cleanup_docker_container(self, container_name: str) -> None:
        sample = str(container_name or "").strip()
        if not sample:
            return
        try:
            subprocess.run(
                ["docker", "rm", "-f", sample],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        except Exception:
            logger.debug("docker cleanup failed for %s", sample, exc_info=True)

    def _artifact_path_for_digest(self, digest: str) -> Optional[Path]:
        token = str(digest or "").strip()
        if not token:
            return None
        if token.startswith("sha256:"):
            token = token.split(":", 1)[1]
        with self._conn() as conn:
            row = conn.execute(
                "SELECT path FROM mesh_artifacts WHERE digest=? ORDER BY created_at DESC LIMIT 1",
                (token,),
            ).fetchone()
        if row is None:
            return None
        return Path(row["path"]).resolve()

    def _resolve_wasm_component_path(self, execution: dict, payload: dict) -> tuple[Path, dict]:
        component_ref = dict(execution.get("component_ref") or {})
        explicit_path = str(
            component_ref.get("path")
            or payload.get("module_path")
            or payload.get("component_path")
            or ""
        ).strip()
        if explicit_path:
            candidate = Path(explicit_path)
            if not candidate.is_absolute():
                candidate = (self.workspace_root / candidate).resolve()
            else:
                candidate = candidate.resolve()
            if self.workspace_root != candidate and self.workspace_root not in candidate.parents:
                raise MeshPolicyError("wasm component path must stay inside workspace_root")
            if not candidate.exists():
                raise MeshPolicyError("wasm component path does not exist")
            return candidate, component_ref
        component_id = str(component_ref.get("id") or "").strip()
        if component_id:
            artifact = self.get_artifact(component_id, requester_peer_id="", include_content=False)
            path = Path(artifact["path"]).resolve()
            if not path.exists():
                raise MeshPolicyError("wasm component artifact is missing")
            return path, {**component_ref, "id": artifact.get("id") or component_id, "digest": artifact.get("digest") or component_ref.get("digest") or ""}
        digest_path = self._artifact_path_for_digest(component_ref.get("digest") or "")
        if digest_path is not None and digest_path.exists():
            return digest_path, component_ref
        raise MeshPolicyError("wasm component source could not be resolved")

    def _execute_job(self, job: dict, *, payload: dict) -> tuple[str, dict, dict]:
        kind = (job.get("kind") or "").strip().lower()
        policy = dict(job.get("policy") or {})
        spec = dict(job.get("spec") or self._normalize_job_spec({**job, "payload": payload}, requirements=job.get("requirements"), policy=policy, metadata=job.get("metadata")))
        execution = dict(spec.get("execution") or {})
        runtime_environment = dict(spec.get("runtime_environment") or {})
        if kind == "shell.command":
            argv = [str(part) for part in (execution.get("command") or [])]
            if not argv:
                raise MeshPolicyError("shell.command requires payload.command")
            cwd_path = self._resolve_runtime_cwd(runtime_environment, execution)
            timeout_seconds = int(execution.get("timeout_seconds") or 300)
            env, secret_delivery = self._build_runtime_env(job=job, payload=payload, spec=spec)
            resume_checkpoint_ref = self._job_resume_checkpoint_ref(job)
            if resume_checkpoint_ref:
                env["OCP_RESUME_ARTIFACT_ID"] = str(resume_checkpoint_ref.get("id") or "")
                env["OCP_RESUME_ARTIFACT_DIGEST"] = str(resume_checkpoint_ref.get("digest") or "")
                env["OCP_RESUME_ARTIFACT_MEDIA_TYPE"] = str(resume_checkpoint_ref.get("media_type") or "")
            completed = subprocess.run(
                argv,
                cwd=str(cwd_path),
                env=env,
                capture_output=True,
                text=True,
                timeout=max(1, timeout_seconds),
                check=False,
            )
            if completed.returncode != 0:
                raise MeshPolicyError(
                    f"shell.command exited with code {completed.returncode}: {completed.stderr.strip() or completed.stdout.strip()}"
                )
            return (
                "shell-worker",
                {
                    "status": "ok",
                    "argv": argv,
                    "cwd": str(cwd_path),
                    "runtime_environment": runtime_environment,
                    "exit_code": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                },
                {"secret_delivery": secret_delivery},
            )
        if kind == "python.inline":
            code = str(execution.get("inline_code") or "").strip()
            if not code:
                raise MeshPolicyError("python.inline requires payload.code")
            args = [str(part) for part in (execution.get("args") or [])]
            shell_payload = {
                "command": [sys.executable, "-c", code, *args],
                "cwd": execution.get("cwd") or "",
                "env": execution.get("env") or {},
                "env_policy": runtime_environment.get("env_policy") or {},
                "filesystem": (runtime_environment.get("filesystem") or {}),
                "secrets": dict(payload.get("secrets") or {}),
                "timeout_seconds": execution.get("timeout_seconds") or 300,
            }
            shell_job = {k: v for k, v in dict(job).items() if k != "spec"}
            shell_job["kind"] = "shell.command"
            return self._execute_job(shell_job, payload=shell_payload)
        if kind == "docker.container":
            if not self.docker_enabled:
                raise MeshPolicyError("docker runtime unavailable")
            image = str(execution.get("image") or "").strip()
            if not image:
                raise MeshPolicyError("docker.container requires payload.image")
            timeout_seconds = int(execution.get("timeout_seconds") or 300)
            env, secret_delivery = self._build_runtime_env(job=job, payload=payload, spec=spec)
            resume_checkpoint_ref = self._job_resume_checkpoint_ref(job)
            if resume_checkpoint_ref:
                env["OCP_RESUME_ARTIFACT_ID"] = str(resume_checkpoint_ref.get("id") or "")
                env["OCP_RESUME_ARTIFACT_DIGEST"] = str(resume_checkpoint_ref.get("digest") or "")
                env["OCP_RESUME_ARTIFACT_MEDIA_TYPE"] = str(resume_checkpoint_ref.get("media_type") or "")
            network_mode = str((runtime_environment.get("network") or {}).get("mode") or "default").strip().lower() or "default"
            if network_mode not in {"default", "bridge", "host", "none"}:
                raise MeshPolicyError(f"unsupported container network mode: {network_mode}")
            path_info = self._container_runtime_paths(runtime_environment, execution)
            container_name = f"ocp-{self.node_id[:16]}-{str(job.get('id') or uuid.uuid4().hex)[:12]}"
            docker_argv = ["docker", "run", "--rm", "--name", container_name]
            if network_mode != "default":
                docker_argv.extend(["--network", network_mode])
            if path_info["mount_workspace"]:
                docker_argv.extend(
                    [
                        "-v",
                        f"{self.workspace_root}:{path_info['container_root']}:rw",
                        "--workdir",
                        path_info["container_workdir"] or path_info["container_root"],
                    ]
                )
            for key in sorted(env):
                docker_argv.extend(["-e", f"{key}={env[key]}"])
            docker_argv.append(image)
            docker_argv.extend([str(part) for part in (execution.get("command") or [])])
            docker_argv.extend([str(part) for part in (execution.get("args") or [])])
            try:
                completed = subprocess.run(
                    docker_argv,
                    capture_output=True,
                    text=True,
                    timeout=max(1, timeout_seconds),
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                self._cleanup_docker_container(container_name)
                raise MeshPolicyError(f"docker.container timed out after {max(1, timeout_seconds)}s") from exc
            if completed.returncode != 0:
                raise MeshPolicyError(
                    f"docker.container exited with code {completed.returncode}: {completed.stderr.strip() or completed.stdout.strip()}"
                )
            return (
                "docker-worker",
                {
                    "status": "ok",
                    "image": image,
                    "command": [str(part) for part in (execution.get("command") or [])],
                    "args": [str(part) for part in (execution.get("args") or [])],
                    "docker_argv": docker_argv,
                    "container_name": container_name,
                    "network_mode": network_mode,
                    "mounted_workspace": bool(path_info["mount_workspace"]),
                    "cwd": str(path_info["host_workdir"] or self.workspace_root),
                    "runtime_environment": runtime_environment,
                    "exit_code": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                },
                {"secret_delivery": secret_delivery},
            )
        if kind == "wasm.component":
            if not self.wasm_enabled:
                raise MeshPolicyError("wasm runtime unavailable")
            component_path, resolved_component_ref = self._resolve_wasm_component_path(execution, payload)
            timeout_seconds = int(execution.get("timeout_seconds") or 300)
            env, secret_delivery = self._build_runtime_env(job=job, payload=payload, spec=spec)
            resume_checkpoint_ref = self._job_resume_checkpoint_ref(job)
            if resume_checkpoint_ref:
                env["OCP_RESUME_ARTIFACT_ID"] = str(resume_checkpoint_ref.get("id") or "")
                env["OCP_RESUME_ARTIFACT_DIGEST"] = str(resume_checkpoint_ref.get("digest") or "")
                env["OCP_RESUME_ARTIFACT_MEDIA_TYPE"] = str(resume_checkpoint_ref.get("media_type") or "")
            env["OCP_COMPONENT_ID"] = str(resolved_component_ref.get("id") or "")
            env["OCP_COMPONENT_DIGEST"] = str(resolved_component_ref.get("digest") or "")
            cwd_path = self._resolve_runtime_cwd(runtime_environment, execution)
            filesystem = dict(runtime_environment.get("filesystem") or {})
            network_mode = str((runtime_environment.get("network") or {}).get("mode") or "default").strip().lower() or "default"
            if network_mode not in {"default", "none"}:
                raise MeshPolicyError(f"unsupported wasm network mode: {network_mode}")
            wasm_argv = [self.wasm_runtime, "run"]
            entrypoint = str(execution.get("entrypoint") or "").strip()
            if entrypoint:
                wasm_argv.extend(["--invoke", entrypoint])
            if str(filesystem.get("profile") or "workspace").strip().lower() != "isolated":
                wasm_argv.extend(["--dir", str(cwd_path)])
            for key in sorted(env):
                wasm_argv.extend(["--env", f"{key}={env[key]}"])
            wasm_argv.append(str(component_path))
            wasm_argv.extend([str(part) for part in (execution.get("args") or [])])
            try:
                completed = subprocess.run(
                    wasm_argv,
                    cwd=str(cwd_path),
                    capture_output=True,
                    text=True,
                    timeout=max(1, timeout_seconds),
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                raise MeshPolicyError(f"wasm.component timed out after {max(1, timeout_seconds)}s") from exc
            if completed.returncode != 0:
                raise MeshPolicyError(
                    f"wasm.component exited with code {completed.returncode}: {completed.stderr.strip() or completed.stdout.strip()}"
                )
            return (
                "wasm-worker",
                {
                    "status": "ok",
                    "component_ref": resolved_component_ref,
                    "component_path": str(component_path),
                    "entrypoint": entrypoint,
                    "args": [str(part) for part in (execution.get("args") or [])],
                    "wasm_argv": wasm_argv,
                    "network_mode": network_mode,
                    "preopened_dir": "" if str(filesystem.get("profile") or "workspace").strip().lower() == "isolated" else str(cwd_path),
                    "cwd": str(cwd_path),
                    "runtime_environment": runtime_environment,
                    "exit_code": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                },
                {"secret_delivery": secret_delivery},
            )
        if kind == "agent.echo":
            return "agent-runtime", {"echo": payload, "status": "ok"}, {}
        if kind in {"mirror.metabolism.trigger", "host.runtime.trigger"}:
            if self.metabolism is None:
                raise MeshPolicyError("metabolism executor unavailable")
            local_job = self.metabolism.trigger(
                kind=(payload.get("kind") or "wake_maintenance"),
                topic=payload.get("topic"),
                payload=dict(payload.get("payload") or {}),
            )
            return "personal-mirror", {"status": "queued", "local_job": local_job}, {}
        if kind == "swarm.submit":
            if self.swarm is None:
                raise MeshPolicyError("swarm gateway unavailable")
            result = self.swarm.submit(payload)
            return "personal-mirror", result, {}
        if kind.startswith("golem.") or "golem-provider" in set(job.get("requirements", {}).get("capabilities") or []):
            result = self.golem_adapter.execute_job(kind, payload, policy)
            return "golem-mesh", result, {}
        raise MeshPolicyError(f"unsupported mesh job kind: {job.get('kind')}")

    def _artifact_descriptor(self, ref: dict, *, role: str = "", annotations: Optional[dict] = None) -> dict:
        merged_annotations = dict(annotations or {})
        if role:
            merged_annotations.setdefault("org.opencompute.role", role)
        descriptor = {
            "id": ref.get("id") or "",
            "digest": ref.get("digest") or "",
            "oci_digest": _oci_digest(ref.get("digest") or ""),
            "media_type": ref.get("media_type") or "application/octet-stream",
            "size_bytes": int(ref.get("size_bytes") or 0),
            "role": str(role or "").strip(),
            "annotations": merged_annotations,
        }
        descriptor["oci_descriptor"] = self._oci_descriptor(ref, annotations=merged_annotations)
        return descriptor

    def _publish_job_result_package(
        self,
        job: dict,
        *,
        result: Any,
        media_type: str,
        executor: str,
        result_artifact: Optional[dict] = None,
        attempt_id: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        job_metadata = dict(job.get("metadata") or {})
        package_metadata = dict(metadata or {})
        secret_delivery = [dict(item) for item in list(package_metadata.get("secret_delivery") or [])]
        result_ref = result_artifact or self.publish_local_artifact(
            result,
            media_type=media_type,
            policy=job["policy"],
            metadata={
                **package_metadata,
                "artifact_kind": package_metadata.get("artifact_kind") or "result",
                "job_id": job["id"],
                "attempt_id": attempt_id,
            },
        )
        descriptors = [self._artifact_descriptor(result_ref, role="result")]
        related_artifacts: dict[str, dict] = {}
        if isinstance(result, dict):
            for stream_name in ("stdout", "stderr"):
                content = result.get(stream_name)
                if content:
                    stream_ref = self.publish_local_artifact(
                        str(content),
                        media_type="text/plain; charset=utf-8",
                        policy=job["policy"],
                        metadata={
                            "artifact_kind": "log",
                            "log_stream": stream_name,
                            "retention_class": "session",
                            "job_id": job["id"],
                            "attempt_id": attempt_id,
                            "parent_artifact_id": result_ref["id"],
                        },
                    )
                    related_artifacts[stream_name] = stream_ref
                    descriptors.append(self._artifact_descriptor(stream_ref, role=stream_name))
            checkpoint_payload = result.get("checkpoint")
            if checkpoint_payload is not None:
                checkpoint_ref = self.publish_local_artifact(
                    checkpoint_payload,
                    media_type="application/json",
                    policy=job["policy"],
                    metadata={
                        "artifact_kind": "checkpoint",
                        "job_id": job["id"],
                        "attempt_id": attempt_id,
                        "parent_artifact_id": result_ref["id"],
                    },
                )
                related_artifacts["checkpoint"] = checkpoint_ref
                descriptors.append(self._artifact_descriptor(checkpoint_ref, role="checkpoint"))
        material_descriptors = [
            self._artifact_descriptor_from_input(item)
            for item in list(job.get("artifact_inputs") or [])
        ]
        config_payload = {
            "kind": "ocp.artifact.config",
            "schema_version": 1,
            "artifact_type": OCP_RESULT_ARTIFACT_TYPE,
            "created_at": _utcnow(),
            "job_id": job["id"],
            "request_id": job.get("request_id") or "",
            "attempt_id": attempt_id,
            "executor": executor,
            "result": {
                "artifact_id": result_ref["id"],
                "digest": result_ref["digest"],
                "media_type": result_ref["media_type"],
            },
            "runtime": {
                "kind": job.get("kind") or "",
                "dispatch_mode": (job.get("spec") or {}).get("dispatch_mode") or "",
                "secret_delivery": secret_delivery,
            },
            "policy": dict(job.get("policy") or {}),
        }
        config_ref = self.publish_local_artifact(
            config_payload,
            media_type=OCP_RESULT_CONFIG_MEDIA_TYPE,
            policy=job["policy"],
            metadata={
                "artifact_kind": "config",
                "artifact_type": OCP_RESULT_ARTIFACT_TYPE,
                "job_id": job["id"],
                "attempt_id": attempt_id,
                "result_artifact_id": result_ref["id"],
            },
        )
        attestation_payload = {
            "kind": "ocp.execution.attestation",
            "schema_version": 2,
            "issued_at": _utcnow(),
            "issuer": {
                "node_id": self.node_id,
                "display_name": self.display_name,
                "public_key": self.public_key,
                "signature_scheme": SIGNATURE_SCHEME,
            },
            "subject": {
                "artifact_id": result_ref["id"],
                "digest": result_ref["digest"],
                "media_type": result_ref["media_type"],
            },
            "subject_descriptor": self._oci_descriptor(
                result_ref,
                annotations={"org.opencompute.role": "result"},
            ),
            "predicate_type": "ocp.execution.result.v1",
            "predicate": {
                "job_id": job["id"],
                "request_id": job.get("request_id") or "",
                "attempt_id": attempt_id,
                "kind": job.get("kind") or "",
                "executor": executor,
                "policy": dict(job.get("policy") or {}),
                "artifact_inputs": list(job.get("artifact_inputs") or []),
                "materials": [item["oci_descriptor"] for item in material_descriptors],
                "result_descriptor": self._oci_descriptor(
                    result_ref,
                    annotations={"org.opencompute.role": "result"},
                ),
                "bundle_members": [descriptor["oci_descriptor"] for descriptor in descriptors],
                "secret_delivery": secret_delivery,
                "job_spec_digest": _sha256_bytes(_json_dump(job.get("spec") or {}).encode("utf-8")),
                "output_roles": [descriptor["role"] for descriptor in descriptors if descriptor.get("role")],
            },
            "verification": {
                "signature_scheme": SIGNATURE_SCHEME,
                "canonical_form": "json-c14n-sort-keys",
            },
        }
        attestation_signature = sign_message(
            self.private_key,
            _json_dump(attestation_payload).encode("utf-8"),
        )
        attestation_payload["signature"] = attestation_signature
        attestation_payload["verification"]["signed_payload_digest"] = _sha256_bytes(
            _json_dump({k: v for k, v in attestation_payload.items() if k != "signature"}).encode("utf-8")
        )
        attestation_ref = self.publish_local_artifact(
            attestation_payload,
            media_type="application/vnd.ocp.artifact.attestation.v1+json",
            policy=job["policy"],
            metadata={
                "artifact_kind": "attestation",
                "artifact_type": "application/vnd.ocp.execution.attestation.v1",
                "subject_artifact_id": result_ref["id"],
                "subject_digest": result_ref["digest"],
                "job_id": job["id"],
                "attempt_id": attempt_id,
                "predicate_type": attestation_payload["predicate_type"],
            },
        )
        descriptors.append(self._artifact_descriptor(attestation_ref, role="attestation"))
        bundle_manifest = {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST_MEDIA_TYPE,
            "artifactType": OCP_RESULT_ARTIFACT_TYPE,
            "config": self._oci_descriptor(
                config_ref,
                annotations={"org.opencompute.role": "config"},
            ),
            "layers": [descriptor["oci_descriptor"] for descriptor in descriptors],
            "subject": self._oci_descriptor(
                result_ref,
                annotations={"org.opencompute.role": "result"},
            ),
            "annotations": {
                "org.opencontainers.artifact.description": "Sovereign Mesh job result package",
                "org.opencompute.job.id": job["id"],
                "org.opencompute.request.id": job.get("request_id") or "",
                "org.opencompute.attempt.id": attempt_id,
                "org.opencompute.executor": executor,
            },
            "kind": "ocp.artifact.bundle",
            "schema_version": 1,
            "bundle_type": "job-result",
            "created_at": _utcnow(),
            "job_id": job["id"],
            "request_id": job.get("request_id") or "",
            "attempt_id": attempt_id,
            "executor": executor,
            "artifact_type": OCP_RESULT_ARTIFACT_TYPE,
            "primary": self._artifact_descriptor(result_ref, role="result"),
            "descriptors": descriptors,
        }
        bundle_ref = self.publish_local_artifact(
            bundle_manifest,
            media_type=OCI_MANIFEST_MEDIA_TYPE,
            policy=job["policy"],
            metadata={
                "artifact_kind": "bundle",
                "bundle_type": "job-result",
                "artifact_type": OCP_RESULT_ARTIFACT_TYPE,
                "job_id": job["id"],
                "attempt_id": attempt_id,
                "primary_artifact_id": result_ref["id"],
                "attestation_artifact_id": attestation_ref["id"],
                "config_artifact_id": config_ref["id"],
                "subject_artifact_id": result_ref["id"],
                "descriptor_count": len(descriptors),
            },
        )
        return {
            "result_ref": result_ref,
            "bundle_ref": bundle_ref,
            "config_ref": config_ref,
            "attestation_ref": attestation_ref,
            "related_artifacts": related_artifacts,
            "secret_delivery": secret_delivery,
        }

    def _ingest_job_submission(
        self,
        *,
        peer_id: str,
        request_id: str,
        job_body: dict,
        peer: Optional[dict],
    ) -> dict:
        existing = self._existing_job_by_request(request_id)
        if existing is not None:
            response = dict(existing)
            response["deduped"] = True
            return {"status": existing["status"], "job": response}

        job_body = dict(job_body or {})
        job_body.setdefault("origin", peer_id)
        job_body.setdefault("request_id", request_id)
        kind = (job_body.get("kind") or "").strip()
        if not kind:
            raise MeshPolicyError("job.kind is required")

        requirements = dict(job_body.get("requirements") or {})
        policy = _normalize_policy(job_body.get("policy") or {})
        if not self._policy_allows_peer(policy, peer):
            job = self._store_job_row(
                job_id=str(uuid.uuid4()),
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="rejected",
                metadata={"reason": "policy_denied"},
            )
            self._record_event(
                "mesh.job.rejected",
                peer_id=peer_id,
                request_id=request_id,
                payload={"job_id": job["id"], "reason": "policy_denied"},
            )
            return {"status": "rejected", "job": job}

        if dict(job_body.get("payload") or {}).get("secrets") and not policy.get("secret_scopes"):
            raise MeshPolicyError("payload.secrets requires explicit policy.secret_scopes")
        metadata = self._normalize_job_metadata(job_body.get("metadata") or {})
        spec = self._normalize_job_spec(
            job_body,
            requirements=requirements,
            policy=policy,
            metadata=metadata,
        )
        self._validate_normalized_job_spec(spec)
        requirements = dict(spec.get("requirements") or {})
        policy = _normalize_policy(spec.get("policy") or policy)
        metadata["job_spec"] = spec
        if not self._requirements_satisfied(requirements):
            job = self._store_job_row(
                job_id=str(uuid.uuid4()),
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="rejected",
                metadata={"reason": "requirements_unmet", "job_spec": spec},
            )
            return {"status": "rejected", "job": job}

        dispatch_mode = self._job_dispatch_mode(kind, job_body)
        if dispatch_mode == "queued":
            metadata["dispatch_mode"] = "queued"
            queue_name = self._queue_name_for_job(job_body, metadata)
            queue_policy = self._queue_policy_for_job(job_body, metadata, queue_name)
            metadata["queue_name"] = queue_name
            metadata["queue_policy"] = dict(queue_policy)
            dedupe_key = self._dedupe_key_for_job(job_body, metadata)
            if dedupe_key:
                metadata["dedupe_key"] = dedupe_key
                existing_queued = self._find_queued_job_by_dedupe_key(dedupe_key, queue_name=queue_name)
                if existing_queued is not None:
                    existing_queue = dict(existing_queued.get("queue") or {})
                    self._record_event(
                        "mesh.queue.deduped",
                        peer_id=peer_id,
                        request_id=request_id,
                        payload={
                            "job_id": existing_queued["id"],
                            "queue_message_id": existing_queue.get("id", ""),
                            "dedupe_key": dedupe_key,
                            "queue_name": queue_name,
                        },
                    )
                    return {"status": existing_queued["status"], "job": existing_queued, "deduped": True}
            queued_job = self._store_job_row(
                job_id=str(uuid.uuid4()),
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="queued",
                metadata=metadata,
            )
            queue_message = self._create_queue_message(
                job_id=queued_job["id"],
                queue_name=queue_name,
                dedupe_key=dedupe_key,
                queue_policy=queue_policy,
                metadata={"request_id": request_id, "kind": kind, "origin_peer_id": peer_id},
            )
            self._record_event(
                "mesh.job.queued",
                peer_id=peer_id,
                request_id=request_id,
                payload={"job_id": queued_job["id"], "kind": kind, "dispatch_mode": "queued"},
            )
            self._record_event(
                "mesh.queue.enqueued",
                peer_id=peer_id,
                request_id=request_id,
                payload={"job_id": queued_job["id"], "queue_message_id": queue_message["id"], "queue_name": queue_name},
            )
            return {"status": "queued", "job": self.get_job(queued_job["id"]), "queue_message": queue_message}

        job_id = str(uuid.uuid4())
        lease = self.acquire_lease(
            peer_id=peer_id,
            resource=(job_body.get("resource") or f"job:{job_id}"),
            agent_id=(job_body.get("agent_id") or "").strip(),
            job_id=job_id,
            ttl_seconds=int(job_body.get("ttl_seconds") or 300),
            metadata={"request_id": request_id, "job_kind": kind},
        )
        job = self._store_job_row(
            job_id=job_id,
            request_id=request_id,
            kind=kind,
            origin_peer_id=peer_id,
            target_peer_id=self.node_id,
            requirements=requirements,
            policy=policy,
            payload_ref=dict(job_body.get("payload_ref") or {}),
            payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="running",
                lease_id=lease["id"],
                metadata={"submitted_by": peer_id, **metadata},
            )
        self._record_event(
            "mesh.job.accepted",
            peer_id=peer_id,
            request_id=request_id,
            payload={"job_id": job_id, "kind": kind},
        )

        try:
            payload = self._resolve_job_payload(job_body)
            executor, result, completion_metadata = self._execute_job(job, payload=payload)
            result_package = self._publish_job_result_package(
                job,
                result=result,
                media_type="application/json",
                executor=executor,
                metadata={"job_id": job_id, "executor": executor, **dict(completion_metadata or {})},
            )
            result_artifact = result_package["result_ref"]
            job = self._store_job_row(
                job_id=job_id,
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="completed",
                result_ref=result_artifact,
                lease_id=lease["id"],
                executor=executor,
                metadata={
                    "submitted_by": peer_id,
                    **metadata,
                    "result_bundle_ref": result_package["bundle_ref"],
                    "result_config_ref": result_package["config_ref"],
                    "result_attestation_ref": result_package["attestation_ref"],
                    "result_artifacts": result_package["related_artifacts"],
                    "secret_delivery": result_package.get("secret_delivery") or [],
                },
            )
            self.release_lease(lease["id"], status="completed")
            job = self.get_job(job_id)
            self._record_event(
                "mesh.job.completed",
                peer_id=peer_id,
                request_id=request_id,
                payload={
                    "job_id": job_id,
                    "executor": executor,
                    "result_artifact_id": result_artifact["id"],
                    "bundle_artifact_id": result_package["bundle_ref"]["id"],
                },
            )
            return {"status": "completed", "job": job}
        except Exception as exc:
            self.release_lease(lease["id"], status="failed")
            job = self._store_job_row(
                job_id=job_id,
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="failed",
                lease_id=lease["id"],
                metadata={"submitted_by": peer_id, **metadata, "error": str(exc)},
            )
            self._record_event(
                "mesh.job.failed",
                peer_id=peer_id,
                request_id=request_id,
                payload={"job_id": job_id, "error": str(exc)},
            )
            raise

    def submit_local_job(self, job: dict, *, request_id: Optional[str] = None) -> dict:
        local_request_id = (request_id or uuid.uuid4().hex).strip()
        local_peer = {
            "peer_id": self.node_id,
            "organism_id": self.node_id,
            "display_name": self.display_name,
            "trust_tier": "self",
            "capability_cards": self.capability_cards(),
            "metadata": {},
        }
        return self._ingest_job_submission(
            peer_id=self.node_id,
            request_id=local_request_id,
            job_body=dict(job or {}),
            peer=local_peer,
        )

    def accept_job_submission(self, envelope: dict) -> dict:
        peer_id, request_meta, body, peer = self._verify_envelope(envelope, route="/mesh/jobs/submit")
        request_id = (request_meta.get("request_id") or "").strip()
        return self._ingest_job_submission(
            peer_id=peer_id,
            request_id=request_id,
            job_body=dict(body.get("job") or {}),
            peer=peer,
        )

    def schedule_job(
        self,
        job: dict,
        *,
        request_id: Optional[str] = None,
        preferred_peer_id: str = "",
        allow_local: bool = True,
        allow_remote: bool = True,
    ) -> dict:
        job = dict(job or {})
        effective_request_id = (request_id or uuid.uuid4().hex).strip()
        explicit_target = (job.get("target") or "").strip()
        if explicit_target and explicit_target != self.node_id:
            response = self.dispatch_job_to_peer(explicit_target, job, request_id=effective_request_id)
            decision = {
                "status": response.get("status") or "submitted",
                "decision": {
                    "status": "placed",
                    "strategy": "explicit-target",
                    "placement": self._normalized_placement(job),
                    "selected": {
                        "target_type": "peer",
                        "peer_id": explicit_target,
                        "score": self._trust_score("trusted"),
                        "reasons": ["explicit_target"],
                        "selected": True,
                    },
                    "candidates": [],
                },
                "job": response.get("job"),
                "response": response,
            }
            persisted = self._record_scheduler_decision(
                request_id=effective_request_id,
                job_id=((response.get("job") or {}).get("id") or "").strip(),
                job_kind=(job.get("kind") or "").strip(),
                decision=decision["decision"],
            )
            decision["decision"]["decision_id"] = persisted["id"]
            return decision
        decision = self.select_execution_target(
            job,
            request_id=effective_request_id,
            preferred_peer_id=preferred_peer_id,
            allow_local=allow_local,
            allow_remote=allow_remote,
        )
        if decision["status"] != "placed":
            raise MeshPolicyError("no eligible execution target found")
        selected = dict(decision["selected"] or {})
        target_type = selected.get("target_type") or "local"
        peer_id = (selected.get("peer_id") or self.node_id).strip() or self.node_id
        if target_type == "local" or peer_id == self.node_id:
            response = self.submit_local_job({**job, "target": self.node_id}, request_id=effective_request_id)
        else:
            response = self.dispatch_job_to_peer(peer_id, {**job, "target": peer_id}, request_id=effective_request_id)
        self._attach_job_id_to_scheduler_decision(
            decision.get("decision_id") or "",
            ((response.get("job") or {}).get("id") or "").strip(),
        )
        return {
            "status": response.get("status") or "submitted",
            "decision": decision,
            "job": response.get("job"),
            "response": response,
        }

    def dispatch_job_to_peer(
        self,
        peer_id: str,
        job: dict,
        *,
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> dict:
        remote_client, _ = self._resolve_peer_client(peer_id, client=client, base_url=base_url)
        envelope = self.build_signed_envelope("/mesh/jobs/submit", {"job": dict(job or {})}, request_id=request_id)
        response = remote_client.submit_job(envelope)
        self._record_event(
            "mesh.job.sent",
            peer_id=peer_id,
            request_id=envelope["request"]["request_id"],
            payload={"job_kind": (job or {}).get("kind") or "", "status": response.get("status")},
        )
        return response

    def get_job(self, job_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_jobs WHERE id=?", ((job_id or "").strip(),)).fetchone()
        if row is None:
            raise MeshPolicyError("job not found")
        return self._row_to_job(row)

    def cancel_job(self, job_id: str, *, reason: str = "cancelled") -> dict:
        job = self.get_job(job_id)
        if job["status"] in {"completed", "failed", "rejected"}:
            job["cancelled"] = False
            return job
        queue_message = self._queue_message_for_job(job_id)
        queue_policy = self._queue_policy_for_message(queue_message)
        lease = dict(job.get("lease") or {})
        if lease.get("id"):
            self.release_lease(lease["id"], status="cancelled")
        now = _utcnow()
        metadata = dict(job.get("metadata") or {})
        metadata["reason"] = reason
        metadata["cancelled_at"] = now
        metadata["current_attempt_id"] = ""
        with self._conn() as conn:
            queue_row = conn.execute("SELECT id FROM mesh_queue_messages WHERE job_id=?", (job_id,)).fetchone()
            conn.execute(
                "UPDATE mesh_jobs SET status='cancelled', metadata=?, updated_at=? WHERE id=?",
                (
                    json.dumps(metadata),
                    now,
                    job_id,
                ),
            )
            conn.execute(
                """
                UPDATE mesh_queue_messages
                SET status='cancelled', visibility_timeout_at='', lease_id='', worker_id='',
                    current_attempt_id='', last_error=?, replay_deadline_at=?, retention_deadline_at=?, updated_at=?
                WHERE job_id=?
                """,
                (
                    str(reason),
                    _utc_after(queue_policy["replay_window_seconds"]),
                    _utc_after(queue_policy["retention_seconds"]),
                    now,
                    job_id,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_jobs WHERE id=?", (job_id,)).fetchone()
        cancelled = self._row_to_job(row)
        if queue_row is not None:
            self._record_event(
                "mesh.queue.cancelled",
                peer_id=self.node_id,
                request_id=cancelled["request_id"],
                payload={"job_id": job_id, "queue_message_id": queue_row["id"], "reason": reason},
            )
        self._record_event(
            "mesh.job.cancelled",
            peer_id=cancelled["origin"],
            request_id=cancelled["request_id"],
            payload={"job_id": job_id, "reason": reason},
        )
        return cancelled

    def accept_handoff(self, envelope: dict) -> dict:
        peer_id, request_meta, body, _ = self._verify_envelope(envelope, route="/mesh/agents/handoff")
        request_id = (request_meta.get("request_id") or "").strip()
        existing = self._existing_handoff_by_request(request_id)
        if existing is not None:
            response = dict(existing)
            response["deduped"] = True
            return {"status": existing["status"], "handoff": response}
        handoff = dict(body.get("handoff") or {})
        summary = (handoff.get("summary") or "").strip()
        if not summary:
            raise MeshPolicyError("handoff.summary is required")
        packet = HandoffPacket(
            id=str(uuid.uuid4()),
            request_id=request_id,
            from_peer_id=peer_id,
            to_peer_id=(handoff.get("to_peer_id") or self.node_id).strip() or self.node_id,
            from_agent=(handoff.get("from_agent") or "").strip(),
            to_agent=(handoff.get("to_agent") or "").strip(),
            summary=summary,
            intent=(handoff.get("intent") or "").strip(),
            constraints=dict(handoff.get("constraints") or {}),
            artifact_refs=list(handoff.get("artifact_refs") or []),
            status="accepted",
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_handoffs
                (id, request_id, from_peer_id, to_peer_id, from_agent, to_agent, summary, intent, constraints, artifact_refs, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    packet.id,
                    packet.request_id,
                    packet.from_peer_id,
                    packet.to_peer_id,
                    packet.from_agent,
                    packet.to_agent,
                    packet.summary,
                    packet.intent,
                    json.dumps(packet.constraints),
                    json.dumps(packet.artifact_refs),
                    packet.status,
                    packet.created_at,
                    packet.updated_at,
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO handoff_packets
                (id, from_agent, to_agent, project_id, objective, context, resource_refs, approval_state, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'clear', ?, ?, ?)
                """,
                (
                    packet.id,
                    packet.from_agent or packet.from_peer_id,
                    packet.to_agent or packet.to_peer_id,
                    packet.constraints.get("project_id") or "",
                    packet.intent or packet.summary,
                    packet.summary,
                    json.dumps(packet.artifact_refs),
                    packet.status,
                    packet.created_at,
                    packet.updated_at,
                ),
            )
            conn.commit()
        self._record_event(
            "mesh.handoff.accepted",
            peer_id=peer_id,
            request_id=request_id,
            payload={"handoff_id": packet.id, "from_agent": packet.from_agent, "to_agent": packet.to_agent},
        )
        return {"status": "accepted", "handoff": packet.to_dict()}

    def handoff_to_peer(
        self,
        peer_id: str,
        handoff: dict,
        *,
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> dict:
        remote_client, _ = self._resolve_peer_client(peer_id, client=client, base_url=base_url)
        envelope = self.build_signed_envelope("/mesh/agents/handoff", {"handoff": dict(handoff or {})}, request_id=request_id)
        response = remote_client.submit_handoff(envelope)
        self._record_event(
            "mesh.handoff.sent",
            peer_id=peer_id,
            request_id=envelope["request"]["request_id"],
            payload={"summary": (handoff or {}).get("summary") or "", "status": response.get("status")},
        )
        return response

    def publish_notification(
        self,
        *,
        notification_type: str = "info",
        priority: str = "normal",
        title: str,
        body: str = "",
        compact_title: str = "",
        compact_body: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
        target_device_classes: Optional[list[str]] = None,
        related_job_id: str = "",
        related_approval_id: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        title_token = str(title or "").strip()
        if not title_token:
            raise MeshPolicyError("notification title is required")
        now = _utcnow()
        notification_id = str(uuid.uuid4())
        device_classes = _unique_tokens(target_device_classes)
        priority_token = str(priority or "normal").strip().lower() or "normal"
        if priority_token not in {"low", "normal", "high", "critical"}:
            priority_token = "normal"
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_notifications
                (id, notification_type, priority, title, body, compact_title, compact_body, status,
                 target_peer_id, target_agent_id, target_device_classes, related_job_id, related_approval_id,
                 metadata, created_at, updated_at, acked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'unread', ?, ?, ?, ?, ?, ?, ?, ?, '')
                """,
                (
                    notification_id,
                    str(notification_type or "info").strip().lower() or "info",
                    priority_token,
                    title_token,
                    str(body or ""),
                    str(compact_title or "").strip() or _compact_text(title_token, limit=48),
                    str(compact_body or "").strip() or _compact_text(body, limit=96),
                    str(target_peer_id or "").strip(),
                    str(target_agent_id or "").strip(),
                    json.dumps(device_classes),
                    str(related_job_id or "").strip(),
                    str(related_approval_id or "").strip(),
                    json.dumps(dict(metadata or {})),
                    now,
                    now,
                ),
            )
            row = conn.execute("SELECT * FROM mesh_notifications WHERE id=?", (notification_id,)).fetchone()
            conn.commit()
        notification = self._row_to_notification(row)
        self._record_event(
            "mesh.notification.published",
            peer_id=notification.get("target_peer_id") or self.node_id,
            payload={
                "notification_id": notification["id"],
                "notification_type": notification["notification_type"],
                "priority": notification["priority"],
                "target_peer_id": notification.get("target_peer_id") or "",
            },
        )
        return notification

    def list_notifications(
        self,
        *,
        limit: int = 25,
        status: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
    ) -> dict:
        status_token = _normalize_notification_status(status) if str(status or "").strip() else ""
        query = [
            "SELECT * FROM mesh_notifications WHERE 1=1",
        ]
        args: list[Any] = []
        if status_token:
            query.append("AND status=?")
            args.append(status_token)
        if str(target_peer_id or "").strip():
            query.append("AND target_peer_id=?")
            args.append(str(target_peer_id or "").strip())
        if str(target_agent_id or "").strip():
            query.append("AND target_agent_id=?")
            args.append(str(target_agent_id or "").strip())
        query.append("ORDER BY created_at DESC LIMIT ?")
        args.append(max(1, int(limit)))
        with self._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(args)).fetchall()
        notifications = [self._row_to_notification(row) for row in rows]
        return {"peer_id": self.node_id, "count": len(notifications), "notifications": notifications}

    def ack_notification(
        self,
        notification_id: str,
        *,
        status: str = "acked",
        actor_peer_id: str = "",
        actor_agent_id: str = "",
        reason: str = "",
    ) -> dict:
        row = None
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_notifications WHERE id=?", ((notification_id or "").strip(),)).fetchone()
        if row is None:
            raise MeshPolicyError("notification not found")
        status_token = _normalize_notification_status(status)
        now = _utcnow()
        metadata = _loads_json(row["metadata"], {})
        metadata["last_actor_peer_id"] = str(actor_peer_id or "")
        metadata["last_actor_agent_id"] = str(actor_agent_id or "")
        metadata["last_ack_reason"] = str(reason or "")
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_notifications
                SET status=?, metadata=?, updated_at=?, acked_at=?
                WHERE id=?
                """,
                (status_token, json.dumps(metadata), now, now if status_token != "unread" else "", (notification_id or "").strip()),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_notifications WHERE id=?", ((notification_id or "").strip(),)).fetchone()
        notification = self._row_to_notification(fresh)
        self._record_event(
            "mesh.notification.acked",
            peer_id=notification.get("target_peer_id") or self.node_id,
            payload={"notification_id": notification["id"], "status": notification["status"]},
        )
        return notification

    def create_approval_request(
        self,
        *,
        title: str,
        summary: str = "",
        action_type: str = "operator_action",
        severity: str = "normal",
        request_id: str = "",
        requested_by_peer_id: str = "",
        requested_by_agent_id: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
        target_device_classes: Optional[list[str]] = None,
        related_job_id: str = "",
        expires_at: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        title_token = str(title or "").strip()
        if not title_token:
            raise MeshPolicyError("approval title is required")
        request_token = str(request_id or "").strip()
        if request_token:
            with self._conn() as conn:
                existing = conn.execute("SELECT * FROM mesh_approvals WHERE request_id=?", (request_token,)).fetchone()
            if existing is not None:
                approval = self._row_to_approval(existing)
                approval["deduped"] = True
                return {"status": approval["status"], "approval": approval}
        severity_token = str(severity or "normal").strip().lower() or "normal"
        if severity_token not in {"low", "normal", "high", "critical"}:
            severity_token = "normal"
        notification = self.publish_notification(
            notification_type="approval.request",
            priority="critical" if severity_token == "critical" else ("high" if severity_token in {"high", "critical"} else "normal"),
            title=title_token,
            body=summary,
            target_peer_id=target_peer_id,
            target_agent_id=target_agent_id,
            target_device_classes=target_device_classes,
            related_job_id=related_job_id,
            metadata={"action_type": action_type, "approval_request": True},
        )
        approval_id = str(uuid.uuid4())
        now = _utcnow()
        device_classes = _unique_tokens(target_device_classes)
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_approvals
                (id, request_id, action_type, severity, title, summary, compact_summary, status,
                 requested_by_peer_id, requested_by_agent_id, target_peer_id, target_agent_id, target_device_classes,
                 related_job_id, notification_id, resolution, metadata, created_at, updated_at, expires_at, resolved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?, ?, '')
                """,
                (
                    approval_id,
                    request_token or None,
                    str(action_type or "operator_action").strip().lower() or "operator_action",
                    severity_token,
                    title_token,
                    str(summary or ""),
                    _compact_text(summary or title_token, limit=96),
                    str(requested_by_peer_id or "").strip(),
                    str(requested_by_agent_id or "").strip(),
                    str(target_peer_id or "").strip(),
                    str(target_agent_id or "").strip(),
                    json.dumps(device_classes),
                    str(related_job_id or "").strip(),
                    notification["id"],
                    json.dumps(dict(metadata or {})),
                    now,
                    now,
                    str(expires_at or "").strip(),
                ),
            )
            row = conn.execute("SELECT * FROM mesh_approvals WHERE id=?", (approval_id,)).fetchone()
            conn.commit()
        approval = self._row_to_approval(row)
        self._record_event(
            "mesh.approval.requested",
            peer_id=approval.get("target_peer_id") or self.node_id,
            request_id=request_token,
            payload={
                "approval_id": approval["id"],
                "action_type": approval["action_type"],
                "target_peer_id": approval.get("target_peer_id") or "",
                "severity": approval["severity"],
            },
        )
        return {"status": "pending", "approval": approval, "notification": notification}

    def _expire_pending_approvals(self) -> int:
        now = _utcnow()
        expired = 0
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_approvals
                WHERE status='pending' AND expires_at != '' AND expires_at < ?
                """,
                (now,),
            ).fetchall()
            for row in rows:
                resolution = {"decision": "expired", "resolved_at": now}
                conn.execute(
                    """
                    UPDATE mesh_approvals
                    SET status='expired', resolution=?, updated_at=?, resolved_at=?
                    WHERE id=?
                    """,
                    (json.dumps(resolution), now, now, row["id"]),
                )
                expired += 1
            conn.commit()
        return expired

    def list_approvals(
        self,
        *,
        limit: int = 25,
        status: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
    ) -> dict:
        self._expire_pending_approvals()
        status_token = _normalize_approval_status(status) if str(status or "").strip() else ""
        query = [
            "SELECT * FROM mesh_approvals WHERE 1=1",
        ]
        args: list[Any] = []
        if status_token:
            query.append("AND status=?")
            args.append(status_token)
        if str(target_peer_id or "").strip():
            query.append("AND target_peer_id=?")
            args.append(str(target_peer_id or "").strip())
        if str(target_agent_id or "").strip():
            query.append("AND target_agent_id=?")
            args.append(str(target_agent_id or "").strip())
        query.append("ORDER BY created_at DESC LIMIT ?")
        args.append(max(1, int(limit)))
        with self._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(args)).fetchall()
        approvals = [self._row_to_approval(row) for row in rows]
        return {"peer_id": self.node_id, "count": len(approvals), "approvals": approvals}

    def resolve_approval(
        self,
        approval_id: str,
        *,
        decision: str,
        operator_peer_id: str = "",
        operator_agent_id: str = "",
        reason: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        self._expire_pending_approvals()
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_approvals WHERE id=?", ((approval_id or "").strip(),)).fetchone()
        if row is None:
            raise MeshPolicyError("approval not found")
        approval = self._row_to_approval(row)
        if approval["status"] != "pending":
            return {"status": approval["status"], "approval": approval}
        decision_token = str(decision or "").strip().lower()
        if decision_token not in {"approved", "rejected", "deferred"}:
            raise MeshPolicyError("unsupported approval decision")
        now = _utcnow()
        resolution = {
            "decision": decision_token,
            "reason": str(reason or ""),
            "operator_peer_id": str(operator_peer_id or ""),
            "operator_agent_id": str(operator_agent_id or ""),
            "metadata": dict(metadata or {}),
            "resolved_at": now,
        }
        merged_metadata = dict(approval.get("metadata") or {})
        merged_metadata["last_resolution"] = resolution
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_approvals
                SET status=?, resolution=?, metadata=?, updated_at=?, resolved_at=?
                WHERE id=?
                """,
                (decision_token, json.dumps(resolution), json.dumps(merged_metadata), now, now, (approval_id or "").strip()),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_approvals WHERE id=?", ((approval_id or "").strip(),)).fetchone()
        updated = self._row_to_approval(fresh)
        resolution_notification = self.publish_notification(
            notification_type="approval.resolved",
            priority="normal",
            title=f"Approval {decision_token}",
            body=updated["title"],
            target_peer_id=updated.get("requested_by_peer_id") or "",
            target_agent_id=updated.get("requested_by_agent_id") or "",
            target_device_classes=updated.get("target_device_classes") or [],
            related_job_id=updated.get("related_job_id") or "",
            related_approval_id=updated["id"],
            metadata={"decision": decision_token, "reason": str(reason or "")},
        )
        self._record_event(
            "mesh.approval.resolved",
            peer_id=updated.get("target_peer_id") or self.node_id,
            payload={"approval_id": updated["id"], "decision": decision_token},
        )
        response = {"status": decision_token, "approval": updated, "notification": resolution_notification}
        if decision_token == "approved":
            response["automation"] = self._apply_autonomous_offload_approval(
                updated,
                decision=decision_token,
                operator_peer_id=operator_peer_id,
                operator_agent_id=operator_agent_id,
                reason=reason,
            )
        elif decision_token in {"rejected", "deferred"}:
            response["automation"] = self._apply_autonomous_offload_approval(
                updated,
                decision=decision_token,
                operator_peer_id=operator_peer_id,
                operator_agent_id=operator_agent_id,
                reason=reason,
            )
        return response

    def notify_peer(
        self,
        peer_id: str,
        notification: dict,
        *,
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
    ) -> dict:
        remote_client, _ = self._resolve_peer_client(peer_id, client=client, base_url=base_url)
        payload = dict(notification or {})
        payload.setdefault("target_peer_id", peer_id)
        response = remote_client.publish_notification(payload)
        self._record_event(
            "mesh.notification.sent",
            peer_id=peer_id,
            payload={"status": response.get("status"), "title": payload.get("title") or ""},
        )
        return response

    def request_approval_from_peer(
        self,
        peer_id: str,
        approval: dict,
        *,
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
    ) -> dict:
        remote_client, _ = self._resolve_peer_client(peer_id, client=client, base_url=base_url)
        payload = dict(approval or {})
        payload.setdefault("target_peer_id", peer_id)
        response = remote_client.request_approval(payload)
        self._record_event(
            "mesh.approval.sent",
            peer_id=peer_id,
            request_id=str(payload.get("request_id") or ""),
            payload={"status": response.get("status"), "title": payload.get("title") or ""},
        )
        return response

    def publish_artifact_to_peer(
        self,
        peer_id: str,
        artifact: dict,
        *,
        client: Optional[MeshPeerClient] = None,
        base_url: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> dict:
        remote_client, _ = self._resolve_peer_client(peer_id, client=client, base_url=base_url)
        envelope = self.build_signed_envelope("/mesh/artifacts/publish", {"artifact": dict(artifact or {})}, request_id=request_id)
        response = remote_client.publish_artifact(envelope)
        self._record_event(
            "mesh.artifact.sent",
            peer_id=peer_id,
            request_id=envelope["request"]["request_id"],
            payload={"status": response.get("status"), "artifact_digest": (response.get("artifact") or {}).get("digest")},
        )
        return response

    def _row_to_notification(self, row) -> Optional[dict]:
        if row is None:
            return None
        target_device_classes = _loads_json(row["target_device_classes"], [])
        compact_title = row["compact_title"] or _compact_text(row["title"] or "", limit=48)
        compact_body = row["compact_body"] or _compact_text(row["body"] or "", limit=96)
        return {
            "id": row["id"],
            "notification_type": row["notification_type"] or "info",
            "priority": row["priority"] or "normal",
            "title": row["title"] or "",
            "body": row["body"] or "",
            "compact_title": compact_title,
            "compact_body": compact_body,
            "status": _normalize_notification_status(row["status"]),
            "target_peer_id": row["target_peer_id"] or "",
            "target_agent_id": row["target_agent_id"] or "",
            "target_device_classes": target_device_classes,
            "related_job_id": row["related_job_id"] or "",
            "related_approval_id": row["related_approval_id"] or "",
            "presentation": {
                "compact": bool(set(target_device_classes) & {"light", "micro"}),
                "compact_title": compact_title,
                "compact_body": compact_body,
            },
            "metadata": _loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "acked_at": row["acked_at"] or "",
        }

    def _row_to_approval(self, row) -> Optional[dict]:
        if row is None:
            return None
        return {
            "id": row["id"],
            "request_id": row["request_id"] or "",
            "action_type": row["action_type"] or "operator_action",
            "severity": row["severity"] or "normal",
            "title": row["title"] or "",
            "summary": row["summary"] or "",
            "compact_summary": row["compact_summary"] or _compact_text(row["summary"] or row["title"] or "", limit=96),
            "status": _normalize_approval_status(row["status"]),
            "requested_by_peer_id": row["requested_by_peer_id"] or "",
            "requested_by_agent_id": row["requested_by_agent_id"] or "",
            "target_peer_id": row["target_peer_id"] or "",
            "target_agent_id": row["target_agent_id"] or "",
            "target_device_classes": _loads_json(row["target_device_classes"], []),
            "related_job_id": row["related_job_id"] or "",
            "notification_id": row["notification_id"] or "",
            "resolution": _loads_json(row["resolution"], {}),
            "metadata": _loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "expires_at": row["expires_at"] or "",
            "resolved_at": row["resolved_at"] or "",
        }

    def _peer_device_profile(self, peer: Optional[dict]) -> dict:
        source = dict(peer or {})
        card = dict(source.get("card") or {})
        metadata = dict(source.get("metadata") or {})
        return _normalize_device_profile(
            source.get("device_profile")
            or card.get("device_profile")
            or metadata.get("remote_device_profile")
            or {}
        )

    def _row_to_peer(self, row) -> Optional[dict]:
        if row is None:
            return None
        metadata = _loads_json(row["metadata"], {})
        card = _loads_json(row["card"], {})
        peer_stub = {
            "peer_id": row["peer_id"],
            "trust_tier": _normalize_trust_tier(row["trust_tier"]),
            "metadata": metadata,
            "capability_cards": _loads_json(row["capability_cards"], []),
            "card": card,
        }
        device_profile = self._peer_device_profile({"metadata": metadata, "card": card})
        sync_policy = dict(metadata.get("remote_sync_policy") or self._device_profile_sync_policy(device_profile))
        return {
            "peer_id": row["peer_id"],
            "organism_id": row["peer_id"],
            "display_name": row["display_name"],
            "public_key": row["public_key"],
            "signature_scheme": row["signature_scheme"],
            "endpoint_url": row["endpoint_url"],
            "stream_url": row["stream_url"],
            "trust_tier": _normalize_trust_tier(row["trust_tier"]),
            "reachability": row["reachability"],
            "status": row["status"],
            "mesh_session_id": row["mesh_session_id"],
            "protocol_version": row["protocol_version"],
            "capability_cards": _loads_json(row["capability_cards"], []),
            "card": card,
            "device_profile": device_profile,
            "sync_policy": sync_policy,
            "metadata": metadata,
            "reliability": self._peer_reliability_summary(peer_stub),
            "load": self._peer_load_summary(peer_stub),
            "sync_state": {
                "remote_cursor": int(metadata.get("remote_cursor") or 0),
                "last_sync_at": metadata.get("last_sync_at") or "",
                "last_imported_event_count": int(metadata.get("last_imported_event_count") or 0),
                "last_sync_error": metadata.get("last_sync_error") or "",
            },
            "heartbeat": dict(metadata.get("heartbeat") or {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_seen_at": row["last_seen_at"],
            "last_handshake_at": row["last_handshake_at"],
        }

    def _row_to_event(self, row) -> dict:
        return {
            "seq": int(row["seq"]),
            "id": row["id"],
            "event_type": row["event_type"],
            "peer_id": row["peer_id"] or "",
            "request_id": row["request_id"] or "",
            "payload": _loads_json(row["payload"], {}),
            "created_at": row["created_at"],
        }

    def _row_to_scheduler_decision(self, row) -> Optional[dict]:
        if row is None:
            return None
        return {
            "id": row["id"],
            "request_id": row["request_id"] or "",
            "job_id": row["job_id"] or "",
            "job_kind": row["job_kind"] or "",
            "status": row["status"] or "",
            "strategy": row["strategy"] or "",
            "target_type": row["target_type"] or "",
            "peer_id": row["peer_id"] or "",
            "score": int(row["score"] or 0),
            "placement": _loads_json(row["placement"], {}),
            "selected": _loads_json(row["selected"], {}),
            "candidates": _loads_json(row["candidates"], []),
            "created_at": row["created_at"],
        }

    def _row_to_lease(self, row) -> dict:
        return LeaseRecord(
            id=row["id"],
            resource=row["resource"],
            peer_id=row["peer_id"],
            agent_id=row["agent_id"] or "",
            job_id=row["job_id"] or "",
            status=row["status"],
            ttl_seconds=int(row["ttl_seconds"] or 300),
            lock_token=row["lock_token"] or "",
            metadata=_loads_json(row["metadata"], {}),
            created_at=row["created_at"],
            heartbeat_at=row["heartbeat_at"],
            expires_at=row["expires_at"],
            released_at=row["released_at"] or "",
        ).to_dict()

    def _row_to_artifact(self, row) -> dict:
        metadata = _loads_json(row["metadata"], {})
        artifact = ArtifactRef(
            id=row["id"],
            digest=row["digest"],
            media_type=row["media_type"],
            size_bytes=int(row["size_bytes"] or 0),
            owner_peer_id=row["owner_peer_id"],
            policy=_normalize_policy(_loads_json(row["policy"], {})),
            path=row["path"],
            created_at=row["created_at"],
            metadata=metadata,
            retention_class=_normalize_retention_class(row["retention_class"] or metadata.get("retention_class")),
            retention_deadline_at=row["retention_deadline_at"] or "",
            download_url=f"{self.base_url}/mesh/artifacts/{row['id']}",
        ).to_dict()
        return artifact | {
            "artifact_kind": str(metadata.get("artifact_kind") or "").strip(),
            "artifact_type": str(metadata.get("artifact_type") or "").strip(),
            "pinned": self._artifact_is_pinned({"metadata": metadata}),
            "artifact_sync": dict(metadata.get("artifact_sync") or {}),
            "mirror_verification": dict(metadata.get("mirror_verification") or {}),
            "oci_descriptor": self._oci_descriptor(artifact, annotations=dict(metadata.get("oci_annotations") or {})),
        }

    def _row_to_queue_message(self, row) -> Optional[dict]:
        if row is None:
            return None
        return QueueMessage(
            id=row["id"],
            job_id=row["job_id"],
            queue_name=row["queue_name"] or "default",
            status=row["status"] or "queued",
            dedupe_key=row["dedupe_key"] or "",
            ack_deadline_seconds=int(row["ack_deadline_seconds"] or 300),
            dead_letter_queue=row["dead_letter_queue"] or "",
            delivery_attempts=int(row["delivery_attempts"] or 0),
            visibility_timeout_at=row["visibility_timeout_at"] or "",
            available_at=row["available_at"] or "",
            claimed_at=row["claimed_at"] or "",
            acked_at=row["acked_at"] or "",
            replay_deadline_at=row["replay_deadline_at"] or "",
            retention_deadline_at=row["retention_deadline_at"] or "",
            lease_id=row["lease_id"] or "",
            worker_id=row["worker_id"] or "",
            current_attempt_id=row["current_attempt_id"] or "",
            last_error=row["last_error"] or "",
            metadata=_loads_json(row["metadata"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        ).to_dict()

    def _row_to_job(self, row) -> dict:
        lease = {}
        lease_id = (row["lease_id"] or "").strip()
        if lease_id:
            lease_row = self._lease_row(lease_id)
            if lease_row is not None:
                lease = self._row_to_lease(lease_row)
        attempts = [self._row_to_attempt(attempt_row) for attempt_row in self._list_attempt_rows(row["id"])]
        queue_message = self._queue_message_for_job(row["id"])
        metadata = _loads_json(row["metadata"], {})
        spec = dict(metadata.get("job_spec") or {})
        if not spec:
            spec = self._normalize_job_spec(
                {
                    "kind": row["kind"],
                    "origin": row["origin_peer_id"],
                    "request_id": row["request_id"],
                    "payload": _loads_json(row["payload_inline"], {}),
                    "payload_ref": _loads_json(row["payload_ref"], {}),
                    "artifact_inputs": _loads_json(row["artifact_inputs"], []),
                    "requirements": _loads_json(row["requirements"], {}),
                    "policy": _loads_json(row["policy"], {}),
                    "metadata": metadata,
                    "created_at": row["created_at"],
                },
                requirements=_loads_json(row["requirements"], {}),
                policy=_loads_json(row["policy"], {}),
                metadata=metadata,
            )
        recovery = self._job_recovery_contract({"status": row["status"], "metadata": metadata, "spec": spec}, metadata=metadata, spec=spec)
        return MeshJob(
            id=row["id"],
            request_id=row["request_id"],
            kind=row["kind"],
            origin=row["origin_peer_id"],
            target=row["target_peer_id"],
            requirements=_loads_json(row["requirements"], {}),
            policy=_normalize_policy(_loads_json(row["policy"], {})),
            payload_ref=_loads_json(row["payload_ref"], {}),
            artifact_inputs=_loads_json(row["artifact_inputs"], []),
            status=row["status"],
            result_ref=_loads_json(row["result_ref"], {}),
            lease=lease,
            metadata=metadata,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        ).to_dict() | {
            "payload_inline": _loads_json(row["payload_inline"], {}),
            "executor": row["executor"] or "",
            "attempts": attempts,
            "queue": queue_message,
            "spec": spec,
            "mission": dict(metadata.get("mission") or {}),
            "latest_checkpoint_ref": dict(metadata.get("latest_checkpoint_ref") or {}),
            "resume_checkpoint_ref": dict(metadata.get("resume_checkpoint_ref") or {}),
            "selected_resume_checkpoint_ref": dict(metadata.get("resume_checkpoint_ref") or {}),
            "resume_count": int(metadata.get("resume_count") or 0),
            "checkpointed_at": str(metadata.get("checkpointed_at") or ""),
            "last_resumed_at": str(metadata.get("last_resumed_at") or ""),
            "last_resumed_by": str(metadata.get("last_resumed_by") or ""),
            "last_resume_reason": str(metadata.get("last_resume_reason") or ""),
            "last_resume_requested_at": str(metadata.get("last_resume_requested_at") or ""),
            "last_resume_requested_by": str(metadata.get("last_resume_requested_by") or ""),
            "last_resume_requested_reason": str(metadata.get("last_resume_requested_reason") or ""),
            "last_restart_at": str(metadata.get("last_restart_at") or ""),
            "last_restart_by": str(metadata.get("last_restart_by") or ""),
            "last_restart_reason": str(metadata.get("last_restart_reason") or ""),
            "recovery": recovery,
            "result_bundle_ref": dict(metadata.get("result_bundle_ref") or {}),
            "result_config_ref": dict(metadata.get("result_config_ref") or {}),
            "result_attestation_ref": dict(metadata.get("result_attestation_ref") or {}),
            "result_artifacts": dict(metadata.get("result_artifacts") or {}),
            "secret_delivery": [dict(item) for item in list(metadata.get("secret_delivery") or [])],
        }

    def _row_to_mission(self, row) -> Optional[dict]:
        if row is None:
            return None
        mission = MissionRecord(
            id=row["id"],
            request_id=row["request_id"] or "",
            title=row["title"] or "",
            intent=row["intent"] or "",
            status=_normalize_mission_status(row["status"]),
            priority=_normalize_mission_priority(row["priority"]),
            workload_class=_normalize_workload_class(row["workload_class"]),
            origin_peer_id=row["origin_peer_id"] or self.node_id,
            target_strategy=_normalize_target_strategy(row["target_strategy"]),
            policy=_normalize_mission_policy(_loads_json(row["policy"], {})),
            continuity=_normalize_mission_continuity(_loads_json(row["continuity"], {})),
            metadata=_loads_json(row["metadata"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        ).to_dict()
        return mission | {
            "child_job_ids": _unique_tokens(_loads_json(row["child_job_ids"], [])),
            "cooperative_task_ids": _unique_tokens(_loads_json(row["cooperative_task_ids"], [])),
            "latest_checkpoint_ref": dict(_loads_json(row["latest_checkpoint_ref"], {})),
            "result_ref": dict(_loads_json(row["result_ref"], {})),
            "result_bundle_ref": dict(_loads_json(row["result_bundle_ref"], {})),
        }

    def _row_to_handoff(self, row) -> dict:
        return HandoffPacket(
            id=row["id"],
            request_id=row["request_id"],
            from_peer_id=row["from_peer_id"],
            to_peer_id=row["to_peer_id"],
            from_agent=row["from_agent"] or "",
            to_agent=row["to_agent"] or "",
            summary=row["summary"],
            intent=row["intent"] or "",
            constraints=_loads_json(row["constraints"], {}),
            artifact_refs=_loads_json(row["artifact_refs"], []),
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        ).to_dict()

    def _row_to_worker(self, row) -> Optional[dict]:
        if row is None:
            return None
        return WorkerCard(
            id=row["id"],
            peer_id=row["peer_id"],
            agent_id=row["agent_id"] or "",
            status=row["status"],
            capabilities=_loads_json(row["capabilities"], []),
            resources=_loads_json(row["resources"], {}),
            labels=_loads_json(row["labels"], []),
            max_concurrent_jobs=int(row["max_concurrent_jobs"] or 1),
            metadata=_loads_json(row["metadata"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_heartbeat_at=row["last_heartbeat_at"],
        ).to_dict() | {
            "active_attempts": self._worker_active_attempts(row["id"]),
        }

    def _row_to_attempt(self, row) -> dict:
        return JobAttempt(
            id=row["id"],
            job_id=row["job_id"],
            attempt_number=int(row["attempt_number"] or 1),
            worker_id=row["worker_id"],
            status=row["status"],
            lease_id=row["lease_id"] or "",
            executor=row["executor"] or "",
            result_ref=_loads_json(row["result_ref"], {}),
            error=row["error"] or "",
            metadata=_loads_json(row["metadata"], {}),
            started_at=row["started_at"],
            heartbeat_at=row["heartbeat_at"],
            finished_at=row["finished_at"] or "",
        ).to_dict()
