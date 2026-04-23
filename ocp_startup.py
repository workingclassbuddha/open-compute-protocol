from __future__ import annotations

import ipaddress
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class StartupPaths:
    state_dir: Path
    db_path: Path
    identity_dir: Path
    workspace_root: Path

    def as_strings(self) -> dict[str, str]:
        return {key: str(value) for key, value in asdict(self).items()}


@dataclass(frozen=True)
class StartupProfile:
    host: str
    port: int
    node_id: str
    display_name: str
    device_class: str
    form_factor: str
    db_path: Path
    identity_dir: Path
    workspace_root: Path
    base_url: str = ""


def slugify(value: str) -> str:
    chars = []
    last_dash = False
    for ch in (value or "").lower():
        if ch.isalnum():
            chars.append(ch)
            last_dash = False
            continue
        if not last_dash:
            chars.append("-")
            last_dash = True
    return "".join(chars).strip("-")


def default_node_id(host_name: str | None = None) -> str:
    name = host_name
    if name is None:
        name = os.uname().nodename if hasattr(os, "uname") else os.environ.get("COMPUTERNAME", "ocp")
    token = slugify(str(name or "")) or "ocp"
    return f"{token}-node"


def display_host_for_browser(host: str) -> str:
    if host in {"0.0.0.0", "::", ""}:
        return "127.0.0.1"
    return host


def is_wildcard_host(host: str) -> bool:
    return str(host or "").strip().lower() in {"", "0.0.0.0", "::", "[::]"}


def is_loopback_host(host: str) -> bool:
    token = str(host or "").strip().lower()
    return token == "localhost" or token.startswith("127.")


def discover_local_ipv4_addresses(*, bind_host: str = "") -> list[str]:
    seen: set[str] = set()
    bind_token = str(bind_host or "").strip()
    if bind_token and not is_wildcard_host(bind_token) and not is_loopback_host(bind_token):
        try:
            if ipaddress.ip_address(bind_token).version == 4:
                seen.add(bind_token)
        except ValueError:
            pass
    try:
        addrinfo_rows = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET, socket.SOCK_DGRAM)
    except OSError:
        addrinfo_rows = []
    for family, _, _, _, sockaddr in addrinfo_rows:
        if family != socket.AF_INET or not sockaddr:
            continue
        host = str(sockaddr[0] or "").strip()
        if host and not is_wildcard_host(host) and not is_loopback_host(host):
            seen.add(host)
    for probe_host in ("192.0.2.1", "10.255.255.255"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((probe_host, 80))
                host = str(sock.getsockname()[0] or "").strip()
                if host and not is_wildcard_host(host) and not is_loopback_host(host):
                    seen.add(host)
        except OSError:
            continue
    return sorted(
        (host for host in seen if host and ipaddress.ip_address(host).version == 4),
        key=lambda host: (not ipaddress.ip_address(host).is_private, host),
    )


def share_urls_for_host(
    host: str,
    port: int,
    *,
    discover_ipv4: Callable[..., list[str]] | None = None,
) -> list[str]:
    token = str(host or "").strip()
    if token and not is_wildcard_host(token) and not is_loopback_host(token):
        return [f"http://{token}:{int(port)}/"]
    if is_wildcard_host(token):
        discover = discover_ipv4 or discover_local_ipv4_addresses
        return [f"http://{address}:{int(port)}/" for address in discover(bind_host=token)]
    return []


def build_open_url(host: str, port: int, path: str = "/") -> str:
    route = path if str(path or "").startswith("/") else f"/{path}"
    route = route or "/"
    return f"http://{display_host_for_browser(host)}:{int(port)}{route}"


def health_url(host: str, port: int) -> str:
    return build_open_url(host, port, "/mesh/manifest")


def default_repo_state_dir(repo_root: Path) -> Path:
    return Path(repo_root) / ".local" / "ocp"


def default_launcher_support_dir(*, home: Path | None = None) -> Path:
    base = Path(home) if home is not None else Path.home()
    return base / "Library" / "Application Support" / "OCP"


def default_launcher_config_path(*, home: Path | None = None) -> Path:
    return default_launcher_support_dir(home=home) / "launcher.json"


def default_launcher_state_dir(*, home: Path | None = None) -> Path:
    return default_launcher_support_dir(home=home) / "state"


def resolve_state_paths(
    repo_root: Path,
    *,
    db_path: str | Path = "",
    identity_dir: str | Path = "",
    workspace_root: str | Path = "",
    state_dir: str | Path | None = None,
    create: bool = True,
) -> StartupPaths:
    root = Path(repo_root).resolve()
    explicit_db = Path(db_path).expanduser() if db_path else None
    if state_dir:
        resolved_state = Path(state_dir).expanduser()
    elif explicit_db is not None:
        resolved_state = explicit_db.parent
    else:
        resolved_state = default_repo_state_dir(root)
    db = explicit_db if explicit_db is not None else resolved_state / "ocp.db"
    identity = Path(identity_dir).expanduser() if identity_dir else resolved_state / "identity"
    workspace = Path(workspace_root).expanduser() if workspace_root else resolved_state / "workspace"
    paths = StartupPaths(
        state_dir=resolved_state,
        db_path=db,
        identity_dir=identity,
        workspace_root=workspace,
    )
    if create:
        ensure_state_paths(paths)
    return paths


def ensure_state_paths(paths: StartupPaths | StartupProfile) -> None:
    db_path = getattr(paths, "db_path")
    identity_dir = getattr(paths, "identity_dir")
    workspace_root = getattr(paths, "workspace_root")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    Path(identity_dir).mkdir(parents=True, exist_ok=True)
    Path(workspace_root).mkdir(parents=True, exist_ok=True)
    state_dir = getattr(paths, "state_dir", None)
    if state_dir is not None:
        Path(state_dir).mkdir(parents=True, exist_ok=True)


def profile_from_values(
    repo_root: Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8421,
    node_id: str = "",
    display_name: str = "OCP Node",
    device_class: str = "full",
    form_factor: str = "workstation",
    db_path: str | Path = "",
    identity_dir: str | Path = "",
    workspace_root: str | Path = "",
    state_dir: str | Path | None = None,
    base_url: str = "",
    create_paths: bool = True,
) -> StartupProfile:
    paths = resolve_state_paths(
        repo_root,
        db_path=db_path,
        identity_dir=identity_dir,
        workspace_root=workspace_root,
        state_dir=state_dir,
        create=create_paths,
    )
    return StartupProfile(
        host=str(host or "127.0.0.1"),
        port=int(port),
        node_id=str(node_id or default_node_id()).strip() or default_node_id(),
        display_name=str(display_name or "OCP Node").strip() or "OCP Node",
        device_class=str(device_class or "full").strip() or "full",
        form_factor=str(form_factor or "workstation").strip() or "workstation",
        db_path=paths.db_path,
        identity_dir=paths.identity_dir,
        workspace_root=paths.workspace_root,
        base_url=str(base_url or "").strip(),
    )


def server_command(
    profile: StartupProfile,
    repo_root: Path,
    *,
    python_executable: str | Path | None = None,
) -> list[str]:
    command = [
        str(python_executable or sys.executable),
        str(Path(repo_root) / "server.py"),
        "--host",
        profile.host,
        "--port",
        str(int(profile.port)),
        "--db-path",
        str(profile.db_path),
        "--workspace-root",
        str(profile.workspace_root),
        "--identity-dir",
        str(profile.identity_dir),
        "--node-id",
        profile.node_id,
        "--display-name",
        profile.display_name,
        "--device-class",
        profile.device_class,
        "--form-factor",
        profile.form_factor,
    ]
    if profile.base_url:
        command.extend(["--base-url", profile.base_url])
    return command


def wait_for_manifest(host: str, port: int, timeout_seconds: float) -> bool:
    url = health_url(host, port)
    deadline = time.time() + max(timeout_seconds, 1.0)
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, OSError):
            time.sleep(0.35)
    return False


def read_json_file(path: Path, *, default: dict | None = None) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default or {})


def write_json_file(path: Path, payload: dict) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(dict(payload or {}), indent=2, sort_keys=True) + "\n", encoding="utf-8")


__all__ = [
    "StartupPaths",
    "StartupProfile",
    "build_open_url",
    "default_launcher_config_path",
    "default_launcher_state_dir",
    "default_launcher_support_dir",
    "default_node_id",
    "default_repo_state_dir",
    "discover_local_ipv4_addresses",
    "display_host_for_browser",
    "ensure_state_paths",
    "health_url",
    "is_loopback_host",
    "is_wildcard_host",
    "profile_from_values",
    "read_json_file",
    "resolve_state_paths",
    "server_command",
    "share_urls_for_host",
    "slugify",
    "wait_for_manifest",
    "write_json_file",
]
