#!/usr/bin/env python3
"""Start OCP with local defaults and open the unified app automatically."""

from __future__ import annotations

import argparse
import ipaddress
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path


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


def default_node_id() -> str:
    host_name = os.uname().nodename if hasattr(os, "uname") else os.environ.get("COMPUTERNAME", "ocp")
    token = slugify(host_name) or "ocp"
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
        (
            host
            for host in seen
            if host and ipaddress.ip_address(host).version == 4
        ),
        key=lambda host: (not ipaddress.ip_address(host).is_private, host),
    )


def share_urls_for_host(host: str, port: int) -> list[str]:
    token = str(host or "").strip()
    if token and not is_wildcard_host(token) and not is_loopback_host(token):
        return [f"http://{token}:{int(port)}/"]
    if is_wildcard_host(token):
        return [f"http://{address}:{int(port)}/" for address in discover_local_ipv4_addresses(bind_host=token)]
    return []


def build_open_url(host: str, port: int, path: str = "/") -> str:
    route = path if str(path or "").startswith("/") else f"/{path}"
    return f"http://{display_host_for_browser(host)}:{int(port)}{route}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start OCP and open the unified app automatically.")
    parser.add_argument("--host", default=os.environ.get("OCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("OCP_PORT", "8421")))
    parser.add_argument("--db-path", default="")
    parser.add_argument("--workspace-root", default="")
    parser.add_argument("--identity-dir", default="")
    parser.add_argument("--node-id", default=os.environ.get("OCP_NODE_ID", default_node_id()))
    parser.add_argument("--display-name", default=os.environ.get("OCP_DISPLAY_NAME", "OCP Node"))
    parser.add_argument("--device-class", default=os.environ.get("OCP_DEVICE_CLASS", "full"))
    parser.add_argument("--form-factor", default=os.environ.get("OCP_FORM_FACTOR", "workstation"))
    parser.add_argument("--base-url", default=os.environ.get("OCP_BASE_URL", ""))
    parser.add_argument("--no-open-browser", action="store_true", help="Start OCP without opening the browser.")
    parser.add_argument("--open-path", default="/", help="Path to open after the server comes up.")
    parser.add_argument("--open-timeout", type=float, default=20.0, help="Seconds to wait before giving up on auto-open.")
    return parser


def wait_for_manifest(host: str, port: int, timeout_seconds: float) -> bool:
    url = build_open_url(host, port, "/mesh/manifest")
    deadline = time.time() + max(timeout_seconds, 1.0)
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, OSError):
            time.sleep(0.35)
    return False


def server_command(args: argparse.Namespace, repo_root: Path) -> list[str]:
    state_dir = Path(args.db_path).parent if args.db_path else (repo_root / ".local" / "ocp")
    db_path = Path(args.db_path) if args.db_path else (state_dir / "ocp.db")
    identity_dir = Path(args.identity_dir) if args.identity_dir else (state_dir / "identity")
    workspace_root = Path(args.workspace_root) if args.workspace_root else (state_dir / "workspace")
    identity_dir.mkdir(parents=True, exist_ok=True)
    workspace_root.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(repo_root / "server.py"),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--db-path",
        str(db_path),
        "--workspace-root",
        str(workspace_root),
        "--identity-dir",
        str(identity_dir),
        "--node-id",
        args.node_id,
        "--display-name",
        args.display_name,
        "--device-class",
        args.device_class,
        "--form-factor",
        args.form_factor,
    ]
    if args.base_url:
        command.extend(["--base-url", args.base_url])
    return command


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    command = server_command(args, repo_root)
    open_url = build_open_url(args.host, args.port, args.open_path)

    print("Starting The Open Compute Protocol")
    print()
    print(f"  repo:         {repo_root}")
    print(f"  host:         {args.host}")
    print(f"  port:         {args.port}")
    print(f"  node id:      {args.node_id}")
    print(f"  display name: {args.display_name}")
    print()
    print("OCP app:")
    print(f"  {open_url}")
    print()
    print("Easy setup module:")
    print(f"  {build_open_url(args.host, args.port, '/easy')}")
    print()
    print("Advanced control module:")
    print(f"  {build_open_url(args.host, args.port, '/control')}")
    share_urls = share_urls_for_host(args.host, args.port)
    if share_urls:
        print()
        print("LAN share URLs:")
        for url in share_urls:
            print(f"  {url}")
    elif discover_local_ipv4_addresses(bind_host=args.host) and is_loopback_host(args.host):
        print()
        print("Detected local network IPs, but this node is local-only right now:")
        for address in discover_local_ipv4_addresses(bind_host=args.host):
            print(f"  {address}")
        print("To share OCP with your phone or another laptop:")
        print(f"  OCP_HOST=0.0.0.0 python3 {Path(__file__).name}")

    child = subprocess.Popen(command, cwd=str(repo_root))
    try:
        if not args.no_open_browser and wait_for_manifest(args.host, args.port, args.open_timeout):
            webbrowser.open(open_url)
        return child.wait()
    except KeyboardInterrupt:
        child.terminate()
        try:
            return child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            return child.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
