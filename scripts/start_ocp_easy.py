#!/usr/bin/env python3
"""Start OCP with local defaults and open the unified app automatically."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import webbrowser
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ocp_startup


def slugify(value: str) -> str:
    return ocp_startup.slugify(value)


def default_node_id() -> str:
    return ocp_startup.default_node_id()


def display_host_for_browser(host: str) -> str:
    return ocp_startup.display_host_for_browser(host)


def is_wildcard_host(host: str) -> bool:
    return ocp_startup.is_wildcard_host(host)


def is_loopback_host(host: str) -> bool:
    return ocp_startup.is_loopback_host(host)


def discover_local_ipv4_addresses(*, bind_host: str = "") -> list[str]:
    return ocp_startup.discover_local_ipv4_addresses(bind_host=bind_host)


def share_urls_for_host(host: str, port: int) -> list[str]:
    return ocp_startup.share_urls_for_host(host, port, discover_ipv4=discover_local_ipv4_addresses)


def build_open_url(host: str, port: int, path: str = "/") -> str:
    return ocp_startup.build_open_url(host, port, path)


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
    return ocp_startup.wait_for_manifest(host, port, timeout_seconds)


def _profile_from_args(args: argparse.Namespace, repo_root: Path) -> ocp_startup.StartupProfile:
    return ocp_startup.profile_from_values(
        repo_root,
        host=args.host,
        port=args.port,
        db_path=args.db_path,
        workspace_root=args.workspace_root,
        identity_dir=args.identity_dir,
        node_id=args.node_id,
        display_name=args.display_name,
        device_class=args.device_class,
        form_factor=args.form_factor,
        base_url=args.base_url,
        create_paths=True,
    )


def server_command(args: argparse.Namespace, repo_root: Path) -> list[str]:
    return ocp_startup.server_command(_profile_from_args(args, repo_root), repo_root)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = REPO_ROOT
    command = server_command(args, repo_root)
    open_url = build_open_url(args.host, args.port, args.open_path)
    profile = _profile_from_args(args, repo_root)

    print("Starting The Open Compute Protocol")
    print()
    print(f"  repo:         {repo_root}")
    print(f"  host:         {args.host}")
    print(f"  port:         {args.port}")
    print(f"  node id:      {args.node_id}")
    print(f"  display name: {args.display_name}")
    print(f"  db:           {profile.db_path}")
    print(f"  identity:     {profile.identity_dir}")
    print(f"  workspace:    {profile.workspace_root}")
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
