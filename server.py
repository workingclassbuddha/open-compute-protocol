"""
Standalone HTTP host for the Sovereign Mesh OCP reference implementation.
"""

from __future__ import annotations

import argparse
import errno
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from mesh import SovereignMesh
from mesh.sovereign import _normalize_base_url, _preferred_local_base_url
from runtime import OCPRegistry, OCPStore
from server_app import build_app_manifest as _build_app_manifest, build_app_page as _build_app_page
from server_connect import build_easy_page as _build_easy_page
from server_control import (
    build_control_state as _build_control_state,
    build_control_stream_payload as _build_control_stream_payload,
    control_peer_advisories as _control_peer_advisories_impl,
    latest_event_cursor as _latest_event_cursor_impl,
)
from server_control_page import build_control_page as _build_control_page
from server_http_handlers import OCPRouteHandlerMixin

server_context = {
    "mesh": None,
    "runtime": None,
    "ready": False,
}


def _is_client_disconnect(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
        return True
    if isinstance(exc, OSError):
        return exc.errno in {errno.EPIPE, errno.ECONNRESET, 54, 104}
    return False


def _latest_event_cursor(mesh: SovereignMesh) -> int:
    return _latest_event_cursor_impl(mesh)


def build_control_state(mesh: SovereignMesh) -> dict[str, Any]:
    return _build_control_state(mesh)


def _control_peer_advisories(state: dict[str, Any]) -> dict[str, Any]:
    return _control_peer_advisories_impl(state)


def build_control_stream_payload(
    mesh: SovereignMesh,
    *,
    since_seq: int = 0,
    limit: int = 50,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _build_control_stream_payload(mesh, since_seq=since_seq, limit=limit, snapshot=snapshot)


def build_control_page(mesh: SovereignMesh) -> str:
    return _build_control_page(mesh)


def build_easy_page(mesh: SovereignMesh) -> str:
    return _build_easy_page(mesh)


def build_app_page(mesh: SovereignMesh) -> str:
    return _build_app_page(mesh)


def build_app_manifest(mesh: SovereignMesh) -> dict[str, Any]:
    return _build_app_manifest(mesh)


class OCPHandler(OCPRouteHandlerMixin, BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def handle(self):
        try:
            super().handle()
        except Exception as exc:
            if _is_client_disconnect(exc):
                return
            raise

    def _mesh(self):
        server_obj = getattr(self, "server", None)
        mesh = getattr(server_obj, "mesh", None) or server_context.get("mesh")
        if mesh is None:
            raise RuntimeError("mesh runtime is not configured")
        return mesh

    def _read_json(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length)
        return json.loads(raw.decode("utf-8")) if raw else {}

    def _send_json(self, payload, code: int = 200):
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, markup: str, code: int = 200):
        raw = str(markup or "").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_manifest_json(self, payload: dict[str, Any], code: int = 200):
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/manifest+json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _begin_sse(self, *, close_connection: bool = False):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close" if close_connection else "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

    def _write_sse_event(self, event_name: str, payload: dict[str, Any], *, event_id: str = ""):
        if event_id:
            self.wfile.write(f"id: {event_id}\n".encode("utf-8"))
        self.wfile.write(f"event: {event_name}\n".encode("utf-8"))
        for line in json.dumps(payload).splitlines():
            self.wfile.write(f"data: {line}\n".encode("utf-8"))
        self.wfile.write(b"\n")
        self.wfile.flush()

    def _write_sse_comment(self, text: str = "keepalive"):
        self.wfile.write(f": {text}\n\n".encode("utf-8"))
        self.wfile.flush()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        try:
            if self._dispatch_get_request(path, params):
                return
            self._send_json({"error": "unknown endpoint"}, 404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 400)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            data = self._read_json()
            if self._dispatch_post_request(path, data):
                return
            self._send_json({"error": "unknown endpoint"}, 404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 400)


def build_http_server(mesh: SovereignMesh, *, host: str = "127.0.0.1", port: int = 8421) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), OCPHandler)
    httpd.mesh = mesh
    return httpd


def _bootstrap_mesh(args) -> SovereignMesh:
    lattice = OCPStore(db_path=args.db_path)
    registry = OCPRegistry(lattice)
    if args.agent_id:
        lattice.register_agent(
            agent_id=args.agent_id,
            agent_name=args.agent_name or args.agent_id,
            capabilities=["mesh", "worker-runtime"],
            metadata={
                "runtime": "ocp-standalone",
                "role": "controller",
                "scope": "standalone OCP control plane",
                "interface": "http",
            },
        )
        lattice.heartbeat_agent_session(
            args.session_id,
            agent_id=args.agent_id,
            runtime="ocp-standalone",
            current_task="serving /mesh routes",
            status="active",
        )
    mesh = SovereignMesh(
        lattice,
        registry=registry,
        workspace_root=args.workspace_root,
        identity_dir=args.identity_dir,
        display_name=args.display_name,
        node_id=args.node_id,
        base_url=(
            _normalize_base_url(args.base_url.rstrip("/"))
            if args.base_url
            else _preferred_local_base_url(bind_host=args.host, port=args.port)
        ),
        device_profile={
            key: value
            for key, value in {
                "device_class": args.device_class,
                "execution_tier": args.execution_tier,
                "power_profile": args.power_profile,
                "network_profile": args.network_profile,
                "mobility": args.mobility,
                "form_factor": args.form_factor,
            }.items()
            if value is not None
        }
        or None,
    )
    mesh.network_bind_host = args.host
    server_context["mesh"] = mesh
    server_context["runtime"] = {"lattice": lattice, "registry": registry}
    server_context["ready"] = True
    return mesh


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the standalone Sovereign Mesh OCP server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8421)
    parser.add_argument("--db-path", default="./ocp.db")
    parser.add_argument("--workspace-root", default=".")
    parser.add_argument("--identity-dir", default="./.mesh")
    parser.add_argument("--node-id", default=None)
    parser.add_argument("--display-name", default="Standalone OCP Organism")
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--device-class", choices=["full", "light", "micro", "relay"], default=None)
    parser.add_argument("--execution-tier", choices=["heavy", "standard", "light", "control", "sensor"], default=None)
    parser.add_argument("--power-profile", choices=["line_powered", "battery", "mixed"], default=None)
    parser.add_argument("--network-profile", choices=["wired", "broadband", "wifi", "metered", "intermittent"], default=None)
    parser.add_argument("--mobility", choices=["fixed", "portable", "mobile", "wearable"], default=None)
    parser.add_argument("--form-factor", choices=["server", "workstation", "laptop", "tablet", "phone", "watch", "relay", "edge"], default=None)
    parser.add_argument("--agent-id", default="ocp-control")
    parser.add_argument("--agent-name", default="OCP Control Plane")
    parser.add_argument("--session-id", default="ocp-control-session")
    args = parser.parse_args(argv)

    mesh = _bootstrap_mesh(args)
    httpd = build_http_server(mesh, host=args.host, port=args.port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
