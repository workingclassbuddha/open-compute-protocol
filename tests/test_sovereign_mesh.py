import base64
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

import server
import server_app
import server_artifacts
import server_connect
import server_control
import server_control_page
import server_contract
import server_missions
import server_ops
import server_routes
import server_runtime
import ocp_startup
from ocp_desktop import launcher as ocp_launcher
from ocp_desktop import macos_app as ocp_macos_app
from mesh import (
    MeshArtifactAccessError,
    MeshPeerClient,
    MeshPolicyError,
    MeshReplayError,
    MeshSignatureError,
    SovereignMesh,
)
from mesh_protocol import (
    SCHEMA_VERSION,
    build_protocol_conformance_snapshot,
    get_protocol_schema,
    list_protocol_schemas,
    validate_protocol_object,
)
from runtime import OCPRegistry, OCPStore

START_OCP_EASY = ROOT / "scripts" / "start_ocp_easy.py"
START_OCP_EASY_SPEC = importlib.util.spec_from_file_location("start_ocp_easy", START_OCP_EASY)
start_ocp_easy = importlib.util.module_from_spec(START_OCP_EASY_SPEC)
assert START_OCP_EASY_SPEC and START_OCP_EASY_SPEC.loader
START_OCP_EASY_SPEC.loader.exec_module(start_ocp_easy)


class StubMetabolism:
    def __init__(self):
        self.calls = []

    def trigger(self, kind="wake_maintenance", *, topic=None, payload=None):
        result = {
            "id": f"metabolism-{len(self.calls) + 1}",
            "kind": kind,
            "topic": topic or kind,
            "payload": dict(payload or {}),
        }
        self.calls.append(result)
        return result


class StubSwarm:
    def __init__(self):
        self.calls = []

    def submit(self, payload):
        payload = dict(payload or {})
        self.calls.append(payload)
        return {
            "status": "accepted",
            "node": {"id": f"swarm-{len(self.calls)}", "text": payload.get("finding") or payload.get("text") or ""},
        }


class ProbeHandler:
    def __init__(self):
        self.payload = None
        self.code = None
        self.content_type = None

    def _send_json(self, data, code=200):
        self.payload = data
        self.code = code

    def _send_html(self, data, code=200):
        self.payload = data
        self.code = code
        self.content_type = "text/html; charset=utf-8"

    def _send_manifest_json(self, data, code=200):
        self.payload = data
        self.code = code
        self.content_type = "application/manifest+json"


ProbeHandler._mesh = server.OCPHandler._mesh
ProbeHandler._dispatch_get_request = server.OCPHandler._dispatch_get_request
ProbeHandler._dispatch_post_request = server.OCPHandler._dispatch_post_request
ProbeHandler._handle_app_page = server.OCPHandler._handle_app_page
ProbeHandler._handle_app_manifest = server.OCPHandler._handle_app_manifest
ProbeHandler._handle_mesh_app_status = server.OCPHandler._handle_mesh_app_status
ProbeHandler._handle_control_page = server.OCPHandler._handle_control_page
ProbeHandler._handle_easy_page = server.OCPHandler._handle_easy_page
ProbeHandler._handle_mesh_contract = server.OCPHandler._handle_mesh_contract
ProbeHandler._handle_mesh_manifest = server.OCPHandler._handle_mesh_manifest
ProbeHandler._handle_mesh_device_profile = server.OCPHandler._handle_mesh_device_profile
ProbeHandler._handle_mesh_device_profile_update = server.OCPHandler._handle_mesh_device_profile_update
ProbeHandler._handle_mesh_connectivity_diagnostics = server.OCPHandler._handle_mesh_connectivity_diagnostics
ProbeHandler._handle_mesh_handshake = server.OCPHandler._handle_mesh_handshake
ProbeHandler._handle_mesh_autonomy_status = server.OCPHandler._handle_mesh_autonomy_status
ProbeHandler._handle_mesh_autonomy_activate = server.OCPHandler._handle_mesh_autonomy_activate
ProbeHandler._handle_mesh_routes_health = server.OCPHandler._handle_mesh_routes_health
ProbeHandler._handle_mesh_routes_probe = server.OCPHandler._handle_mesh_routes_probe
ProbeHandler._handle_mesh_discovery_candidates = server.OCPHandler._handle_mesh_discovery_candidates
ProbeHandler._handle_mesh_discovery_seek = server.OCPHandler._handle_mesh_discovery_seek
ProbeHandler._handle_mesh_discovery_scan_local = server.OCPHandler._handle_mesh_discovery_scan_local
ProbeHandler._handle_mesh_peers_connect = server.OCPHandler._handle_mesh_peers_connect
ProbeHandler._handle_mesh_peers_connect_all = server.OCPHandler._handle_mesh_peers_connect_all
ProbeHandler._handle_mesh_peers_sync = server.OCPHandler._handle_mesh_peers_sync
ProbeHandler._handle_mesh_missions = server.OCPHandler._handle_mesh_missions
ProbeHandler._handle_mesh_mission_continuity_get = server.OCPHandler._handle_mesh_mission_continuity_get
ProbeHandler._handle_mesh_mission_continuity_export = server.OCPHandler._handle_mesh_mission_continuity_export
ProbeHandler._handle_mesh_continuity_vessel_verify = server.OCPHandler._handle_mesh_continuity_vessel_verify
ProbeHandler._handle_mesh_continuity_restore_plan = server.OCPHandler._handle_mesh_continuity_restore_plan
ProbeHandler._handle_mesh_mission_get = server.OCPHandler._handle_mesh_mission_get
ProbeHandler._handle_mesh_mission_launch = server.OCPHandler._handle_mesh_mission_launch
ProbeHandler._handle_mesh_mission_test_launch = server.OCPHandler._handle_mesh_mission_test_launch
ProbeHandler._handle_mesh_mission_test_mesh_launch = server.OCPHandler._handle_mesh_mission_test_mesh_launch
ProbeHandler._handle_mesh_mission_cancel = server.OCPHandler._handle_mesh_mission_cancel
ProbeHandler._handle_mesh_mission_resume = server.OCPHandler._handle_mesh_mission_resume
ProbeHandler._handle_mesh_mission_resume_from_checkpoint = server.OCPHandler._handle_mesh_mission_resume_from_checkpoint
ProbeHandler._handle_mesh_mission_restart = server.OCPHandler._handle_mesh_mission_restart
ProbeHandler._handle_mesh_cooperative_tasks = server.OCPHandler._handle_mesh_cooperative_tasks
ProbeHandler._handle_mesh_cooperative_task_get = server.OCPHandler._handle_mesh_cooperative_task_get
ProbeHandler._handle_mesh_cooperative_task_launch = server.OCPHandler._handle_mesh_cooperative_task_launch
ProbeHandler._handle_mesh_pressure = server.OCPHandler._handle_mesh_pressure
ProbeHandler._handle_mesh_helpers = server.OCPHandler._handle_mesh_helpers
ProbeHandler._handle_mesh_helpers_plan = server.OCPHandler._handle_mesh_helpers_plan
ProbeHandler._handle_mesh_helpers_enlist = server.OCPHandler._handle_mesh_helpers_enlist
ProbeHandler._handle_mesh_helpers_drain = server.OCPHandler._handle_mesh_helpers_drain
ProbeHandler._handle_mesh_helpers_retire = server.OCPHandler._handle_mesh_helpers_retire
ProbeHandler._handle_mesh_helpers_auto_seek = server.OCPHandler._handle_mesh_helpers_auto_seek
ProbeHandler._handle_mesh_helpers_preferences = server.OCPHandler._handle_mesh_helpers_preferences
ProbeHandler._handle_mesh_helpers_preferences_set = server.OCPHandler._handle_mesh_helpers_preferences_set
ProbeHandler._handle_mesh_helpers_autonomy = server.OCPHandler._handle_mesh_helpers_autonomy
ProbeHandler._handle_mesh_helpers_autonomy_run = server.OCPHandler._handle_mesh_helpers_autonomy_run
ProbeHandler._handle_mesh_workers = server.OCPHandler._handle_mesh_workers
ProbeHandler._handle_mesh_notifications = server.OCPHandler._handle_mesh_notifications
ProbeHandler._handle_mesh_notification_publish = server.OCPHandler._handle_mesh_notification_publish
ProbeHandler._handle_mesh_notification_ack = server.OCPHandler._handle_mesh_notification_ack
ProbeHandler._handle_mesh_approvals = server.OCPHandler._handle_mesh_approvals
ProbeHandler._handle_mesh_approval_request = server.OCPHandler._handle_mesh_approval_request
ProbeHandler._handle_mesh_approval_resolve = server.OCPHandler._handle_mesh_approval_resolve
ProbeHandler._handle_mesh_treaties = server.OCPHandler._handle_mesh_treaties
ProbeHandler._handle_mesh_treaty_get = server.OCPHandler._handle_mesh_treaty_get
ProbeHandler._handle_mesh_treaty_propose = server.OCPHandler._handle_mesh_treaty_propose
ProbeHandler._handle_mesh_treaty_audit = server.OCPHandler._handle_mesh_treaty_audit
ProbeHandler._handle_mesh_secrets = server.OCPHandler._handle_mesh_secrets
ProbeHandler._handle_mesh_secret_put = server.OCPHandler._handle_mesh_secret_put
ProbeHandler._handle_mesh_queue = server.OCPHandler._handle_mesh_queue
ProbeHandler._handle_mesh_queue_events = server.OCPHandler._handle_mesh_queue_events
ProbeHandler._handle_mesh_queue_metrics = server.OCPHandler._handle_mesh_queue_metrics
ProbeHandler._handle_mesh_queue_replay = server.OCPHandler._handle_mesh_queue_replay
ProbeHandler._handle_mesh_queue_ack_deadline = server.OCPHandler._handle_mesh_queue_ack_deadline
ProbeHandler._handle_mesh_worker_register = server.OCPHandler._handle_mesh_worker_register
ProbeHandler._handle_mesh_scheduler_decisions = server.OCPHandler._handle_mesh_scheduler_decisions
ProbeHandler._handle_mesh_job_resume = server.OCPHandler._handle_mesh_job_resume
ProbeHandler._handle_mesh_job_resume_from_checkpoint = server.OCPHandler._handle_mesh_job_resume_from_checkpoint
ProbeHandler._handle_mesh_job_restart = server.OCPHandler._handle_mesh_job_restart
ProbeHandler._handle_mesh_artifact_get = server.OCPHandler._handle_mesh_artifact_get
ProbeHandler._artifact_content_is_public = server.OCPHandler._artifact_content_is_public


def make_mesh_http_server(mesh):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            return

        def _send_json(self, payload, code=200):
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_html(self, payload, code=200):
            raw = str(payload or "").encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_sse(self, event_name, payload, event_id=""):
            if event_id:
                self.wfile.write(f"id: {event_id}\n".encode("utf-8"))
            self.wfile.write(f"event: {event_name}\n".encode("utf-8"))
            for line in json.dumps(payload).splitlines():
                self.wfile.write(f"data: {line}\n".encode("utf-8"))
            self.wfile.write(b"\n")
            self.wfile.flush()

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            params = parse_qs(parsed.query)
            try:
                if path in {"/", "/app"}:
                    self._send_html(server.build_app_page(mesh))
                    return
                if path == "/app.webmanifest":
                    self._send_json(server.build_app_manifest(mesh))
                    return
                if path == "/easy":
                    self._send_html(server.build_easy_page(mesh))
                    return
                if path in {"/control", "/control/mobile"}:
                    self._send_html(server.build_control_page(mesh))
                    return
                if path == "/mesh/control/stream":
                    since = int(params.get("since", ["0"])[0])
                    limit = int(params.get("limit", ["50"])[0])
                    envelope = server.build_control_stream_payload(mesh, since_seq=since, limit=limit)
                    cursor = int(envelope.get("cursor") or since or 0)
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    self._send_sse("stream-open", {"status": "ok", "cursor": cursor, "route": "/mesh/control/stream"}, event_id=str(cursor))
                    self._send_sse("control-state", envelope, event_id=str(cursor))
                    return
                if path == "/mesh/manifest":
                    self._send_json(mesh.get_manifest())
                    return
                if path == "/mesh/device-profile":
                    self._send_json({"status": "ok", "device_profile": dict(mesh.device_profile)})
                    return
                if path == "/mesh/connectivity/diagnostics":
                    self._send_json(mesh.connectivity_diagnostics(limit=24))
                    return
                if path == "/mesh/app/status":
                    self._send_json(server.build_app_status(mesh))
                    return
                if path == "/mesh/autonomy/status":
                    self._send_json(mesh.autonomy_status())
                    return
                if path == "/mesh/routes/health":
                    self._send_json(mesh.routes_health(limit=50))
                    return
                if path == "/mesh/discovery/candidates":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(mesh.list_discovery_candidates(limit=limit, status=params.get("status", [""])[0]))
                    return
                if path == "/mesh/stream":
                    since = int(params.get("since", ["0"])[0])
                    limit = int(params.get("limit", ["50"])[0])
                    self._send_json(mesh.stream_snapshot(since_seq=since, limit=limit))
                    return
                if path == "/mesh/missions":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(mesh.list_missions(limit=limit, status=params.get("status", [""])[0]))
                    return
                if path.startswith("/mesh/missions/") and path.endswith("/continuity"):
                    mission_id = path[len("/mesh/missions/"):-len("/continuity")].strip("/")
                    self._send_json(mesh.get_mission_continuity(mission_id))
                    return
                if path.startswith("/mesh/missions/"):
                    self._send_json(mesh.get_mission(path.split("/mesh/missions/", 1)[1]))
                    return
                if path == "/mesh/cooperative-tasks":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(mesh.list_cooperative_tasks(limit=limit, state=params.get("state", [""])[0]))
                    return
                if path.startswith("/mesh/cooperative-tasks/"):
                    self._send_json(mesh.get_cooperative_task(path.split("/mesh/cooperative-tasks/", 1)[1]))
                    return
                if path == "/mesh/pressure":
                    self._send_json(mesh.mesh_pressure())
                    return
                if path == "/mesh/helpers":
                    limit = int(params.get("limit", ["100"])[0])
                    self._send_json(mesh.list_helpers(limit=limit))
                    return
                if path == "/mesh/helpers/preferences":
                    self._send_json(
                        mesh.list_offload_preferences(
                            limit=int(params.get("limit", ["100"])[0]),
                            peer_id=params.get("peer_id", [""])[0],
                            workload_class=params.get("workload_class", [""])[0],
                        )
                    )
                    return
                if path == "/mesh/helpers/autonomy":
                    self._send_json(mesh.evaluate_autonomous_offload())
                    return
                if path == "/mesh/workers":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(mesh.list_workers(limit=limit))
                    return
                if path == "/mesh/notifications":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(
                        mesh.list_notifications(
                            limit=limit,
                            status=params.get("status", [""])[0],
                            target_peer_id=params.get("target_peer_id", [""])[0],
                            target_agent_id=params.get("target_agent_id", [""])[0],
                        )
                    )
                    return
                if path == "/mesh/approvals":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(
                        mesh.list_approvals(
                            limit=limit,
                            status=params.get("status", [""])[0],
                            target_peer_id=params.get("target_peer_id", [""])[0],
                            target_agent_id=params.get("target_agent_id", [""])[0],
                        )
                    )
                    return
                if path == "/mesh/treaties":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(
                        mesh.list_treaties(
                            limit=limit,
                            status=params.get("status", [""])[0],
                            treaty_type=params.get("treaty_type", [""])[0],
                        )
                    )
                    return
                if path.startswith("/mesh/treaties/"):
                    self._send_json(mesh.get_treaty(path.split("/mesh/treaties/", 1)[1]))
                    return
                if path == "/mesh/secrets":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(mesh.list_secrets(limit=limit, scope=params.get("scope", [""])[0]))
                    return
                if path == "/mesh/queue":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(mesh.list_queue_messages(limit=limit, status=params.get("status", [""])[0]))
                    return
                if path == "/mesh/queue/events":
                    limit = int(params.get("limit", ["50"])[0])
                    since_seq = int(params.get("since", params.get("since_seq", ["0"]))[0])
                    self._send_json(
                        mesh.list_queue_events(
                            since_seq=since_seq,
                            limit=limit,
                            queue_message_id=params.get("queue_message_id", [""])[0],
                            job_id=params.get("job_id", [""])[0],
                        )
                    )
                    return
                if path == "/mesh/queue/metrics":
                    self._send_json(mesh.queue_metrics())
                    return
                if path == "/mesh/scheduler/decisions":
                    limit = int(params.get("limit", ["25"])[0])
                    self._send_json(
                        mesh.list_scheduler_decisions(
                            limit=limit,
                            status=params.get("status", [""])[0],
                            target_type=params.get("target_type", [""])[0],
                        )
                    )
                    return
                if path.startswith("/mesh/jobs/"):
                    self._send_json(mesh.get_job(path.split("/mesh/jobs/", 1)[1]))
                    return
                if path == "/mesh/artifacts":
                    self._send_json(
                        mesh.list_artifacts(
                            limit=int(params.get("limit", ["25"])[0]),
                            artifact_kind=params.get("artifact_kind", [""])[0],
                            digest=params.get("digest", [""])[0],
                            job_id=params.get("job_id", [""])[0],
                            attempt_id=params.get("attempt_id", [""])[0],
                            parent_artifact_id=params.get("parent_artifact_id", [""])[0],
                            owner_peer_id=params.get("owner_peer_id", [""])[0],
                            media_type=params.get("media_type", [""])[0],
                            retention_class=params.get("retention_class", [""])[0],
                        )
                    )
                    return
                if path.startswith("/mesh/artifacts/"):
                    artifact_id = path.split("/mesh/artifacts/", 1)[1]
                    self._send_json(
                        mesh.get_artifact(
                            artifact_id,
                            requester_peer_id=params.get("peer_id", [""])[0],
                            include_content=params.get("include_content", ["1"])[0] != "0",
                        )
                    )
                    return
                self._send_json({"error": "unknown endpoint"}, 404)
            except Exception as exc:
                self._send_json({"error": str(exc)}, 400)

        def do_POST(self):
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(content_length)
                payload = json.loads(raw.decode("utf-8")) if raw else {}
                if path == "/mesh/handshake":
                    self._send_json(mesh.accept_handshake(payload))
                    return
                if path == "/mesh/device-profile":
                    self._send_json(mesh.update_device_profile(dict(payload.get("device_profile") or {})))
                    return
                if path == "/mesh/autonomy/activate":
                    self._send_json(
                        mesh.activate_autonomic_mesh(
                            mode=(payload.get("mode") or "assisted").strip(),
                            limit=int(payload.get("limit") or 24),
                            scan_timeout=float(payload.get("scan_timeout") or 0.8),
                            timeout=float(payload.get("timeout") or 3.0),
                            run_proof=bool(payload.get("run_proof", True)),
                            repair=bool(payload.get("repair", True)),
                            max_enlist=int(payload.get("max_enlist") or 2),
                            actor_agent_id=(payload.get("actor_agent_id") or "test-http").strip(),
                            request_id=(payload.get("request_id") or "").strip() or None,
                        )
                    )
                    return
                if path == "/mesh/routes/probe":
                    self._send_json(
                        mesh.probe_routes(
                            peer_id=(payload.get("peer_id") or "").strip(),
                            base_url=(payload.get("base_url") or "").strip(),
                            timeout=float(payload.get("timeout") or 2.0),
                            limit=int(payload.get("limit") or 8),
                        )
                    )
                    return
                if path == "/mesh/discovery/seek":
                    self._send_json(
                        mesh.seek_peers(
                            base_urls=list(payload.get("base_urls") or []),
                            hosts=list(payload.get("hosts") or []),
                            cidr=(payload.get("cidr") or "").strip(),
                            port=int(payload.get("port") or 8421),
                            trust_tier=(payload.get("trust_tier") or "trusted").strip(),
                            auto_connect=bool(payload.get("auto_connect", False)),
                            include_self=bool(payload.get("include_self", False)),
                            limit=int(payload.get("limit") or 32),
                            timeout=float(payload.get("timeout") or 2.0),
                            refresh_known=bool(payload.get("refresh_known", True)),
                        )
                    )
                    return
                if path == "/mesh/discovery/scan-local":
                    self._send_json(
                        mesh.scan_local_peers(
                            trust_tier=(payload.get("trust_tier") or "trusted").strip(),
                            timeout=float(payload.get("timeout") or 0.8),
                            limit=int(payload.get("limit") or 24),
                            port=int(payload.get("port") or 0),
                        )
                    )
                    return
                if path == "/mesh/peers/connect":
                    self._send_json(
                        mesh.connect_device(
                            base_url=(payload.get("base_url") or "").strip(),
                            peer_id=(payload.get("peer_id") or "").strip(),
                            trust_tier=(payload.get("trust_tier") or "trusted").strip(),
                            timeout=float(payload.get("timeout") or 3.0),
                            refresh_manifest=bool(payload.get("refresh_manifest", True)),
                        )
                    )
                    return
                if path == "/mesh/peers/connect-all":
                    self._send_json(
                        mesh.connect_all_devices(
                            trust_tier=(payload.get("trust_tier") or "trusted").strip(),
                            timeout=float(payload.get("timeout") or 3.0),
                            scan_timeout=float(payload.get("scan_timeout") or 0.8),
                            limit=int(payload.get("limit") or 24),
                            port=int(payload.get("port") or 0),
                            refresh_manifest=bool(payload.get("refresh_manifest", True)),
                        )
                    )
                    return
                if path == "/mesh/missions/test-mesh-launch":
                    self._send_json(
                        mesh.launch_mesh_test_mission(
                            include_local=bool(payload.get("include_local", True)),
                            limit=int(payload.get("limit") or 24),
                            request_id=(payload.get("request_id") or "").strip() or None,
                        )
                    )
                    return
                if path == "/mesh/notifications/publish":
                    self._send_json(
                        {
                            "status": "ok",
                            "notification": mesh.publish_notification(
                                notification_type=(payload.get("notification_type") or "info").strip(),
                                priority=(payload.get("priority") or "normal").strip(),
                                title=(payload.get("title") or "").strip(),
                                body=(payload.get("body") or "").strip(),
                                compact_title=(payload.get("compact_title") or "").strip(),
                                compact_body=(payload.get("compact_body") or "").strip(),
                                target_peer_id=(payload.get("target_peer_id") or "").strip(),
                                target_agent_id=(payload.get("target_agent_id") or "").strip(),
                                target_device_classes=list(payload.get("target_device_classes") or []),
                                related_job_id=(payload.get("related_job_id") or "").strip(),
                                related_approval_id=(payload.get("related_approval_id") or "").strip(),
                                metadata=dict(payload.get("metadata") or {}),
                            ),
                        }
                    )
                    return
                if path.startswith("/mesh/notifications/") and path.endswith("/ack"):
                    notification_id = path[len("/mesh/notifications/"):-len("/ack")].strip("/")
                    self._send_json(
                        {
                            "status": "ok",
                            "notification": mesh.ack_notification(
                                notification_id,
                                status=(payload.get("status") or "acked").strip(),
                                actor_peer_id=(payload.get("actor_peer_id") or "").strip(),
                                actor_agent_id=(payload.get("actor_agent_id") or "").strip(),
                                reason=(payload.get("reason") or "").strip(),
                            ),
                        }
                    )
                    return
                if path == "/mesh/approvals/request":
                    self._send_json(
                        mesh.create_approval_request(
                            title=(payload.get("title") or "").strip(),
                            summary=(payload.get("summary") or "").strip(),
                            action_type=(payload.get("action_type") or "operator_action").strip(),
                            severity=(payload.get("severity") or "normal").strip(),
                            request_id=(payload.get("request_id") or "").strip(),
                            requested_by_peer_id=(payload.get("requested_by_peer_id") or "").strip(),
                            requested_by_agent_id=(payload.get("requested_by_agent_id") or "").strip(),
                            target_peer_id=(payload.get("target_peer_id") or "").strip(),
                            target_agent_id=(payload.get("target_agent_id") or "").strip(),
                            target_device_classes=list(payload.get("target_device_classes") or []),
                            related_job_id=(payload.get("related_job_id") or "").strip(),
                            expires_at=(payload.get("expires_at") or "").strip(),
                            metadata=dict(payload.get("metadata") or {}),
                        )
                    )
                    return
                if path.startswith("/mesh/approvals/") and path.endswith("/resolve"):
                    approval_id = path[len("/mesh/approvals/"):-len("/resolve")].strip("/")
                    self._send_json(
                        mesh.resolve_approval(
                            approval_id,
                            decision=(payload.get("decision") or "").strip(),
                            operator_peer_id=(payload.get("operator_peer_id") or "").strip(),
                            operator_agent_id=(payload.get("operator_agent_id") or "").strip(),
                            reason=(payload.get("reason") or "").strip(),
                            metadata=dict(payload.get("metadata") or {}),
                        )
                    )
                    return
                if path == "/mesh/treaties/propose":
                    self._send_json(
                        {
                            "status": "ok",
                            "treaty": mesh.propose_treaty(
                                treaty_id=(payload.get("treaty_id") or "").strip(),
                                title=(payload.get("title") or "").strip(),
                                summary=(payload.get("summary") or "").strip(),
                                treaty_type=(payload.get("treaty_type") or "continuity").strip(),
                                status=(payload.get("status") or "active").strip(),
                                parties=list(payload.get("parties") or []),
                                document=dict(payload.get("document") or {}),
                                metadata=dict(payload.get("metadata") or {}),
                                created_by_peer_id=(payload.get("created_by_peer_id") or "").strip(),
                                expires_at=(payload.get("expires_at") or "").strip(),
                            )
                        }
                    )
                    return
                if path == "/mesh/treaties/audit":
                    self._send_json(
                        mesh.audit_treaty_requirements(
                            list(payload.get("treaty_requirements") or []),
                            operation=(payload.get("operation") or "").strip(),
                        )
                    )
                    return
                if path == "/mesh/jobs/submit":
                    self._send_json(mesh.accept_job_submission(payload))
                    return
                if path == "/mesh/jobs/schedule":
                    self._send_json(
                        mesh.schedule_job(
                            dict(payload.get("job") or {}),
                            request_id=(payload.get("request_id") or "").strip() or None,
                            preferred_peer_id=(payload.get("preferred_peer_id") or "").strip(),
                            allow_local=bool(payload.get("allow_local", True)),
                            allow_remote=bool(payload.get("allow_remote", True)),
                        )
                    )
                    return
                if path == "/mesh/missions/launch":
                    self._send_json(
                        mesh.launch_mission(
                            title=(payload.get("title") or "").strip(),
                            intent=(payload.get("intent") or "").strip(),
                            request_id=(payload.get("request_id") or "").strip() or None,
                            priority=(payload.get("priority") or "normal").strip(),
                            workload_class=(payload.get("workload_class") or "").strip(),
                            target_strategy=(payload.get("target_strategy") or "").strip(),
                            policy=dict(payload.get("policy") or {}),
                            continuity=dict(payload.get("continuity") or {}),
                            metadata=dict(payload.get("metadata") or {}),
                            job=dict(payload.get("job") or {}),
                            cooperative_task=dict(payload.get("cooperative_task") or {}),
                        )
                    )
                    return
                if path == "/mesh/missions/test-launch":
                    self._send_json(
                        mesh.launch_test_mission(
                            peer_id=(payload.get("peer_id") or "").strip(),
                            base_url=(payload.get("base_url") or "").strip(),
                            trust_tier=(payload.get("trust_tier") or "trusted").strip(),
                            timeout=float(payload.get("timeout") or 3.0),
                            request_id=(payload.get("request_id") or "").strip() or None,
                        )
                    )
                    return
                if path.startswith("/mesh/missions/") and path.endswith("/continuity/export"):
                    mission_id = path[len("/mesh/missions/"):-len("/continuity/export")].strip("/")
                    self._send_json(
                        mesh.export_mission_continuity_vessel(
                            mission_id,
                            dry_run=bool(payload.get("dry_run", True)),
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "").strip(),
                        )
                    )
                    return
                if path == "/mesh/continuity/vessels/verify":
                    self._send_json(
                        mesh.verify_continuity_vessel(
                            (payload.get("artifact_id") or payload.get("vessel_artifact_id") or "").strip()
                        )
                    )
                    return
                if path == "/mesh/continuity/vessels/restore-plan":
                    self._send_json(
                        mesh.plan_continuity_restore(
                            (payload.get("artifact_id") or payload.get("vessel_artifact_id") or "").strip(),
                            target_peer_id=(payload.get("target_peer_id") or "").strip(),
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "").strip(),
                        )
                    )
                    return
                if path.startswith("/mesh/missions/") and path.endswith("/cancel"):
                    mission_id = path[len("/mesh/missions/"):-len("/cancel")].strip("/")
                    self._send_json(
                        mesh.cancel_mission(
                            mission_id,
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "mission_cancelled").strip(),
                        )
                    )
                    return
                if path.startswith("/mesh/missions/") and path.endswith("/resume-from-checkpoint"):
                    mission_id = path[len("/mesh/missions/"):-len("/resume-from-checkpoint")].strip("/")
                    self._send_json(
                        mesh.resume_mission_from_checkpoint(
                            mission_id,
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "mission_resume_checkpoint").strip(),
                            checkpoint_artifact_id=(payload.get("checkpoint_artifact_id") or "").strip(),
                        )
                    )
                    return
                if path.startswith("/mesh/missions/") and path.endswith("/resume"):
                    mission_id = path[len("/mesh/missions/"):-len("/resume")].strip("/")
                    self._send_json(
                        mesh.resume_mission(
                            mission_id,
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "mission_resume_latest").strip(),
                        )
                    )
                    return
                if path.startswith("/mesh/missions/") and path.endswith("/restart"):
                    mission_id = path[len("/mesh/missions/"):-len("/restart")].strip("/")
                    self._send_json(
                        mesh.restart_mission(
                            mission_id,
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "mission_restart").strip(),
                        )
                    )
                    return
                if path == "/mesh/cooperative-tasks/launch":
                    self._send_json(
                        mesh.launch_cooperative_task(
                            name=(payload.get("name") or "").strip(),
                            request_id=(payload.get("request_id") or "").strip() or None,
                            strategy=(payload.get("strategy") or "spread").strip(),
                            allow_local=bool(payload.get("allow_local", True)),
                            allow_remote=bool(payload.get("allow_remote", True)),
                            target_peer_ids=list(payload.get("target_peer_ids") or []),
                            base_job=dict(payload.get("base_job") or {}),
                            shards=list(payload.get("shards") or []),
                            auto_enlist=bool(payload.get("auto_enlist", False)),
                        )
                    )
                    return
                if path == "/mesh/helpers/plan":
                    self._send_json(
                        mesh.plan_helper_enlistment(
                            job=dict(payload.get("job") or {}),
                            limit=int(payload.get("limit") or 6),
                        )
                    )
                    return
                if path == "/mesh/helpers/enlist":
                    self._send_json(
                        mesh.enlist_helper(
                            (payload.get("peer_id") or "").strip(),
                            mode=(payload.get("mode") or "on_demand").strip(),
                            role=(payload.get("role") or "helper").strip(),
                            reason=(payload.get("reason") or "operator_enlist").strip(),
                            source=(payload.get("source") or "operator").strip(),
                        )
                    )
                    return
                if path == "/mesh/helpers/drain":
                    self._send_json(
                        mesh.drain_helper(
                            (payload.get("peer_id") or "").strip(),
                            drain_reason=(payload.get("drain_reason") or payload.get("reason") or "operator_drain").strip(),
                            source=(payload.get("source") or "operator").strip(),
                        )
                    )
                    return
                if path == "/mesh/helpers/retire":
                    self._send_json(
                        mesh.retire_helper(
                            (payload.get("peer_id") or "").strip(),
                            reason=(payload.get("reason") or "operator_retire").strip(),
                            source=(payload.get("source") or "operator").strip(),
                        )
                    )
                    return
                if path == "/mesh/helpers/auto-seek":
                    self._send_json(
                        mesh.auto_seek_help(
                            job=dict(payload.get("job") or {}),
                            max_enlist=int(payload.get("max_enlist") or 2),
                            mode=(payload.get("mode") or "on_demand").strip(),
                            reason=(payload.get("reason") or "auto_pressure").strip(),
                            allow_remote_seek=bool(payload.get("allow_remote_seek") or False),
                            seek_hosts=list(payload.get("seek_hosts") or []) or None,
                        )
                    )
                    return
                if path == "/mesh/helpers/preferences/set":
                    self._send_json(
                        mesh.set_offload_preference(
                            (payload.get("peer_id") or "").strip(),
                            workload_class=(payload.get("workload_class") or "default").strip(),
                            preference=(payload.get("preference") or "allow").strip(),
                            source=(payload.get("source") or "operator").strip(),
                            metadata=dict(payload.get("metadata") or {}),
                        )
                    )
                    return
                if path == "/mesh/helpers/autonomy/run":
                    self._send_json(
                        mesh.run_autonomous_offload(
                            job=dict(payload.get("job") or {}),
                            actor_agent_id=(payload.get("actor_agent_id") or "test-client").strip(),
                        )
                    )
                    return
                if path == "/mesh/secrets/put":
                    self._send_json(
                        {
                            "status": "ok",
                            "secret": mesh.put_secret(
                                (payload.get("name") or "").strip(),
                                payload.get("value"),
                                scope=(payload.get("scope") or "").strip(),
                                metadata=dict(payload.get("metadata") or {}),
                            ),
                        }
                    )
                    return
                if path.startswith("/mesh/jobs/") and path.endswith("/resume-from-checkpoint"):
                    job_id = path[len("/mesh/jobs/"):-len("/resume-from-checkpoint")].strip("/")
                    self._send_json(
                        mesh.resume_job_from_checkpoint(
                            job_id,
                            checkpoint_artifact_id=(payload.get("checkpoint_artifact_id") or "").strip(),
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "operator_resume_checkpoint").strip(),
                        )
                    )
                    return
                if path.startswith("/mesh/jobs/") and path.endswith("/resume"):
                    job_id = path[len("/mesh/jobs/"):-len("/resume")].strip("/")
                    self._send_json(
                        mesh.resume_job(
                            job_id,
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "operator_resume_latest").strip(),
                        )
                    )
                    return
                if path.startswith("/mesh/jobs/") and path.endswith("/restart"):
                    job_id = path[len("/mesh/jobs/"):-len("/restart")].strip("/")
                    self._send_json(
                        mesh.restart_job(
                            job_id,
                            operator_id=(payload.get("operator_id") or "").strip(),
                            reason=(payload.get("reason") or "operator_restart").strip(),
                        )
                    )
                    return
                if path.startswith("/mesh/jobs/") and path.endswith("/cancel"):
                    job_id = path[len("/mesh/jobs/"):-len("/cancel")].strip("/")
                    self._send_json({"status": "cancelled", "job": mesh.cancel_job(job_id)})
                    return
                if path == "/mesh/workers/register":
                    self._send_json(
                        {
                            "status": "ok",
                            "worker": mesh.register_worker(
                                worker_id=(payload.get("worker_id") or "").strip(),
                                agent_id=(payload.get("agent_id") or "").strip(),
                                capabilities=list(payload.get("capabilities") or []),
                                resources=dict(payload.get("resources") or {}),
                                labels=list(payload.get("labels") or []),
                                max_concurrent_jobs=int(payload.get("max_concurrent_jobs") or 1),
                                metadata=dict(payload.get("metadata") or {}),
                                status=(payload.get("status") or "active").strip().lower(),
                            ),
                        }
                    )
                    return
                if path.startswith("/mesh/workers/") and path.endswith("/heartbeat"):
                    worker_id = path[len("/mesh/workers/"):-len("/heartbeat")].strip("/")
                    self._send_json(
                        {
                            "status": "ok",
                            "worker": mesh.heartbeat_worker(
                                worker_id,
                                status=(payload.get("status") or "").strip(),
                                metadata=dict(payload.get("metadata") or {}),
                            ),
                        }
                    )
                    return
                if path.startswith("/mesh/workers/") and path.endswith("/poll"):
                    worker_id = path[len("/mesh/workers/"):-len("/poll")].strip("/")
                    self._send_json(mesh.poll_jobs(worker_id, limit=int(payload.get("limit") or 10)))
                    return
                if path.startswith("/mesh/workers/") and path.endswith("/claim"):
                    worker_id = path[len("/mesh/workers/"):-len("/claim")].strip("/")
                    self._send_json(
                        mesh.claim_next_job(
                            worker_id,
                            job_id=(payload.get("job_id") or "").strip(),
                            ttl_seconds=int(payload.get("ttl_seconds") or 0),
                        )
                    )
                    return
                if path == "/mesh/queue/replay":
                    self._send_json(
                        mesh.replay_queue_message(
                            queue_message_id=(payload.get("queue_message_id") or "").strip(),
                            job_id=(payload.get("job_id") or "").strip(),
                            reason=(payload.get("reason") or "operator_replay").strip(),
                        )
                    )
                    return
                if path == "/mesh/queue/ack-deadline":
                    self._send_json(
                        mesh.set_queue_ack_deadline(
                            queue_message_id=(payload.get("queue_message_id") or "").strip(),
                            attempt_id=(payload.get("attempt_id") or "").strip(),
                            ttl_seconds=int(payload.get("ttl_seconds", payload.get("ack_deadline_seconds", 0)) or 0),
                            reason=(payload.get("reason") or "operator_ack_deadline_update").strip(),
                        )
                    )
                    return
                if path.startswith("/mesh/jobs/attempts/") and path.endswith("/heartbeat"):
                    attempt_id = path[len("/mesh/jobs/attempts/"):-len("/heartbeat")].strip("/")
                    self._send_json(
                        {
                            "status": "ok",
                            "attempt": mesh.heartbeat_job_attempt(
                                attempt_id,
                                ttl_seconds=int(payload.get("ttl_seconds") or 300),
                                metadata=dict(payload.get("metadata") or {}),
                            ),
                        }
                    )
                    return
                if path.startswith("/mesh/jobs/attempts/") and path.endswith("/complete"):
                    attempt_id = path[len("/mesh/jobs/attempts/"):-len("/complete")].strip("/")
                    self._send_json(
                        mesh.complete_job_attempt(
                            attempt_id,
                            payload.get("result"),
                            media_type=(payload.get("media_type") or "application/json").strip(),
                            executor=(payload.get("executor") or "").strip(),
                            metadata=dict(payload.get("metadata") or {}),
                        )
                    )
                    return
                if path.startswith("/mesh/jobs/attempts/") and path.endswith("/fail"):
                    attempt_id = path[len("/mesh/jobs/attempts/"):-len("/fail")].strip("/")
                    self._send_json(
                        mesh.fail_job_attempt(
                            attempt_id,
                            error=(payload.get("error") or "job attempt failed").strip(),
                            retryable=bool(payload.get("retryable", True)),
                            metadata=dict(payload.get("metadata") or {}),
                        )
                    )
                    return
                if path == "/mesh/artifacts/publish":
                    self._send_json(mesh.accept_artifact_publish(payload))
                    return
                if path == "/mesh/artifacts/replicate":
                    self._send_json(
                        mesh.replicate_artifact_from_peer(
                            (payload.get("peer_id") or "").strip(),
                            artifact_id=(payload.get("artifact_id") or "").strip(),
                            digest=(payload.get("digest") or "").strip(),
                            pin=bool(payload.get("pin", False)),
                        )
                    )
                    return
                if path == "/mesh/artifacts/replicate-graph":
                    self._send_json(
                        mesh.replicate_artifact_graph_from_peer(
                            (payload.get("peer_id") or "").strip(),
                            artifact_id=(payload.get("artifact_id") or "").strip(),
                            digest=(payload.get("digest") or "").strip(),
                            pin=bool(payload.get("pin", False)),
                        )
                    )
                    return
                if path == "/mesh/artifacts/pin":
                    self._send_json(
                        {
                            "status": "ok",
                            "artifact": mesh.set_artifact_pin(
                                (payload.get("artifact_id") or "").strip(),
                                pinned=bool(payload.get("pinned", True)),
                                reason=(payload.get("reason") or "operator_pin").strip(),
                            ),
                        }
                    )
                    return
                if path == "/mesh/artifacts/verify-mirror":
                    self._send_json(
                        mesh.verify_artifact_mirror(
                            (payload.get("artifact_id") or "").strip(),
                            peer_id=(payload.get("peer_id") or "").strip(),
                            source_artifact_id=(payload.get("source_artifact_id") or "").strip(),
                            digest=(payload.get("digest") or "").strip(),
                        )
                    )
                    return
                if path == "/mesh/artifacts/purge":
                    self._send_json(mesh.purge_expired_artifacts(limit=int(payload.get("limit") or 100)))
                    return
                if path == "/mesh/agents/handoff":
                    self._send_json(mesh.accept_handoff(payload))
                    return
                self._send_json({"error": "unknown endpoint"}, 404)
            except Exception as exc:
                self._send_json({"error": str(exc)}, 400)

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


class SovereignMeshTests(unittest.TestCase):
    def setUp(self):
        self._tmpdirs = []
        self._servers = []
        self._old_server_context = dict(server.server_context)

    def tearDown(self):
        server.server_context.clear()
        server.server_context.update(self._old_server_context)
        for httpd, thread in self._servers:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)
        for tmpdir in self._tmpdirs:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def make_stack(
        self,
        name: str,
        *,
        golem_enabled: bool = False,
        docker_enabled=None,
        wasm_enabled=None,
        device_profile=None,
    ):
        tmpdir = tempfile.mkdtemp(prefix=f"{name}-mesh-")
        self._tmpdirs.append(tmpdir)
        db_path = os.path.join(tmpdir, "mesh.db")
        lattice = OCPStore(db_path=db_path)
        registry = OCPRegistry(lattice)
        metabolism = StubMetabolism()
        swarm = StubSwarm()
        agent_id = f"{name}-agent"
        lattice.register_agent(
            agent_id=agent_id,
            agent_name=f"{name.title()} Agent",
            capabilities=["chat", "handoff", "registry_locking"],
            metadata={
                "runtime": "codex-cli",
                "role": "executor",
                "scope": f"{name} test agent",
                "interface": "terminal",
            },
        )
        lattice.heartbeat_agent_session(
            f"{name}-session",
            agent_id=agent_id,
            runtime="codex-cli",
            current_task="mesh federation",
            status="active",
        )
        mesh = SovereignMesh(
            lattice,
            registry=registry,
            metabolism=metabolism,
            swarm=swarm,
            workspace_root=tmpdir,
            identity_dir=os.path.join(tmpdir, ".mesh"),
            display_name=f"{name.title()} Organism",
            node_id=f"{name}-node",
            base_url=f"http://{name}.local:8421",
            golem_enabled=golem_enabled,
            docker_enabled=docker_enabled,
            wasm_enabled=wasm_enabled,
            device_profile=device_profile,
        )
        return SimpleNamespace(
            name=name,
            tmpdir=tmpdir,
            lattice=lattice,
            registry=registry,
            metabolism=metabolism,
            swarm=swarm,
            mesh=mesh,
            agent_id=agent_id,
        )

    def serve_mesh(self, stack):
        import threading

        httpd, base_url = make_mesh_http_server(stack.mesh)
        stack.mesh.base_url = base_url
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        self._servers.append((httpd, thread))
        return MeshPeerClient(base_url), base_url

    def handshake(self, source, target, *, trust_tier="trusted"):
        manifest = source.mesh.get_manifest()
        peer_card = dict(manifest["organism_card"])
        peer_card["trust_tier"] = trust_tier
        envelope = source.mesh.build_signed_envelope(
            "/mesh/handshake",
            {
                "peer_card": peer_card,
                "agent_presence": source.mesh.export_agent_presence(limit=20),
                "beacons": source.mesh.export_beacons(limit=10),
            },
        )
        return target.mesh.accept_handshake(envelope), envelope

    def _register_default_worker(self, stack, worker_id="beta-worker"):
        stack.mesh.register_worker(
            worker_id=worker_id,
            agent_id=stack.agent_id,
            capabilities=["python", "worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        return worker_id

    def _checkpointed_job(
        self,
        stack,
        *,
        worker_id="beta-worker",
        request_id="checkpointed-job",
        code="import os; print(os.environ.get('OCP_RESUME_ARTIFACT_ID', 'fresh'))",
        checkpoint_payload=None,
        retryable=False,
        resumable=True,
    ):
        self._register_default_worker(stack, worker_id=worker_id)
        submitted = stack.mesh.submit_local_job(
            {
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": code},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "resumability": {"enabled": resumable},
                    "checkpoint_policy": {"enabled": resumable, "mode": "manual", "on_retry": False},
                },
            },
            request_id=request_id,
        )
        claimed = stack.mesh.claim_next_job(worker_id, job_id=submitted["job"]["id"], ttl_seconds=120)
        failed = stack.mesh.fail_job_attempt(
            claimed["attempt"]["id"],
            error="checkpointed failure",
            retryable=retryable,
            metadata={"checkpoint": checkpoint_payload or {"cursor": 11, "phase": "saved"}},
        )
        return {"submitted": submitted, "claimed": claimed, "failed": failed}

    def _checkpointed_mission(
        self,
        stack,
        *,
        worker_id="beta-worker",
        request_id="checkpointed-mission",
        code="import os; print(os.environ.get('OCP_RESUME_ARTIFACT_ID', 'fresh'))",
    ):
        self._register_default_worker(stack, worker_id=worker_id)
        mission = stack.mesh.launch_mission(
            title=f"Mission {request_id}",
            intent="Mission recovery test",
            request_id=request_id,
            continuity={"resumable": True, "checkpoint_strategy": "manual"},
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": code},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual", "on_retry": False},
                },
            },
        )
        claimed = stack.mesh.claim_next_job(worker_id, job_id=mission["child_job_ids"][0], ttl_seconds=120)
        failed = stack.mesh.fail_job_attempt(
            claimed["attempt"]["id"],
            error="mission checkpoint failure",
            retryable=False,
            metadata={"checkpoint": {"cursor": 11, "phase": "saved"}},
        )
        return {"mission": stack.mesh.get_mission(mission["id"]), "claimed": claimed, "failed": failed}

    def test_signed_handshake_rejects_bad_signature_stale_timestamp_and_replay(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")

        response, envelope = self.handshake(alpha, beta)
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["peer"]["peer_id"], "alpha-node")

        with self.assertRaises(MeshReplayError):
            beta.mesh.accept_handshake(envelope)

        stale = alpha.mesh.build_signed_envelope(
            "/mesh/handshake",
            {
                "peer_card": {**alpha.mesh.get_manifest()["organism_card"], "trust_tier": "trusted"},
                "agent_presence": alpha.mesh.export_agent_presence(),
                "beacons": [],
            },
            timestamp="2000-01-01T00:00:00Z",
        )
        with self.assertRaises(MeshSignatureError):
            beta.mesh.accept_handshake(stale)

        tampered = alpha.mesh.build_signed_envelope(
            "/mesh/handshake",
            {
                "peer_card": {**alpha.mesh.get_manifest()["organism_card"], "trust_tier": "trusted"},
                "agent_presence": alpha.mesh.export_agent_presence(),
                "beacons": [],
            },
        )
        tampered["body"]["peer_card"]["display_name"] = "forged display"
        with self.assertRaises(MeshSignatureError):
            beta.mesh.accept_handshake(tampered)

    def test_mesh_exposes_protocol_scheduler_and_artifact_services(self):
        beta = self.make_stack("beta")

        envelope = beta.mesh.protocol.build_signed_envelope(
            "/mesh/jobs/submit",
            {"job": {"kind": "custom.inline"}},
        )
        self.assertEqual(envelope["request"]["node_id"], "beta-node")

        artifact = beta.mesh.artifacts.publish_local_artifact(
            {"hello": "world"},
            metadata={"artifact_kind": "bundle"},
        )
        fetched = beta.mesh.get_artifact(artifact["id"], include_content=False)
        self.assertEqual(fetched["id"], artifact["id"])
        with beta.mesh._conn() as conn:
            artifact_row = conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", (artifact["id"],)).fetchone()
        shaped_artifact = beta.mesh.artifacts.row_to_artifact(artifact_row)
        self.assertEqual(shaped_artifact["id"], artifact["id"])
        self.assertEqual(shaped_artifact["artifact_kind"], "bundle")

        decision = beta.mesh.scheduler.select_execution_target(
            {"kind": "custom.inline", "policy": {"classification": "trusted", "mode": "batch"}},
            allow_remote=False,
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["target_type"], "local")

        mission = beta.mesh.launch_mission(
            title="Service Mission",
            intent="Verify mission row shaping service seam",
            request_id="service-mission-seam",
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('service mission seam')"},
            },
        )
        with beta.mesh._conn() as conn:
            mission_row = conn.execute("SELECT * FROM mesh_missions WHERE id=?", (mission["id"],)).fetchone()
        shaped_mission = beta.mesh.missions.row_to_mission(mission_row)
        self.assertEqual(shaped_mission["id"], mission["id"])
        self.assertEqual(shaped_mission["title"], "Service Mission")
        self.assertEqual(len(shaped_mission["child_job_ids"]), 1)

    def test_mesh_exposes_execution_service_for_agent_echo(self):
        beta = self.make_stack("beta-exec")

        executor, result, metadata = beta.mesh.execution.execute_job(
            {"kind": "agent.echo", "policy": {"classification": "trusted", "mode": "batch"}},
            payload={"message": "hello"},
        )

        self.assertEqual(executor, "agent-runtime")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["echo"]["message"], "hello")
        self.assertEqual(metadata, {})

    def test_mesh_execution_service_publishes_result_package_bundle(self):
        beta = self.make_stack("beta-exec-package")

        package = beta.mesh.execution.publish_job_result_package(
            {
                "id": "job-exec-package",
                "request_id": "job-exec-package-req",
                "kind": "agent.echo",
                "policy": {"classification": "trusted", "mode": "batch"},
                "artifact_inputs": [],
                "spec": {"dispatch_mode": "queued"},
            },
            result={"status": "ok", "stdout": "hello package\n"},
            media_type="application/json",
            executor="agent-runtime",
            attempt_id="attempt-exec-package",
            metadata={"secret_delivery": []},
        )

        self.assertTrue(package["result_ref"]["id"])
        self.assertTrue(package["bundle_ref"]["id"])
        self.assertTrue(package["config_ref"]["id"])
        self.assertTrue(package["attestation_ref"]["id"])
        bundle = beta.mesh.get_artifact(package["bundle_ref"]["id"], include_content=False)
        self.assertEqual(bundle["artifact_kind"], "bundle")

    def test_mesh_execution_service_submits_local_job_through_execution_boundary(self):
        beta = self.make_stack("beta-exec-submit")

        submitted = beta.mesh.execution.submit_local_job(
            {
                "kind": "agent.echo",
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"message": "hello execution submit"},
                "artifact_inputs": [],
            },
            request_id="execution-service-submit",
        )

        self.assertEqual(submitted["status"], "completed")
        self.assertEqual(submitted["job"]["status"], "completed")
        artifact = beta.mesh.get_artifact(submitted["job"]["result_ref"]["id"])
        payload = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(payload["echo"]["message"], "hello execution submit")
        self.assertTrue(submitted["job"]["result_bundle_ref"]["id"])

    def test_stream_snapshot_converges_on_peer_presence_after_mutual_handshake(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")

        self.handshake(alpha, beta)
        self.handshake(beta, alpha)

        alpha_peers = alpha.mesh.list_peers(limit=10)["peers"]
        beta_peers = beta.mesh.list_peers(limit=10)["peers"]
        self.assertEqual(alpha_peers[0]["peer_id"], "beta-node")
        self.assertEqual(beta_peers[0]["peer_id"], "alpha-node")
        self.assertTrue(alpha_peers[0]["metadata"]["remote_agent_presence"])
        self.assertTrue(beta_peers[0]["metadata"]["remote_agent_presence"])

        alpha_stream = alpha.mesh.stream_snapshot(limit=20)
        beta_stream = beta.mesh.stream_snapshot(limit=20)
        self.assertTrue(alpha_stream["agent_presence"])
        self.assertTrue(beta_stream["agent_presence"])
        self.assertIn("beta-node", {peer["peer_id"] for peer in alpha_stream["peers"]})
        self.assertIn("alpha-node", {peer["peer_id"] for peer in beta_stream["peers"]})

    def test_mesh_exposes_state_service_for_events_and_secrets(self):
        alpha = self.make_stack("alpha-state")
        beta = self.make_stack("beta-state")
        _, beta_base_url = self.serve_mesh(beta)

        alpha.mesh.seek_peers(base_urls=[beta_base_url], auto_connect=True, trust_tier="trusted")

        event = alpha.mesh.state.record_event(
            "mesh.state.test",
            peer_id=alpha.mesh.node_id,
            payload={"hello": "world"},
        )
        self.assertEqual(event["event_type"], "mesh.state.test")

        stream = alpha.mesh.state.stream_snapshot(limit=10)
        self.assertTrue(any(item["id"] == event["id"] for item in stream["events"]))

        secret = alpha.mesh.state.put_secret(
            "api-token",
            "top-secret",
            scope="mesh.ops",
            metadata={"origin": "state-test"},
        )
        self.assertTrue(secret["value_present"])
        self.assertNotIn("value", secret)

        fetched = alpha.mesh.get_secret("api-token", scope="mesh.ops", include_value=True)
        self.assertEqual(fetched["value"], "top-secret")
        self.assertEqual(fetched["metadata"]["origin"], "state-test")

        peers = alpha.mesh.state.list_peers(limit=10)
        self.assertEqual(peers["peers"][0]["peer_id"], "beta-state-node")
        projected_peers = alpha.mesh.state.projections.list_peers(limit=10)
        self.assertEqual(projected_peers["peers"][0]["peer_id"], "beta-state-node")

        candidates = alpha.mesh.state.list_discovery_candidates(limit=10)
        self.assertEqual(candidates["count"], 1)
        self.assertEqual(candidates["candidates"][0]["peer_id"], "beta-state-node")
        projected_candidates = alpha.mesh.state.projections.list_discovery_candidates(limit=10)
        self.assertEqual(projected_candidates["candidates"][0]["peer_id"], "beta-state-node")

        alpha.mesh.register_worker(
            worker_id="alpha-state-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        workers = alpha.mesh.state.list_workers(limit=10)
        self.assertEqual(workers["count"], 1)
        self.assertEqual(workers["workers"][0]["id"], "alpha-state-worker")
        projected_workers = alpha.mesh.state.projections.list_workers(limit=10)
        self.assertEqual(projected_workers["workers"][0]["id"], "alpha-state-worker")

    def test_seek_peers_discovers_and_auto_connects_reachable_peer(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")

        _, beta_base_url = self.serve_mesh(beta)

        sought = alpha.mesh.seek_peers(base_urls=[beta_base_url], auto_connect=True, trust_tier="trusted")

        self.assertEqual(sought["discovered"], 1)
        self.assertEqual(sought["connected"], 1)
        self.assertEqual(sought["results"][0]["peer_id"], "beta-node")
        peers = alpha.mesh.list_peers(limit=10)["peers"]
        self.assertEqual(peers[0]["peer_id"], "beta-node")
        candidates = alpha.mesh.list_discovery_candidates(limit=10)
        self.assertEqual(candidates["count"], 1)
        self.assertEqual(candidates["candidates"][0]["status"], "connected")

    def test_handoff_is_deduplicated_by_request_id(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self.handshake(alpha, beta)

        body = {
            "handoff": {
                "to_peer_id": "beta-node",
                "from_agent": alpha.agent_id,
                "to_agent": beta.agent_id,
                "summary": "Investigate remote job routing",
                "intent": "Continue the federation setup",
                "constraints": {"project_id": "mesh-rollout"},
                "artifact_refs": [],
            }
        }
        first = alpha.mesh.build_signed_envelope("/mesh/agents/handoff", body)
        result = beta.mesh.accept_handoff(first)
        self.assertEqual(result["status"], "accepted")

        second = alpha.mesh.build_signed_envelope(
            "/mesh/agents/handoff",
            body,
            request_id=first["request"]["request_id"],
        )
        deduped = beta.mesh.accept_handoff(second)
        self.assertTrue(deduped["handoff"]["deduped"])
        self.assertEqual(deduped["handoff"]["id"], result["handoff"]["id"])

    def test_remote_job_acquires_lease_executes_and_returns_result_artifact(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self.handshake(alpha, beta)

        envelope = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "agent.echo",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"message": "hello mesh"},
                    "artifact_inputs": [],
                }
            },
        )
        response = beta.mesh.accept_job_submission(envelope)
        self.assertEqual(response["status"], "completed")
        job = response["job"]
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["lease"]["status"], "completed")
        artifact = beta.mesh.get_artifact(job["result_ref"]["id"])
        payload = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(payload["echo"]["message"], "hello mesh")

    def test_private_job_is_refused_for_market_peer(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self.handshake(alpha, beta, trust_tier="market")

        envelope = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "agent.echo",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "private", "mode": "batch"},
                    "payload": {"message": "secret"},
                    "artifact_inputs": [],
                }
            },
        )
        response = beta.mesh.accept_job_submission(envelope)
        self.assertEqual(response["status"], "rejected")
        self.assertEqual(response["job"]["status"], "rejected")

    def test_artifact_publication_verifies_digest_and_respects_policy(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        gamma = self.make_stack("gamma")
        self.handshake(alpha, beta)
        self.handshake(gamma, beta, trust_tier="public")

        trusted_artifact = alpha.mesh.build_signed_envelope(
            "/mesh/artifacts/publish",
            {
                "artifact": {
                    "json": {"plan": "bounded"},
                    "media_type": "application/json",
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "digest": hashlib.sha256(json.dumps({"plan": "bounded"}, sort_keys=True).encode("utf-8")).hexdigest(),
                }
            },
        )
        published = beta.mesh.accept_artifact_publish(trusted_artifact)
        with self.assertRaises(MeshArtifactAccessError):
            beta.mesh.get_artifact(published["artifact"]["id"], requester_peer_id="gamma-node")
        self.assertEqual(
            published["artifact"]["oci_descriptor"]["digest"],
            f"sha256:{published['artifact']['digest']}",
        )

        bad_digest = alpha.mesh.build_signed_envelope(
            "/mesh/artifacts/publish",
            {
                "artifact": {
                    "content": "hello",
                    "media_type": "text/plain",
                    "policy": {"classification": "public", "mode": "batch"},
                    "digest": "deadbeef",
                }
            },
        )
        with self.assertRaises(MeshPolicyError):
            beta.mesh.accept_artifact_publish(bad_digest)

        bad_descriptor = alpha.mesh.build_signed_envelope(
            "/mesh/artifacts/publish",
            {
                "artifact": {
                    "content": "descriptor-check",
                    "media_type": "text/plain",
                    "policy": {"classification": "public", "mode": "batch"},
                    "descriptor": {
                        "mediaType": "text/plain",
                        "size": 999,
                        "digest": f"sha256:{hashlib.sha256(b'descriptor-check').hexdigest()}",
                    },
                }
            },
        )
        with self.assertRaises(MeshPolicyError):
            beta.mesh.accept_artifact_publish(bad_descriptor)

    def test_inline_job_publishes_result_bundle_and_attestation(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self.handshake(alpha, beta)

        envelope = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "agent.echo",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"message": "bundle me"},
                    "artifact_inputs": [],
                }
            },
        )
        response = beta.mesh.accept_job_submission(envelope)
        self.assertEqual(response["status"], "completed")
        job = response["job"]
        self.assertTrue(job["result_bundle_ref"]["id"])
        self.assertTrue(job["result_config_ref"]["id"])
        self.assertTrue(job["result_attestation_ref"]["id"])

        bundle = beta.mesh.get_artifact(job["result_bundle_ref"]["id"])
        bundle_payload = json.loads(base64.b64decode(bundle["content_base64"]).decode("utf-8"))
        self.assertEqual(bundle["media_type"], "application/vnd.oci.image.manifest.v1+json")
        self.assertEqual(bundle_payload["schemaVersion"], 2)
        self.assertEqual(bundle_payload["artifactType"], "application/vnd.ocp.job-result.v1")
        self.assertEqual(bundle_payload["bundle_type"], "job-result")
        self.assertEqual(bundle_payload["primary"]["id"], job["result_ref"]["id"])
        self.assertEqual(bundle_payload["config"]["mediaType"], "application/vnd.ocp.job-result.config.v1+json")
        self.assertEqual(bundle_payload["subject"]["digest"], f"sha256:{job['result_ref']['digest']}")
        roles = {descriptor["role"] for descriptor in bundle_payload["descriptors"]}
        self.assertIn("result", roles)
        self.assertIn("attestation", roles)

        config = beta.mesh.get_artifact(job["result_config_ref"]["id"])
        config_payload = json.loads(base64.b64decode(config["content_base64"]).decode("utf-8"))
        self.assertEqual(config_payload["artifact_type"], "application/vnd.ocp.job-result.v1")
        self.assertEqual(config_payload["result"]["artifact_id"], job["result_ref"]["id"])

        attestation = beta.mesh.get_artifact(job["result_attestation_ref"]["id"])
        attestation_payload = json.loads(base64.b64decode(attestation["content_base64"]).decode("utf-8"))
        self.assertEqual(attestation_payload["subject"]["artifact_id"], job["result_ref"]["id"])
        self.assertEqual(
            attestation_payload["subject_descriptor"]["digest"],
            f"sha256:{job['result_ref']['digest']}",
        )
        self.assertEqual(attestation_payload["predicate"]["job_id"], job["id"])
        self.assertEqual(
            attestation_payload["predicate"]["result_descriptor"]["digest"],
            f"sha256:{job['result_ref']['digest']}",
        )
        self.assertIn("signed_payload_digest", attestation_payload["verification"])
        self.assertTrue(attestation_payload["signature"])

    def test_personal_mirror_adapter_exports_presence_and_submits_remote_background_job(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self.handshake(alpha, beta)

        presence = alpha.mesh.mirror_adapter.export_agent_presence(limit=10)
        self.assertEqual(presence[0]["agent_id"], alpha.agent_id)

        envelope = alpha.mesh.mirror_adapter.build_remote_metabolism_job(
            target_peer_id="beta-node",
            kind="wake_maintenance",
            topic="remote wake",
            payload={"source": "alpha"},
        )
        response = beta.mesh.accept_job_submission(envelope)
        self.assertEqual(response["status"], "completed")
        self.assertEqual(beta.metabolism.calls[0]["kind"], "wake_maintenance")
        self.assertEqual(beta.metabolism.calls[0]["topic"], "remote wake")

    def test_golem_capability_is_advertised_and_only_accepts_public_jobs(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta", golem_enabled=True)
        self.handshake(alpha, beta)

        manifest = beta.mesh.get_manifest()
        capabilities = {card["name"]: card for card in manifest["organism_card"]["capability_cards"]}
        self.assertTrue(capabilities["golem-provider"]["available"])

        public_job = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "golem.synthetic",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "requirements": {"capabilities": ["golem-provider"]},
                    "policy": {"classification": "public", "mode": "batch"},
                    "payload": {"task": "render"},
                    "artifact_inputs": [],
                }
            },
        )
        response = beta.mesh.accept_job_submission(public_job)
        self.assertEqual(response["status"], "completed")
        self.assertEqual(response["job"]["executor"], "golem-mesh")

        trusted_job = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "golem.synthetic",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "requirements": {"capabilities": ["golem-provider"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"task": "render"},
                    "artifact_inputs": [],
                }
            },
        )
        with self.assertRaises(MeshPolicyError):
            beta.mesh.accept_job_submission(trusted_job)

    def test_queued_shell_job_runs_through_worker_runtime_and_publishes_result_artifact(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self.handshake(alpha, beta)

        worker = beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "python", "worker-runtime"],
            resources={"cpu": 2, "memory_mb": 1024},
            labels=["local", "trusted"],
        )
        self.assertEqual(worker["id"], "beta-worker")

        envelope = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "shell.command",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"], "resources": {"cpu": 1.5, "memory_mb": 128}},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {
                        "command": [sys.executable, "-c", "print('hello queued mesh')"],
                    },
                    "artifact_inputs": [],
                    "artifact_outputs": [{"name": "stdout-log", "media_type": "text/plain"}],
                    "metadata": {"retry_policy": {"max_attempts": 1}},
                }
            },
        )
        submitted = beta.mesh.accept_job_submission(envelope)
        self.assertEqual(submitted["status"], "queued")
        queued_job = submitted["job"]
        self.assertEqual(queued_job["status"], "queued")
        queued_messages = beta.mesh.list_queue_messages(limit=10)
        self.assertEqual(queued_messages["count"], 1)
        self.assertEqual(queued_messages["messages"][0]["job_id"], queued_job["id"])
        self.assertEqual(queued_messages["messages"][0]["status"], "queued")

        preview = beta.mesh.poll_jobs("beta-worker", limit=10)
        self.assertEqual(preview["jobs"][0]["id"], queued_job["id"])

        executed = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(executed["status"], "completed")
        completed_job = executed["job"]
        self.assertEqual(completed_job["status"], "completed")
        self.assertEqual(len(completed_job["attempts"]), 1)
        artifact = beta.mesh.get_artifact(completed_job["result_ref"]["id"])
        result = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(result["stdout"], "hello queued mesh\n")
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(completed_job["queue"]["status"], "acked")
        self.assertEqual(completed_job["queue"]["delivery_attempts"], 1)
        self.assertEqual(completed_job["spec"]["execution"]["runtime_type"], "shell")
        self.assertEqual(completed_job["spec"]["execution"]["command"][:2], [sys.executable, "-c"])
        self.assertEqual(completed_job["spec"]["requirements"]["resources"]["cpu"], 1.5)
        self.assertEqual(completed_job["spec"]["artifacts"]["outputs"][0]["name"], "stdout-log")
        self.assertEqual(completed_job["spec"]["provenance"]["request_id"], envelope["request"]["request_id"])
        self.assertTrue(completed_job["result_bundle_ref"]["id"])
        self.assertTrue(completed_job["result_config_ref"]["id"])
        self.assertTrue(completed_job["result_attestation_ref"]["id"])
        self.assertTrue(completed_job["result_artifacts"]["stdout"]["id"])

        bundle = beta.mesh.get_artifact(completed_job["result_bundle_ref"]["id"])
        bundle_payload = json.loads(base64.b64decode(bundle["content_base64"]).decode("utf-8"))
        self.assertEqual(bundle_payload["artifactType"], "application/vnd.ocp.job-result.v1")
        bundle_roles = {descriptor["role"] for descriptor in bundle_payload["descriptors"]}
        self.assertIn("stdout", bundle_roles)
        self.assertIn("attestation", bundle_roles)

        stdout_artifact = beta.mesh.get_artifact(completed_job["result_artifacts"]["stdout"]["id"])
        stdout_text = base64.b64decode(stdout_artifact["content_base64"]).decode("utf-8")
        self.assertEqual(stdout_text, "hello queued mesh\n")

    def test_artifact_listing_surfaces_checkpoint_and_retention_metadata(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )
        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('checkpoint')"]},
                "artifact_inputs": [],
            },
            request_id="artifact-discovery-checkpoint",
        )
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"], ttl_seconds=120)
        completed = beta.mesh.complete_job_attempt(
            claimed["attempt"]["id"],
            {
                "stdout": "checkpointed\n",
                "stderr": "",
                "exit_code": 0,
                "checkpoint": {"cursor": 7, "phase": "saved"},
            },
            executor="manual-worker",
        )
        job = completed["job"]
        self.assertTrue(job["result_artifacts"]["checkpoint"]["id"])

        listed = beta.mesh.list_artifacts(limit=20, job_id=job["id"])
        listed_ids = {artifact["id"] for artifact in listed["artifacts"]}
        self.assertIn(job["result_ref"]["id"], listed_ids)
        self.assertIn(job["result_bundle_ref"]["id"], listed_ids)
        self.assertIn(job["result_attestation_ref"]["id"], listed_ids)
        self.assertIn(job["result_artifacts"]["stdout"]["id"], listed_ids)
        self.assertIn(job["result_artifacts"]["checkpoint"]["id"], listed_ids)

        checkpoint_page = beta.mesh.list_artifacts(limit=5, artifact_kind="checkpoint", job_id=job["id"])
        self.assertEqual(checkpoint_page["count"], 1)
        self.assertEqual(checkpoint_page["artifacts"][0]["id"], job["result_artifacts"]["checkpoint"]["id"])
        self.assertEqual(checkpoint_page["artifacts"][0]["retention_class"], "durable")

        log_page = beta.mesh.list_artifacts(limit=5, artifact_kind="log", job_id=job["id"])
        self.assertEqual(log_page["count"], 1)
        self.assertEqual(log_page["artifacts"][0]["id"], job["result_artifacts"]["stdout"]["id"])
        self.assertEqual(log_page["artifacts"][0]["retention_class"], "session")

    def test_artifact_retention_purges_expired_entries_and_blobs(self):
        beta = self.make_stack("beta")
        artifact = beta.mesh.publish_local_artifact(
            "temporary artifact",
            media_type="text/plain; charset=utf-8",
            metadata={"artifact_kind": "log", "retention_class": "ephemeral", "retention_seconds": 300},
        )
        artifact_path = Path(artifact["path"])
        self.assertTrue(artifact_path.exists())

        with beta.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_artifacts SET retention_deadline_at=? WHERE id=?",
                ("2000-01-01T00:00:00Z", artifact["id"]),
            )
            conn.commit()

        purged = beta.mesh.purge_expired_artifacts(limit=10)
        self.assertEqual(purged["purged"], 1)
        self.assertFalse(artifact_path.exists())
        listed = beta.mesh.list_artifacts(limit=10, artifact_kind="log")
        self.assertNotIn(artifact["id"], {item["id"] for item in listed["artifacts"]})
        with self.assertRaises(MeshArtifactAccessError):
            beta.mesh.get_artifact(artifact["id"])

    def test_artifact_replication_from_peer_by_artifact_id_preserves_digest_and_content(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        source_artifact = beta.mesh.publish_local_artifact(
            {"kind": "replicated", "value": 7},
            media_type="application/json",
            metadata={"artifact_kind": "bundle", "job_id": "job-replicated"},
        )

        replicated = alpha.mesh.replicate_artifact_from_peer(
            "beta-node",
            artifact_id=source_artifact["id"],
            client=beta_client,
        )

        self.assertEqual(replicated["status"], "replicated")
        self.assertEqual(replicated["artifact"]["digest"], source_artifact["digest"])
        self.assertEqual(replicated["source"]["artifact_id"], source_artifact["id"])
        self.assertEqual(replicated["artifact"]["metadata"]["replicated_from_peer_id"], "beta-node")
        self.assertEqual(replicated["verification"]["status"], "verified")
        self.assertEqual(replicated["artifact"]["mirror_verification"]["status"], "verified")
        fetched = alpha.mesh.get_artifact(replicated["artifact"]["id"])
        payload = json.loads(base64.b64decode(fetched["content_base64"]).decode("utf-8"))
        self.assertEqual(payload, {"kind": "replicated", "value": 7})

    def test_artifact_replication_from_peer_by_digest_reuses_local_cas_entry(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        source_artifact = beta.mesh.publish_local_artifact(
            "digest-only artifact",
            media_type="text/plain; charset=utf-8",
            metadata={"artifact_kind": "log"},
        )

        first = alpha.mesh.replicate_artifact_from_peer(
            "beta-node",
            digest=source_artifact["digest"],
            client=beta_client,
        )
        second = alpha.mesh.replicate_artifact_from_peer(
            "beta-node",
            digest=source_artifact["digest"],
            client=beta_client,
        )

        self.assertEqual(first["status"], "replicated")
        self.assertEqual(second["status"], "already_present")
        self.assertEqual(second["artifact"]["id"], first["artifact"]["id"])
        listed = alpha.mesh.list_artifacts(limit=10, digest=source_artifact["digest"])
        self.assertEqual(listed["count"], 1)
        self.assertEqual(listed["artifacts"][0]["digest"], source_artifact["digest"])

    def test_pinned_replicated_artifact_survives_retention_purge(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        source_artifact = beta.mesh.publish_local_artifact(
            {"kind": "pin-me"},
            media_type="application/json",
            metadata={"artifact_kind": "bundle", "retention_class": "session", "retention_seconds": 60},
        )

        replicated = alpha.mesh.replicate_artifact_from_peer(
            "beta-node",
            artifact_id=source_artifact["id"],
            client=beta_client,
            pin=True,
        )
        self.assertTrue(replicated["artifact"]["pinned"])
        self.assertEqual(replicated["artifact"]["retention_class"], "durable")

        with alpha.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_artifacts SET retention_deadline_at=? WHERE id=?",
                ("2000-01-01T00:00:00Z", replicated["artifact"]["id"]),
            )
            conn.commit()

        purged = alpha.mesh.purge_expired_artifacts(limit=10)
        self.assertEqual(purged["purged"], 0)
        still_there = alpha.mesh.get_artifact(replicated["artifact"]["id"], include_content=False)
        self.assertTrue(still_there["pinned"])

    def test_verify_artifact_mirror_updates_verification_state(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        source_artifact = beta.mesh.publish_local_artifact(
            {"kind": "verify-me"},
            media_type="application/json",
            metadata={"artifact_kind": "bundle"},
        )
        replicated = alpha.mesh.replicate_artifact_from_peer(
            "beta-node",
            artifact_id=source_artifact["id"],
            client=beta_client,
        )

        verified = alpha.mesh.verify_artifact_mirror(
            replicated["artifact"]["id"],
            peer_id="beta-node",
            source_artifact_id=source_artifact["id"],
            client=beta_client,
        )

        self.assertEqual(verified["status"], "verified")
        self.assertTrue(verified["verification"]["verified"])
        self.assertEqual(verified["artifact"]["mirror_verification"]["status"], "verified")
        self.assertEqual(verified["artifact"]["artifact_sync"]["verification_status"], "verified")

    def test_verify_artifact_mirror_reports_missing_remote_source(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        source_artifact = beta.mesh.publish_local_artifact(
            {"kind": "vanish-me"},
            media_type="application/json",
            metadata={"artifact_kind": "bundle"},
        )
        replicated = alpha.mesh.replicate_artifact_from_peer(
            "beta-node",
            artifact_id=source_artifact["id"],
            client=beta_client,
        )
        with beta.mesh._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_artifacts WHERE id=?", (source_artifact["id"],)).fetchone()
        beta.mesh._delete_artifact_row(row, reason="test_cleanup")

        verified = alpha.mesh.verify_artifact_mirror(
            replicated["artifact"]["id"],
            peer_id="beta-node",
            source_artifact_id=source_artifact["id"],
            client=beta_client,
        )

        self.assertEqual(verified["status"], "missing_remote")
        self.assertEqual(verified["artifact"]["mirror_verification"]["reason"], "remote_artifact_not_found")

    def test_treaty_bound_vessel_replication_requires_custody_capable_peer(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack(
            "beta-light",
            device_profile={
                "device_class": "full",
                "execution_tier": "standard",
                "network_profile": "broadband",
                "approval_capable": True,
                "secure_secret_capable": False,
            },
        )
        self._register_default_worker(beta, worker_id="beta-light-worker")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")
        alpha.mesh.propose_treaty(
            treaty_id="treaty/custody-replication-v1",
            title="Custody Replication Treaty",
            document={"witness_required": True, "artifact_export": "sealed"},
        )
        beta.mesh.propose_treaty(
            treaty_id="treaty/custody-replication-v1",
            title="Custody Replication Treaty",
            document={"witness_required": True, "artifact_export": "sealed"},
        )
        mission = beta.mesh.launch_mission(
            title="Treaty Artifact Mission",
            intent="Export a sealed continuity vessel",
            request_id="treaty-artifact-replication",
            continuity={
                "resumable": True,
                "checkpoint_strategy": "manual",
                "treaty_requirements": ["treaty/custody-replication-v1"],
            },
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('sealed vessel')"},
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual", "on_retry": False},
                },
            },
        )
        claimed = beta.mesh.claim_next_job("beta-light-worker", job_id=mission["child_job_ids"][0], ttl_seconds=120)
        beta.mesh.fail_job_attempt(
            claimed["attempt"]["id"],
            error="sealed vessel checkpoint failure",
            retryable=False,
            metadata={"checkpoint": {"cursor": 5, "phase": "saved"}},
        )
        exported = beta.mesh.missions.export_continuity_vessel(mission["id"], dry_run=False)
        remote_vessel = beta.mesh.get_artifact(exported["vessel_ref"]["id"], include_content=False)
        self.assertEqual(remote_vessel["metadata"]["treaty_requirements"], ["treaty/custody-replication-v1"])

        with self.assertRaises(MeshPolicyError):
            alpha.mesh.replicate_artifact_from_peer(
                beta.mesh.node_id,
                artifact_id=exported["vessel_ref"]["id"],
                client=beta_client,
            )

    def test_bundle_graph_replication_pulls_linked_result_artifacts(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('graph bundle')"]},
                "artifact_inputs": [],
            },
            request_id="bundle-graph-sync",
        )
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"], ttl_seconds=120)
        completed = beta.mesh.complete_job_attempt(
            claimed["attempt"]["id"],
            {"stdout": "graph bundle\n", "stderr": "", "exit_code": 0, "checkpoint": {"cursor": 3, "phase": "saved"}},
            executor="beta-worker",
        )
        job = completed["job"]

        graph = alpha.mesh.replicate_artifact_graph_from_peer(
            "beta-node",
            artifact_id=job["result_bundle_ref"]["id"],
            client=beta_client,
            pin=True,
        )

        self.assertEqual(graph["status"], "replicated")
        self.assertGreaterEqual(graph["graph"]["count"], 6)
        linked_digests = {artifact["digest"] for artifact in graph["artifacts"]}
        self.assertEqual(graph["root"]["source"]["artifact_id"], job["result_bundle_ref"]["id"])
        self.assertIn(job["result_ref"]["digest"], linked_digests)
        self.assertIn(job["result_config_ref"]["digest"], linked_digests)
        self.assertIn(job["result_attestation_ref"]["digest"], linked_digests)
        self.assertIn(job["result_artifacts"]["stdout"]["digest"], linked_digests)
        self.assertIn(job["result_artifacts"]["checkpoint"]["digest"], linked_digests)
        alpha_list = alpha.mesh.list_artifacts(limit=20, job_id=job["id"], attempt_id=claimed["attempt"]["id"])
        self.assertGreaterEqual(alpha_list["count"], 6)
        self.assertTrue(all(item["pinned"] for item in alpha_list["artifacts"]))

    def test_checkpoint_graph_replication_pulls_attempt_artifact_set(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('graph checkpoint')"]},
                "artifact_inputs": [],
            },
            request_id="checkpoint-graph-sync",
        )
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"], ttl_seconds=120)
        completed = beta.mesh.complete_job_attempt(
            claimed["attempt"]["id"],
            {"stdout": "graph checkpoint\n", "stderr": "", "exit_code": 0, "checkpoint": {"cursor": 9, "phase": "saved"}},
            executor="beta-worker",
        )
        job = completed["job"]
        checkpoint_id = job["result_artifacts"]["checkpoint"]["id"]

        graph = alpha.mesh.replicate_artifact_graph_from_peer(
            "beta-node",
            artifact_id=checkpoint_id,
            client=beta_client,
        )

        linked_digests = {artifact["digest"] for artifact in graph["artifacts"]}
        self.assertIn(job["result_artifacts"]["checkpoint"]["digest"], linked_digests)
        self.assertIn(job["result_ref"]["digest"], linked_digests)
        self.assertIn(job["result_bundle_ref"]["digest"], linked_digests)
        self.assertIn(job["result_attestation_ref"]["digest"], linked_digests)
        self.assertIn(job["result_config_ref"]["digest"], linked_digests)

    def test_universal_python_job_spec_is_exposed_on_queued_jobs(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["python", "worker-runtime", "shell"],
            resources={"cpu": 1},
        )

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"resources": {"cpu": 1, "memory_mb": 256}},
                "policy": {"classification": "trusted", "mode": "batch", "secret_scopes": ["mesh.test"]},
                "payload": {
                    "code": "print('spec')",
                    "args": ["--flag"],
                    "env": {"MODE": "test"},
                    "env_policy": {"inherit_host_env": False, "allow_env_override": False},
                    "filesystem": {"profile": "workspace", "writable_paths": ["tmp/runtime"]},
                    "secrets": {"API_TOKEN": {"value": "hidden-token", "scope": "mesh.test", "required": True}},
                },
                "artifact_inputs": [{"id": "input-artifact"}],
                "artifact_outputs": [{"name": "result-json", "media_type": "application/json"}],
                "metadata": {
                    "retry_policy": {"max_attempts": 3},
                    "dependencies": ["requests==2.32.0"],
                    "python_version": "3.11",
                    "resumability": {"enabled": True, "max_resume_attempts": 2},
                    "checkpoint_policy": {"enabled": True, "mode": "manual", "on_retry": True},
                },
            },
            request_id="python-spec-visible",
        )

        self.assertEqual(submitted["status"], "queued")
        spec = submitted["job"]["spec"]
        self.assertEqual(spec["execution"]["runtime_type"], "python")
        self.assertEqual(spec["execution"]["inline_code"], "print('spec')")
        self.assertEqual(spec["execution"]["args"], ["--flag"])
        self.assertEqual(spec["execution"]["env"]["MODE"], "test")
        self.assertEqual(spec["execution"]["python_version"], "3.11")
        self.assertEqual(spec["execution"]["dependencies"], ["requests==2.32.0"])
        self.assertEqual(spec["requirements"]["capabilities"], ["python"])
        self.assertEqual(spec["requirements"]["resources"]["memory_mb"], 256)
        self.assertEqual(spec["policy"]["secret_scopes"], ["mesh.test"])
        self.assertEqual(spec["runtime_environment"]["cwd"], "")
        self.assertFalse(spec["runtime_environment"]["env_policy"]["inherit_host_env"])
        self.assertFalse(spec["runtime_environment"]["env_policy"]["allow_env_override"])
        self.assertEqual(spec["runtime_environment"]["filesystem"]["profile"], "workspace")
        self.assertEqual(spec["runtime_environment"]["filesystem"]["writable_paths"], ["tmp/runtime"])
        self.assertEqual(spec["runtime_environment"]["network"]["mode"], "default")
        self.assertEqual(spec["runtime_environment"]["secrets"]["delivery"], "env")
        self.assertEqual(spec["runtime_environment"]["secrets"]["provider_count"], 1)
        self.assertEqual(spec["runtime_environment"]["secrets"]["sources"], ["inline"])
        self.assertEqual(spec["runtime_environment"]["secrets"]["bindings"][0]["env_var"], "API_TOKEN")
        self.assertEqual(spec["runtime_environment"]["secrets"]["bindings"][0]["scope"], "mesh.test")
        self.assertEqual(spec["retries"]["max_attempts"], 3)
        self.assertTrue(spec["checkpoints"]["enabled"])
        self.assertEqual(spec["checkpoints"]["mode"], "manual")
        self.assertEqual(spec["checkpoints"]["retention_class"], "durable")
        self.assertTrue(spec["checkpoints"]["on_retry"])
        self.assertTrue(spec["resumability"]["enabled"])
        self.assertEqual(spec["resumability"]["mode"], "checkpoint")
        self.assertEqual(spec["resumability"]["max_resume_attempts"], 2)
        self.assertIn("checkpointed", spec["status_model"]["states"])
        self.assertIn("checkpointed", spec["status_model"]["failure_states"])
        self.assertEqual(spec["artifacts"]["inputs"][0]["id"], "input-artifact")
        self.assertEqual(spec["artifacts"]["outputs"][0]["name"], "result-json")

    def test_job_spec_surfaces_provider_backed_secret_bindings(self):
        beta = self.make_stack("beta")
        secret_dir = Path(beta.tmpdir) / "secrets"
        secret_dir.mkdir(parents=True, exist_ok=True)
        (secret_dir / "provider-token.txt").write_text("file-token\n", encoding="utf-8")

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch", "secret_scopes": ["mesh.ops"]},
                "payload": {
                    "code": "print('providers')",
                    "env_policy": {"inherit_host_env": False, "allow_env_override": False},
                    "secrets": {
                        "ENV_TOKEN": {"source": "env", "name": "HOST_SECRET_PROVIDER", "scope": "mesh.ops"},
                        "STORE_TOKEN": {"source": "store", "name": "runtime-token", "scope": "mesh.ops"},
                        "FILE_TOKEN": {"source": "file", "path": "secrets/provider-token.txt", "scope": "mesh.ops"},
                    },
                },
                "artifact_inputs": [],
            },
            request_id="provider-secret-spec",
        )

        bindings = {
            binding["env_var"]: binding
            for binding in submitted["job"]["spec"]["runtime_environment"]["secrets"]["bindings"]
        }
        self.assertEqual(submitted["job"]["spec"]["runtime_environment"]["secrets"]["sources"], ["env", "file", "store"])
        self.assertEqual(bindings["ENV_TOKEN"]["provider_ref"], "env:HOST_SECRET_PROVIDER")
        self.assertEqual(bindings["STORE_TOKEN"]["provider_ref"], "store:mesh.ops/runtime-token")
        self.assertEqual(bindings["FILE_TOKEN"]["provider_ref"], "file:secrets/provider-token.txt")

    def test_shell_runtime_environment_injects_secret_bindings_and_honors_env_policy(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )
        previous_shared = os.environ.get("SHARED_ENV")
        previous_host_only = os.environ.get("HOST_ONLY_ENV")
        os.environ["SHARED_ENV"] = "host-shared"
        os.environ["HOST_ONLY_ENV"] = "host-visible"
        try:
            code = (
                "import json, os\n"
                "print(json.dumps({\n"
                "  'shared': os.environ.get('SHARED_ENV', ''),\n"
                "  'host_only': os.environ.get('HOST_ONLY_ENV', ''),\n"
                "  'explicit': os.environ.get('EXPLICIT_ENV', ''),\n"
                "  'inline_token': os.environ.get('INLINE_TOKEN', '')\n"
                "}, sort_keys=True))\n"
            )
            submitted = beta.mesh.submit_local_job(
                {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch", "secret_scopes": ["mesh.ops"]},
                    "payload": {
                        "command": [sys.executable, "-c", code],
                        "env": {"SHARED_ENV": "job-shared", "EXPLICIT_ENV": "job-explicit"},
                        "env_policy": {
                            "inherit_host_env": True,
                            "inherit_env_allowlist": ["SHARED_ENV", "HOST_ONLY_ENV"],
                            "allow_env_override": False,
                        },
                        "secrets": {"INLINE_TOKEN": {"value": "super-secret", "scope": "mesh.ops"}},
                    },
                    "artifact_inputs": [],
                },
                request_id="runtime-env-secret-policy",
            )
            executed = beta.mesh.run_worker_once("beta-worker")
            self.assertEqual(executed["status"], "completed")
            self.assertEqual(executed["job"]["id"], submitted["job"]["id"])
            artifact = beta.mesh.get_artifact(executed["job"]["result_ref"]["id"])
            result = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
            runtime_env = json.loads(result["stdout"])
            self.assertEqual(runtime_env["shared"], "host-shared")
            self.assertEqual(runtime_env["host_only"], "host-visible")
            self.assertEqual(runtime_env["explicit"], "job-explicit")
            self.assertEqual(runtime_env["inline_token"], "super-secret")
            self.assertEqual(executed["job"]["spec"]["runtime_environment"]["secrets"]["bindings"][0]["env_var"], "INLINE_TOKEN")
        finally:
            if previous_shared is None:
                os.environ.pop("SHARED_ENV", None)
            else:
                os.environ["SHARED_ENV"] = previous_shared
            if previous_host_only is None:
                os.environ.pop("HOST_ONLY_ENV", None)
            else:
                os.environ["HOST_ONLY_ENV"] = previous_host_only

    def test_shell_runtime_environment_resolves_provider_backed_secrets_and_attests_delivery(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )
        secret_dir = Path(beta.tmpdir) / "secrets"
        secret_dir.mkdir(parents=True, exist_ok=True)
        (secret_dir / "runtime-token.txt").write_text("file-provider-token\n", encoding="utf-8")
        beta.mesh.put_secret("runtime-token", "store-provider-token", scope="mesh.ops", metadata={"origin": "test"})
        previous_host_secret = os.environ.get("HOST_SECRET_PROVIDER")
        os.environ["HOST_SECRET_PROVIDER"] = "env-provider-token"
        try:
            code = (
                "import json, os\n"
                "print(json.dumps({\n"
                "  'inline': os.environ.get('INLINE_TOKEN', ''),\n"
                "  'env': os.environ.get('ENV_TOKEN', ''),\n"
                "  'store': os.environ.get('STORE_TOKEN', ''),\n"
                "  'file': os.environ.get('FILE_TOKEN', '')\n"
                "}, sort_keys=True))\n"
            )
            submitted = beta.mesh.submit_local_job(
                {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch", "secret_scopes": ["mesh.ops"]},
                    "payload": {
                        "command": [sys.executable, "-c", code],
                        "env_policy": {"inherit_host_env": False, "allow_env_override": False},
                        "secrets": {
                            "INLINE_TOKEN": {"value": "inline-provider-token", "scope": "mesh.ops"},
                            "ENV_TOKEN": {"source": "env", "name": "HOST_SECRET_PROVIDER", "scope": "mesh.ops"},
                            "STORE_TOKEN": {"source": "store", "name": "runtime-token", "scope": "mesh.ops"},
                            "FILE_TOKEN": {"source": "file", "path": "secrets/runtime-token.txt", "scope": "mesh.ops"},
                        },
                    },
                    "artifact_inputs": [],
                },
                request_id="runtime-provider-secret-policy",
            )
            executed = beta.mesh.run_worker_once("beta-worker")
            self.assertEqual(executed["status"], "completed")
            self.assertEqual(executed["job"]["id"], submitted["job"]["id"])
            artifact = beta.mesh.get_artifact(executed["job"]["result_ref"]["id"])
            result = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
            runtime_env = json.loads(result["stdout"])
            self.assertEqual(runtime_env["inline"], "inline-provider-token")
            self.assertEqual(runtime_env["env"], "env-provider-token")
            self.assertEqual(runtime_env["store"], "store-provider-token")
            self.assertEqual(runtime_env["file"], "file-provider-token")
            secret_delivery = executed["job"]["secret_delivery"]
            self.assertEqual({item["source"] for item in secret_delivery}, {"inline", "env", "store", "file"})
            self.assertTrue(all(item["resolved"] for item in secret_delivery))
            self.assertTrue(all(item["value_digest"] for item in secret_delivery))
            rendered_delivery = json.dumps(secret_delivery, sort_keys=True)
            self.assertNotIn("inline-provider-token", rendered_delivery)
            self.assertNotIn("env-provider-token", rendered_delivery)
            self.assertNotIn("store-provider-token", rendered_delivery)
            self.assertNotIn("file-provider-token", rendered_delivery)

            attestation = beta.mesh.get_artifact(executed["job"]["result_attestation_ref"]["id"])
            attestation_payload = json.loads(base64.b64decode(attestation["content_base64"]).decode("utf-8"))
            self.assertEqual(len(attestation_payload["predicate"]["secret_delivery"]), 4)
            self.assertEqual(
                {item["source"] for item in attestation_payload["predicate"]["secret_delivery"]},
                {"inline", "env", "store", "file"},
            )
        finally:
            if previous_host_secret is None:
                os.environ.pop("HOST_SECRET_PROVIDER", None)
            else:
                os.environ["HOST_SECRET_PROVIDER"] = previous_host_secret

    def test_secret_binding_scope_must_be_allowed_by_policy(self):
        beta = self.make_stack("beta")
        with self.assertRaises(MeshPolicyError):
            beta.mesh.submit_local_job(
                {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch", "secret_scopes": ["mesh.allowed"]},
                    "payload": {
                        "command": [sys.executable, "-c", "print('never runs')"],
                        "secrets": {"API_TOKEN": {"value": "denied", "scope": "mesh.denied"}},
                    },
                    "artifact_inputs": [],
                },
                request_id="secret-scope-denied",
            )

    def test_missing_required_store_secret_fails_job_terminally(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )
        beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch", "secret_scopes": ["mesh.ops"]},
                "payload": {
                    "command": [sys.executable, "-c", "print('never runs')"],
                    "secrets": {
                        "STORE_TOKEN": {"source": "store", "name": "missing-token", "scope": "mesh.ops", "required": True}
                    },
                },
                "artifact_inputs": [],
                "metadata": {"retry_policy": {"max_attempts": 1}},
            },
            request_id="missing-store-secret",
        )

        failed = beta.mesh.run_worker_once("beta-worker")

        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["job"]["status"], "failed")
        self.assertIn("required secret binding missing value: STORE_TOKEN", failed["attempt"]["error"])

    def test_resumable_retry_uses_checkpoint_context_on_next_attempt(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["python", "worker-runtime", "shell"],
            resources={"cpu": 1},
        )

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "import os; print(os.environ.get('OCP_RESUME_ARTIFACT_ID', 'fresh'))"},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 2},
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual", "on_retry": True},
                },
            },
            request_id="resumable-retry-job",
        )

        first_claim = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"], ttl_seconds=120)
        failed = beta.mesh.fail_job_attempt(
            first_claim["attempt"]["id"],
            error="checkpointed failure",
            retryable=True,
            metadata={"checkpoint": {"cursor": 11, "phase": "saved"}},
        )
        self.assertEqual(failed["status"], "retry_wait")
        self.assertEqual(failed["job"]["status"], "retry_wait")
        checkpoint_ref = failed["job"]["latest_checkpoint_ref"]
        self.assertTrue(checkpoint_ref["id"])
        self.assertEqual(failed["job"]["resume_checkpoint_ref"]["id"], checkpoint_ref["id"])

        executed = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(executed["status"], "completed")
        completed_job = executed["job"]
        self.assertEqual(completed_job["attempts"][1]["metadata"]["resumed_from_checkpoint_ref"]["id"], checkpoint_ref["id"])
        self.assertEqual(completed_job["resume_checkpoint_ref"], {})

        checkpoint_artifact = beta.mesh.get_artifact(checkpoint_ref["id"])
        checkpoint_payload = json.loads(base64.b64decode(checkpoint_artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(checkpoint_payload["cursor"], 11)

        result_artifact = beta.mesh.get_artifact(completed_job["result_ref"]["id"])
        result_payload = json.loads(base64.b64decode(result_artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(result_payload["stdout"], f"{checkpoint_ref['id']}\n")

    def test_resumable_failure_enters_checkpointed_state_and_surfaces_recovery_fields(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_job(beta, request_id="checkpointed-surface")
        failed = state["failed"]
        self.assertEqual(failed["status"], "checkpointed")
        job = failed["job"]
        self.assertEqual(job["status"], "checkpointed")
        self.assertEqual(job["queue"]["status"], "dead_letter")
        self.assertTrue(job["latest_checkpoint_ref"]["id"])
        self.assertEqual(job["selected_resume_checkpoint_ref"]["id"], job["latest_checkpoint_ref"]["id"])
        self.assertTrue(job["checkpointed_at"])
        self.assertEqual(job["recovery"]["state"], "checkpointed")
        self.assertTrue(job["recovery"]["resumable"])

    def test_operator_resume_from_latest_checkpoint_requeues_and_runs_with_resume_context(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_job(beta, request_id="resume-latest")
        checkpoint_ref = state["failed"]["job"]["latest_checkpoint_ref"]
        self.assertEqual(state["failed"]["queue_message"]["status"], "dead_letter")

        resumed = beta.mesh.resume_job(
            state["failed"]["job"]["id"],
            operator_id="operator-alpha",
            reason="resume latest checkpoint",
        )
        self.assertEqual(resumed["status"], "retry_wait")
        self.assertEqual(resumed["job"]["status"], "retry_wait")
        self.assertEqual(resumed["job"]["resume_checkpoint_ref"]["id"], checkpoint_ref["id"])
        self.assertEqual(resumed["queue_message"]["status"], "queued")

        claimed = beta.mesh.claim_next_job("beta-worker", job_id=resumed["job"]["id"], ttl_seconds=120)
        self.assertEqual(claimed["job"]["status"], "resuming")
        self.assertEqual(claimed["queue_message"]["status"], "inflight")

        completed = beta.mesh.complete_job_attempt(
            claimed["attempt"]["id"],
            {"stdout": f"{checkpoint_ref['id']}\n", "stderr": "", "exit_code": 0},
            metadata={"path": "operator-resume"},
        )
        self.assertEqual(completed["status"], "completed")
        job = completed["job"]
        self.assertEqual(job["attempts"][1]["metadata"]["resumed_from_checkpoint_ref"]["id"], checkpoint_ref["id"])
        self.assertEqual(job["resume_count"], 1)
        self.assertTrue(job["last_resumed_at"])
        self.assertEqual(job["last_resumed_by"], "operator-alpha")
        self.assertEqual(job["last_resume_reason"], "resume latest checkpoint")
        self.assertEqual(job["queue"]["status"], "acked")
        self.assertEqual(job["recovery"]["state"], "completed")

    def test_operator_resume_from_explicit_checkpoint_artifact(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_job(beta, request_id="resume-explicit")
        failed_job = state["failed"]["job"]
        alternate_checkpoint = beta.mesh.publish_local_artifact(
            {"cursor": 99, "phase": "manual-override"},
            media_type="application/json",
            policy=failed_job["policy"],
            metadata={"artifact_kind": "checkpoint", "job_id": failed_job["id"], "retention_class": "durable"},
        )

        resumed = beta.mesh.resume_job_from_checkpoint(
            failed_job["id"],
            checkpoint_artifact_id=alternate_checkpoint["id"],
            operator_id="operator-beta",
            reason="resume explicit checkpoint",
        )
        self.assertEqual(resumed["status"], "retry_wait")
        self.assertEqual(resumed["job"]["resume_checkpoint_ref"]["id"], alternate_checkpoint["id"])

        executed = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(executed["status"], "completed")
        completed_job = executed["job"]
        self.assertEqual(
            completed_job["attempts"][1]["metadata"]["resumed_from_checkpoint_ref"]["id"],
            alternate_checkpoint["id"],
        )
        result_artifact = beta.mesh.get_artifact(completed_job["result_ref"]["id"])
        result_payload = json.loads(base64.b64decode(result_artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(result_payload["stdout"], f"{alternate_checkpoint['id']}\n")

    def test_operator_restart_from_scratch_clears_resume_checkpoint_and_runs_fresh(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_job(beta, request_id="restart-fresh")
        restarted = beta.mesh.restart_job(
            state["failed"]["job"]["id"],
            operator_id="operator-gamma",
            reason="restart from scratch",
        )
        self.assertEqual(restarted["status"], "retry_wait")
        self.assertEqual(restarted["job"]["resume_checkpoint_ref"], {})
        self.assertEqual(restarted["queue_message"]["status"], "queued")

        executed = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(executed["status"], "completed")
        completed_job = executed["job"]
        self.assertEqual(completed_job["attempts"][1]["metadata"]["resumed_from_checkpoint_ref"], {})
        self.assertEqual(completed_job["last_restart_by"], "operator-gamma")
        self.assertEqual(completed_job["last_restart_reason"], "restart from scratch")
        result_artifact = beta.mesh.get_artifact(completed_job["result_ref"]["id"])
        result_payload = json.loads(base64.b64decode(result_artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(result_payload["stdout"], "fresh\n")

    def test_non_resumable_jobs_refuse_resume_controls(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_job(beta, request_id="non-resume-refusal", resumable=False)
        self.assertEqual(state["failed"]["status"], "failed")
        with self.assertRaises(MeshPolicyError):
            beta.mesh.resume_job(state["failed"]["job"]["id"], operator_id="operator-zeta", reason="should fail")

    def test_resume_from_latest_refuses_missing_checkpoint_artifact(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_job(beta, request_id="missing-checkpoint")
        checkpoint_id = state["failed"]["job"]["latest_checkpoint_ref"]["id"]
        with beta.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_artifacts SET retention_deadline_at=? WHERE id=?",
                ("2000-01-01T00:00:00Z", checkpoint_id),
            )
            conn.commit()
        beta.mesh.purge_expired_artifacts(limit=10)

        with self.assertRaises(MeshPolicyError):
            beta.mesh.resume_job(state["failed"]["job"]["id"], operator_id="operator-eta", reason="missing checkpoint")
        self.assertEqual(beta.mesh.get_job(state["failed"]["job"]["id"])["status"], "checkpointed")

    def test_recovery_controls_are_exposed_over_http(self):
        beta = self.make_stack("beta")
        beta_client, _ = self.serve_mesh(beta)

        latest_job = self._checkpointed_job(beta, request_id="http-recovery-latest")["failed"]["job"]
        resumed = beta_client.resume_job(
            latest_job["id"],
            operator_id="http-operator",
            reason="http latest resume",
        )
        self.assertEqual(resumed["status"], "retry_wait")
        self.assertEqual(resumed["job"]["resume_checkpoint_ref"]["id"], latest_job["latest_checkpoint_ref"]["id"])

        explicit_job = self._checkpointed_job(beta, request_id="http-recovery-explicit")["failed"]["job"]
        explicit_checkpoint = beta.mesh.publish_local_artifact(
            {"cursor": 55, "phase": "http-explicit"},
            media_type="application/json",
            policy=explicit_job["policy"],
            metadata={"artifact_kind": "checkpoint", "job_id": explicit_job["id"], "retention_class": "durable"},
        )
        explicit = beta_client.resume_job_from_checkpoint(
            explicit_job["id"],
            checkpoint_artifact_id=explicit_checkpoint["id"],
            operator_id="http-operator",
            reason="http explicit resume",
        )
        self.assertEqual(explicit["status"], "retry_wait")
        self.assertEqual(explicit["job"]["resume_checkpoint_ref"]["id"], explicit_checkpoint["id"])

        restart_job = self._checkpointed_job(beta, request_id="http-recovery-restart")["failed"]["job"]
        restarted = beta_client.restart_job(
            restart_job["id"],
            operator_id="http-operator",
            reason="http restart",
        )
        self.assertEqual(restarted["status"], "retry_wait")
        self.assertEqual(restarted["job"]["resume_checkpoint_ref"], {})

    def test_queued_container_job_runs_through_docker_runtime_and_publishes_result_artifact(self):
        beta = self.make_stack("beta", docker_enabled=True)
        beta.mesh.register_worker(
            worker_id="beta-docker-worker",
            agent_id=beta.agent_id,
            capabilities=["docker", "worker-runtime"],
            resources={"cpu": 2, "memory_mb": 1024},
        )
        docker_calls = []

        def fake_run(argv, **kwargs):
            docker_calls.append({"argv": list(argv), "kwargs": dict(kwargs)})
            if list(argv[:2]) == ["docker", "run"]:
                self.assertIn("python:3.12-alpine", argv)
                self.assertIn("--network", argv)
                self.assertIn("none", argv)
                self.assertIn("-v", argv)
                self.assertIn("/workspace", " ".join(argv))
                env_entries = [argv[idx + 1] for idx, token in enumerate(argv[:-1]) if token == "-e"]
                self.assertTrue(any(item.startswith("GREETING=hello-container") for item in env_entries))
                self.assertTrue(any(item.startswith("INLINE_TOKEN=mesh-secret") for item in env_entries))
                return subprocess.CompletedProcess(argv, 0, stdout="hello from docker\n", stderr="")
            raise AssertionError(f"unexpected subprocess call: {argv}")

        with mock.patch("mesh.sovereign.subprocess.run", side_effect=fake_run):
            submitted = beta.mesh.submit_local_job(
                {
                    "kind": "docker.container",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["docker"]},
                    "policy": {"classification": "trusted", "mode": "batch", "secret_scopes": ["mesh.ops"]},
                    "payload": {
                        "image": "python:3.12-alpine",
                        "command": ["python", "-c", "print('container')"],
                        "args": ["--demo"],
                        "env": {"GREETING": "hello-container"},
                        "secrets": {"INLINE_TOKEN": {"value": "mesh-secret", "scope": "mesh.ops"}},
                        "filesystem": {"profile": "workspace", "writable_paths": ["tmp/build"]},
                        "network": {"mode": "none"},
                    },
                    "artifact_inputs": [],
                },
                request_id="docker-runtime-success",
            )
            self.assertEqual(submitted["status"], "queued")
            executed = beta.mesh.run_worker_once("beta-docker-worker")

        self.assertEqual(executed["status"], "completed")
        job = executed["job"]
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["executor"], "docker-worker")
        self.assertEqual(job["spec"]["execution"]["runtime_type"], "container")
        self.assertEqual(job["spec"]["execution"]["image"], "python:3.12-alpine")
        self.assertEqual(job["spec"]["runtime_environment"]["network"]["mode"], "none")
        self.assertEqual(job["spec"]["runtime_environment"]["filesystem"]["profile"], "workspace")
        artifact = beta.mesh.get_artifact(job["result_ref"]["id"])
        result = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(result["stdout"], "hello from docker\n")
        self.assertEqual(result["image"], "python:3.12-alpine")
        self.assertEqual(result["network_mode"], "none")
        self.assertTrue(result["mounted_workspace"])
        self.assertNotIn("mesh-secret", json.dumps(result, sort_keys=True))
        self.assertIn("INLINE_TOKEN=<redacted>", result["docker_argv"])
        self.assertEqual(len(docker_calls), 1)

    def test_queued_wasm_component_job_runs_through_wasmtime_and_publishes_result_artifact(self):
        beta = self.make_stack("beta", wasm_enabled=True)
        beta.mesh.register_worker(
            worker_id="beta-wasm-worker",
            agent_id=beta.agent_id,
            capabilities=["wasm", "worker-runtime"],
            resources={"cpu": 1, "memory_mb": 512},
        )
        component_artifact = beta.mesh.publish_local_artifact(
            b"\x00asm\x01\x00\x00\x00",
            media_type="application/wasm",
            metadata={"artifact_kind": "component"},
        )
        wasm_calls = []

        def fake_run(argv, **kwargs):
            wasm_calls.append({"argv": list(argv), "kwargs": dict(kwargs)})
            if len(argv) >= 2 and argv[0].endswith("wasmtime") and argv[1] == "run":
                self.assertIn("--invoke", argv)
                self.assertIn("run", argv)
                self.assertIn("--dir", argv)
                self.assertIn(component_artifact["path"], argv)
                env_entries = [argv[idx + 1] for idx, token in enumerate(argv[:-1]) if token == "--env"]
                self.assertTrue(any(item.startswith("MODE=wasm") for item in env_entries))
                self.assertTrue(any(item.startswith("INLINE_TOKEN=mesh-secret") for item in env_entries))
                self.assertTrue(any(item.startswith("OCP_COMPONENT_ID=") for item in env_entries))
                return subprocess.CompletedProcess(argv, 0, stdout="hello from wasm\n", stderr="")
            raise AssertionError(f"unexpected subprocess call: {argv}")

        with mock.patch("mesh.sovereign.subprocess.run", side_effect=fake_run):
            submitted = beta.mesh.submit_local_job(
                {
                    "kind": "wasm.component",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["wasm"]},
                    "policy": {"classification": "trusted", "mode": "batch", "secret_scopes": ["mesh.ops"]},
                    "payload": {
                        "component_ref": {"id": component_artifact["id"], "digest": f"sha256:{component_artifact['digest']}"},
                        "entrypoint": "run",
                        "args": ["--fast"],
                        "env": {"MODE": "wasm"},
                        "secrets": {"INLINE_TOKEN": {"value": "mesh-secret", "scope": "mesh.ops"}},
                        "filesystem": {"profile": "workspace", "writable_paths": ["tmp/components"]},
                        "network": {"mode": "none"},
                    },
                    "artifact_inputs": [],
                },
                request_id="wasm-runtime-success",
            )
            self.assertEqual(submitted["status"], "queued")
            executed = beta.mesh.run_worker_once("beta-wasm-worker")

        self.assertEqual(executed["status"], "completed")
        job = executed["job"]
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["executor"], "wasm-worker")
        self.assertEqual(job["spec"]["execution"]["runtime_type"], "wasm")
        self.assertEqual(job["spec"]["execution"]["component_ref"]["id"], component_artifact["id"])
        self.assertEqual(job["spec"]["runtime_environment"]["network"]["mode"], "none")
        artifact = beta.mesh.get_artifact(job["result_ref"]["id"])
        result = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(result["stdout"], "hello from wasm\n")
        self.assertEqual(result["component_ref"]["id"], component_artifact["id"])
        self.assertEqual(result["network_mode"], "none")
        self.assertNotIn("mesh-secret", json.dumps(result, sort_keys=True))
        self.assertIn("INLINE_TOKEN=<redacted>", result["wasm_argv"])
        self.assertTrue(result["preopened_dir"])
        self.assertEqual(len(wasm_calls), 1)

    def test_container_and_wasm_jobs_reject_when_runtimes_are_unavailable(self):
        beta = self.make_stack("beta", docker_enabled=False, wasm_enabled=False)

        docker_job = beta.mesh.submit_local_job(
            {
                "kind": "docker.container",
                "dispatch_mode": "queued",
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {
                    "image": "python:3.12-alpine",
                    "command": ["python", "-c", "print('container')"],
                    "args": ["--demo"],
                    "env": {"HELLO": "world"},
                },
                "artifact_inputs": [],
            },
            request_id="docker-spec-rejected",
        )
        self.assertEqual(docker_job["status"], "rejected")
        self.assertEqual(docker_job["job"]["spec"]["execution"]["runtime_type"], "container")
        self.assertEqual(docker_job["job"]["spec"]["execution"]["image"], "python:3.12-alpine")
        self.assertIn("docker", docker_job["job"]["spec"]["requirements"]["capabilities"])

        wasm_job = beta.mesh.submit_local_job(
            {
                "kind": "wasm.component",
                "dispatch_mode": "queued",
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {
                    "component_ref": {"id": "component-demo", "digest": "sha256:abc123"},
                    "entrypoint": "run",
                    "args": ["--fast"],
                },
                "artifact_inputs": [],
            },
            request_id="wasm-spec-rejected",
        )
        self.assertEqual(wasm_job["status"], "rejected")
        self.assertEqual(wasm_job["job"]["spec"]["execution"]["runtime_type"], "wasm")
        self.assertEqual(wasm_job["job"]["spec"]["execution"]["component_ref"]["id"], "component-demo")
        self.assertIn("wasm", wasm_job["job"]["spec"]["requirements"]["capabilities"])

    def test_queued_job_retries_after_worker_failure_and_then_completes(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self.handshake(alpha, beta)
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["python", "shell", "worker-runtime"],
            resources={"cpu": 2, "memory_mb": 1024},
        )

        sentinel_path = Path(beta.tmpdir) / "retry-sentinel.txt"
        code = (
            "from pathlib import Path\n"
            f"p = Path({str(sentinel_path)!r})\n"
            "import sys\n"
            "if p.exists():\n"
            "    print('second-pass')\n"
            "    sys.exit(0)\n"
            "p.write_text('first-pass', encoding='utf-8')\n"
            "print('retry-me')\n"
            "sys.exit(2)\n"
        )
        envelope = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "python.inline",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"code": code},
                    "artifact_inputs": [],
                    "metadata": {"retry_policy": {"max_attempts": 2}},
                }
            },
        )
        submitted = beta.mesh.accept_job_submission(envelope)
        self.assertEqual(submitted["status"], "queued")

        first_attempt = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(first_attempt["status"], "retry_wait")
        retried_job = first_attempt["job"]
        self.assertEqual(retried_job["status"], "retry_wait")
        self.assertEqual(len(retried_job["attempts"]), 1)
        self.assertTrue(sentinel_path.exists())
        self.assertEqual(first_attempt["queue_message"]["status"], "queued")
        self.assertEqual(first_attempt["queue_message"]["delivery_attempts"], 1)

        second_attempt = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(second_attempt["status"], "completed")
        completed_job = second_attempt["job"]
        self.assertEqual(completed_job["status"], "completed")
        self.assertEqual(len(completed_job["attempts"]), 2)
        self.assertEqual(completed_job["attempts"][0]["status"], "failed")
        self.assertEqual(completed_job["attempts"][1]["status"], "completed")
        artifact = beta.mesh.get_artifact(completed_job["result_ref"]["id"])
        result = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(result["stdout"], "second-pass\n")
        self.assertEqual(second_attempt["queue_message"]["status"], "acked")
        self.assertEqual(second_attempt["queue_message"]["delivery_attempts"], 2)

    def test_queue_message_visibility_timeout_requeues_and_allows_redelivery(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self.handshake(alpha, beta)
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )

        envelope = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "shell.command",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", "print('redelivery')"]},
                    "artifact_inputs": [],
                }
            },
        )
        submitted = beta.mesh.accept_job_submission(envelope)
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"], ttl_seconds=120)
        self.assertEqual(claimed["status"], "claimed")
        queue_id = claimed["queue_message"]["id"]
        attempt_id = claimed["attempt"]["id"]

        with beta.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_queue_messages SET visibility_timeout_at=?, updated_at=? WHERE id=?",
                ("2000-01-01T00:00:00Z", "2000-01-01T00:00:00Z", queue_id),
            )
            conn.commit()

        polled = beta.mesh.poll_jobs("beta-worker", limit=10)
        self.assertEqual(polled["jobs"][0]["id"], submitted["job"]["id"])
        requeued_job = beta.mesh.get_job(submitted["job"]["id"])
        self.assertEqual(requeued_job["status"], "queued")
        self.assertEqual(requeued_job["queue"]["status"], "queued")
        self.assertEqual(requeued_job["attempts"][0]["status"], "expired")

        reclaimed = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"], ttl_seconds=120)
        self.assertEqual(reclaimed["status"], "claimed")
        self.assertEqual(reclaimed["attempt"]["attempt_number"], 2)
        self.assertEqual(reclaimed["queue_message"]["delivery_attempts"], 2)
        self.assertNotEqual(reclaimed["attempt"]["id"], attempt_id)

    def test_queued_submission_dedupes_by_dedupe_key(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )

        first = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('once')"]},
                "artifact_inputs": [],
                "metadata": {"dedupe_key": "same-job"},
            },
            request_id="dedupe-one",
        )
        second = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('twice')"]},
                "artifact_inputs": [],
                "metadata": {"dedupe_key": "same-job"},
            },
            request_id="dedupe-two",
        )

        self.assertEqual(first["job"]["id"], second["job"]["id"])
        self.assertTrue(second["deduped"])
        queued_messages = beta.mesh.list_queue_messages(limit=10)
        self.assertEqual(queued_messages["count"], 1)

    def test_queue_events_metrics_and_replay_are_exposed_over_http(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-http-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )
        beta_client, _ = self.serve_mesh(beta)

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "import sys; sys.exit(7)"]},
                "artifact_inputs": [],
                "metadata": {"retry_policy": {"max_attempts": 1}},
            },
            request_id="http-queue-dead-letter",
        )
        job_id = submitted["job"]["id"]
        first_claim = beta_client.claim_job("beta-http-worker", job_id=job_id, ttl_seconds=120)
        failed = beta_client.fail_attempt(first_claim["attempt"]["id"], error="http dead letter", retryable=True)
        self.assertEqual(failed["status"], "failed")
        queue_message_id = failed["queue_message"]["id"]
        self.assertEqual(failed["queue_message"]["status"], "dead_letter")

        first_page = beta_client.list_queue_events(limit=2, job_id=job_id)
        self.assertEqual(first_page["count"], 2)
        self.assertGreater(first_page["next_cursor"], 0)
        second_page = beta_client.list_queue_events(since_seq=first_page["next_cursor"], limit=10, job_id=job_id)
        event_types = [item["event_type"] for item in first_page["events"] + second_page["events"]]
        self.assertIn("mesh.queue.enqueued", event_types)
        self.assertIn("mesh.queue.claimed", event_types)
        self.assertIn("mesh.queue.dead_lettered", event_types)

        metrics = beta_client.queue_metrics()
        self.assertEqual(metrics["counts"]["dead_letter"], 1)
        self.assertEqual(metrics["workers"]["registered"], 1)

        replayed = beta_client.replay_queue_message(job_id=job_id, reason="operator replay")
        self.assertEqual(replayed["status"], "queued")
        self.assertEqual(replayed["queue_message"]["id"], queue_message_id)
        self.assertEqual(replayed["queue_message"]["status"], "queued")

        replay_events = beta_client.list_queue_events(limit=20, queue_message_id=queue_message_id)
        self.assertIn("mesh.queue.replayed", [item["event_type"] for item in replay_events["events"]])

    def test_queue_metrics_report_backpressure(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
            max_concurrent_jobs=1,
        )
        for idx in range(2):
            beta.mesh.submit_local_job(
                {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", f"print('backlog-{idx}')"]},
                    "artifact_inputs": [],
                },
                request_id=f"backpressure-{idx}",
            )

        metrics = beta.mesh.queue_metrics()
        self.assertEqual(metrics["counts"]["queued"], 2)
        self.assertEqual(metrics["workers"]["total_slots"], 1)
        self.assertEqual(metrics["pressure"], "elevated")
        self.assertGreater(metrics["scheduler_penalty"], 0)

    def test_queue_policy_controls_ack_deadlines_and_dead_letter_routing(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )
        beta_client, _ = self.serve_mesh(beta)

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "import sys; sys.exit(9)"]},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "queue_policy": {
                        "ack_deadline_seconds": 600,
                        "replay_window_seconds": 120,
                        "retention_seconds": 1800,
                        "dead_letter_queue": "critical.dlq",
                    },
                },
            },
            request_id="queue-policy-dead-letter",
        )
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"])
        self.assertEqual(claimed["job"]["lease"]["ttl_seconds"], 600)

        updated = beta_client.set_queue_ack_deadline(
            queue_message_id=claimed["queue_message"]["id"],
            ttl_seconds=720,
            reason="operator extend",
        )
        self.assertEqual(updated["status"], "ok")
        self.assertEqual(updated["queue_message"]["ack_deadline_seconds"], 720)

        failed = beta.mesh.fail_job_attempt(claimed["attempt"]["id"], error="fatal queue policy", retryable=True)
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["queue_message"]["status"], "dead_letter")
        self.assertEqual(failed["queue_message"]["queue_name"], "critical.dlq")
        self.assertEqual(failed["queue_message"]["dead_letter_queue"], "critical.dlq")
        self.assertTrue(failed["queue_message"]["replay_deadline_at"])
        self.assertTrue(failed["queue_message"]["retention_deadline_at"])

    def test_queue_policy_replay_window_is_enforced(self):
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["shell", "worker-runtime"],
            resources={"cpu": 1},
        )

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "import sys; sys.exit(4)"]},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "queue_policy": {"replay_window_seconds": 60},
                },
            },
            request_id="queue-policy-replay-window",
        )
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"])
        failed = beta.mesh.fail_job_attempt(claimed["attempt"]["id"], error="expired replay", retryable=True)
        self.assertEqual(failed["queue_message"]["status"], "dead_letter")

        with beta.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_queue_messages SET replay_deadline_at=?, updated_at=? WHERE id=?",
                ("2000-01-01T00:00:00Z", "2000-01-01T00:00:00Z", failed["queue_message"]["id"]),
            )
            conn.commit()

        with self.assertRaises(MeshPolicyError):
            beta.mesh.replay_queue_message(job_id=submitted["job"]["id"], reason="too late")

    def test_queue_retention_policy_purges_final_messages_after_deadline(self):
        beta = self.make_stack("beta")
        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('cancelled')"]},
                "artifact_inputs": [],
                "metadata": {"queue_policy": {"retention_seconds": 600}},
            },
            request_id="queue-retention-policy",
        )
        cancelled = beta.mesh.cancel_job(submitted["job"]["id"], reason="retention test")
        self.assertEqual(cancelled["queue"]["status"], "cancelled")

        with beta.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_queue_messages SET retention_deadline_at=?, updated_at=? WHERE job_id=?",
                ("2000-01-01T00:00:00Z", "2000-01-01T00:00:00Z", submitted["job"]["id"]),
            )
            conn.commit()

        listed = beta.mesh.list_queue_messages(limit=10)
        self.assertEqual(listed["count"], 0)
        self.assertEqual(beta.mesh.get_job(submitted["job"]["id"])["queue"], {})

    def test_worker_http_endpoints_register_poll_claim_and_complete_job(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        registered = beta_client.register_worker(
            {
                "worker_id": "beta-http-worker",
                "agent_id": beta.agent_id,
                "capabilities": ["worker-runtime", "shell"],
                "resources": {"cpu": 1, "memory_mb": 512},
                "labels": ["http"],
                "max_concurrent_jobs": 1,
            }
        )
        self.assertEqual(registered["status"], "ok")
        workers = beta_client.list_workers(limit=10)
        self.assertEqual(workers["workers"][0]["id"], "beta-http-worker")

        envelope = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "shell.command",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", "print('http-complete')"]},
                    "artifact_inputs": [],
                }
            },
        )
        submitted = beta_client.submit_job(envelope)
        self.assertEqual(submitted["status"], "queued")
        queued_job = submitted["job"]

        polled = beta_client.poll_jobs("beta-http-worker", limit=10)
        self.assertEqual(polled["jobs"][0]["id"], queued_job["id"])

        claimed = beta_client.claim_job("beta-http-worker", ttl_seconds=120)
        self.assertEqual(claimed["status"], "claimed")
        attempt_id = claimed["attempt"]["id"]
        beta_client.heartbeat_attempt(attempt_id, ttl_seconds=120, metadata={"phase": "ready"})

        completed = beta_client.complete_attempt(
            attempt_id,
            {"stdout": "http-complete\n", "stderr": "", "exit_code": 0},
            executor="http-worker",
            metadata={"path": "manual-http-complete"},
        )
        self.assertEqual(completed["status"], "completed")
        job = completed["job"]
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["attempts"][0]["metadata"]["phase"], "ready")
        artifact = beta.mesh.get_artifact(job["result_ref"]["id"])
        result = json.loads(base64.b64decode(artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(result["stdout"], "http-complete\n")

    def test_artifact_discovery_and_purge_are_exposed_over_http(self):
        beta = self.make_stack("beta")
        beta_client, _ = self.serve_mesh(beta)
        artifact = beta.mesh.publish_local_artifact(
            {"kind": "bundle-demo"},
            media_type="application/json",
            metadata={"artifact_kind": "bundle", "job_id": "job-http-bundle"},
        )

        listed = beta_client.list_artifacts(limit=10, artifact_kind="bundle", job_id="job-http-bundle")
        self.assertEqual(listed["count"], 1)
        self.assertEqual(listed["artifacts"][0]["id"], artifact["id"])
        self.assertEqual(listed["artifacts"][0]["artifact_kind"], "bundle")

        with beta.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_artifacts SET retention_class=?, retention_deadline_at=? WHERE id=?",
                ("ephemeral", "2000-01-01T00:00:00Z", artifact["id"]),
            )
            conn.commit()

        purged = beta_client.purge_artifacts(limit=10)
        self.assertEqual(purged["purged"], 1)
        post_purge = beta_client.list_artifacts(limit=10, artifact_kind="bundle", job_id="job-http-bundle")
        self.assertEqual(post_purge["count"], 0)

    def test_artifact_replication_is_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha_client, _ = self.serve_mesh(alpha)
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        artifact = beta.mesh.publish_local_artifact(
            {"kind": "http-replication"},
            media_type="application/json",
            metadata={"artifact_kind": "bundle", "job_id": "job-http-replication"},
        )

        replicated = alpha_client.replicate_artifact(
            peer_id="beta-node",
            artifact_id=artifact["id"],
            pin=True,
        )
        self.assertEqual(replicated["status"], "replicated")
        self.assertEqual(replicated["artifact"]["digest"], artifact["digest"])
        self.assertTrue(replicated["artifact"]["metadata"]["artifact_sync"]["pinned"])

        listed = alpha_client.list_artifacts(limit=10, digest=artifact["digest"])
        self.assertEqual(listed["count"], 1)
        self.assertEqual(listed["artifacts"][0]["digest"], artifact["digest"])

        verified = alpha_client.verify_artifact_mirror(
            replicated["artifact"]["id"],
            peer_id="beta-node",
            source_artifact_id=artifact["id"],
        )
        self.assertEqual(verified["status"], "verified")

        pinned = alpha_client.set_artifact_pin(replicated["artifact"]["id"], pinned=True, reason="http-pin")
        self.assertEqual(pinned["status"], "ok")
        self.assertTrue(pinned["artifact"]["pinned"])

    def test_artifact_graph_replication_is_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha_client, _ = self.serve_mesh(alpha)
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        result = beta.mesh.publish_local_artifact(
            {"kind": "graph-http-result"},
            media_type="application/json",
            metadata={"artifact_kind": "result", "job_id": "job-http-graph", "attempt_id": "attempt-http-graph"},
        )
        config = beta.mesh.publish_local_artifact(
            {"kind": "ocp.artifact.config", "result": {"artifact_id": result["id"], "digest": result["digest"], "media_type": result["media_type"]}},
            media_type="application/vnd.ocp.job-result.config.v1+json",
            metadata={"artifact_kind": "config", "job_id": "job-http-graph", "attempt_id": "attempt-http-graph"},
        )
        attestation = beta.mesh.publish_local_artifact(
            {"kind": "ocp.execution.attestation", "subject": {"artifact_id": result["id"], "digest": result["digest"], "media_type": result["media_type"]}},
            media_type="application/vnd.ocp.artifact.attestation.v1+json",
            metadata={"artifact_kind": "attestation", "job_id": "job-http-graph", "attempt_id": "attempt-http-graph"},
        )
        bundle = beta.mesh.publish_local_artifact(
            {
                "kind": "ocp.artifact.bundle",
                "bundle_type": "job-result",
                "primary": {"id": result["id"], "digest": result["digest"], "media_type": result["media_type"], "size_bytes": result["size_bytes"], "role": "result"},
                "config": beta.mesh._oci_descriptor(config),
                "subject": beta.mesh._oci_descriptor(result),
                "descriptors": [
                    beta.mesh._artifact_descriptor(result, role="result"),
                    beta.mesh._artifact_descriptor(attestation, role="attestation"),
                ],
            },
            media_type="application/vnd.oci.image.manifest.v1+json",
            metadata={
                "artifact_kind": "bundle",
                "bundle_type": "job-result",
                "job_id": "job-http-graph",
                "attempt_id": "attempt-http-graph",
                "primary_artifact_id": result["id"],
                "config_artifact_id": config["id"],
                "attestation_artifact_id": attestation["id"],
                "subject_artifact_id": result["id"],
            },
        )

        graph = alpha_client.replicate_artifact_graph(
            peer_id="beta-node",
            artifact_id=bundle["id"],
            pin=True,
        )
        self.assertEqual(graph["status"], "replicated")
        self.assertGreaterEqual(graph["graph"]["count"], 4)
        replicated_digests = {artifact["digest"] for artifact in graph["artifacts"]}
        self.assertIn(result["digest"], replicated_digests)
        self.assertIn(config["digest"], replicated_digests)
        self.assertIn(attestation["digest"], replicated_digests)

    def test_worker_http_fail_endpoint_requeues_until_retry_budget_exhausts(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")
        beta_client.register_worker(
            {
                "worker_id": "beta-http-worker",
                "agent_id": beta.agent_id,
                "capabilities": ["worker-runtime", "shell"],
                "resources": {"cpu": 1},
            }
        )

        envelope = alpha.mesh.build_signed_envelope(
            "/mesh/jobs/submit",
            {
                "job": {
                    "kind": "shell.command",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", "print('will not run')"]},
                    "artifact_inputs": [],
                    "metadata": {"retry_policy": {"max_attempts": 2}},
                }
            },
        )
        submitted = beta_client.submit_job(envelope)
        queued_job = submitted["job"]

        first_claim = beta_client.claim_job("beta-http-worker", job_id=queued_job["id"], ttl_seconds=120)
        first_fail = beta_client.fail_attempt(first_claim["attempt"]["id"], error="first http fail", retryable=True)
        self.assertEqual(first_fail["status"], "retry_wait")

        second_claim = beta_client.claim_job("beta-http-worker", job_id=queued_job["id"], ttl_seconds=120)
        second_fail = beta_client.fail_attempt(second_claim["attempt"]["id"], error="second http fail", retryable=True)
        self.assertEqual(second_fail["status"], "failed")
        self.assertEqual(second_fail["job"]["status"], "failed")
        self.assertEqual(len(second_fail["job"]["attempts"]), 2)

    def test_manifest_and_stream_include_registered_workers(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )

        manifest = alpha.mesh.get_manifest()
        stream = alpha.mesh.stream_snapshot(limit=20)
        self.assertEqual(manifest["workers"][0]["id"], "alpha-worker")
        self.assertEqual(stream["workers"][0]["id"], "alpha-worker")
        self.assertEqual(manifest["reliability"]["source"], "local_jobs")
        self.assertEqual(manifest["reliability"]["total"], 0)

    def test_manifest_surfaces_treaty_capabilities_and_governance_summary(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.propose_treaty(
            treaty_id="treaty/manifest-v1",
            title="Manifest Treaty",
            document={"witness_required": True},
        )

        manifest = alpha.mesh.get_manifest()

        self.assertTrue(manifest["treaty_capabilities"]["treaty_documents"])
        self.assertTrue(manifest["treaty_capabilities"]["continuity_validation"])
        self.assertEqual(manifest["governance_summary"]["count"], 1)
        self.assertEqual(manifest["governance_summary"]["active_treaty_ids"], ["treaty/manifest-v1"])
        self.assertEqual(manifest["organism_card"]["governance_summary"]["count"], 1)

    def test_manifest_stream_and_peer_sync_surface_device_profiles(self):
        alpha = self.make_stack(
            "alpha",
            device_profile={
                "device_class": "full",
                "execution_tier": "standard",
                "network_profile": "wired",
                "form_factor": "workstation",
            },
        )
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "light",
                "execution_tier": "light",
                "power_profile": "battery",
                "network_profile": "wifi",
                "mobility": "mobile",
                "form_factor": "phone",
                "approval_capable": True,
                "artifact_mirror_capable": False,
            },
        )
        beta_client, base_url = self.serve_mesh(beta)
        manifest = alpha.mesh.get_manifest()
        stream = alpha.mesh.stream_snapshot(limit=20)

        self.assertEqual(manifest["device_profile"]["device_class"], "full")
        self.assertEqual(manifest["organism_card"]["device_profile"]["form_factor"], "workstation")
        self.assertEqual(stream["device_profile"]["network_profile"], "wired")
        self.assertEqual(manifest["sync_policy"]["mode"], "continuous")

        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", client=beta_client, refresh_manifest=True)
        peer = alpha.mesh.list_peers(limit=10)["peers"][0]

        self.assertEqual(peer["device_profile"]["device_class"], "light")
        self.assertEqual(peer["device_profile"]["form_factor"], "phone")
        self.assertTrue(peer["device_profile"]["approval_capable"])
        self.assertEqual(peer["metadata"]["remote_device_profile"]["network_profile"], "wifi")
        self.assertEqual(peer["sync_policy"]["mode"], "intermittent")
        self.assertEqual(peer["heartbeat"]["sync_mode"], "intermittent")
        self.assertTrue(peer["sync_policy"]["sleep_capable"])
        self.assertIn("operator", peer["habitat_roles"])
        self.assertTrue(peer["continuity_capabilities"]["restore_dry_run"])
        self.assertTrue(peer["continuity_capabilities"]["long_sleep"])
        self.assertTrue(peer["treaty_capabilities"]["treaty_documents"])
        self.assertEqual(peer["treaty_compatibility"]["advisory_state"], "full")
        self.assertTrue(peer["treaty_compatibility"]["shared_treaty_validation"])

    def test_scheduler_prefers_local_worker_when_available(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        scheduled = alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "payload": {"command": [sys.executable, "-c", "print('scheduled-local')"]},
                "artifact_inputs": [],
            }
        )
        self.assertEqual(scheduled["status"], "queued")
        self.assertEqual(scheduled["decision"]["selected"]["target_type"], "local")
        self.assertEqual(scheduled["decision"]["selected"]["peer_id"], "alpha-node")
        self.assertEqual(scheduled["job"]["target"], "alpha-node")

    def test_scheduler_routes_to_trusted_remote_peer_when_local_disabled(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        alpha_client, alpha_base_url = self.serve_mesh(alpha)
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        scheduled = alpha_client.schedule_job(
            {
                "job": {
                    "kind": "shell.command",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "dispatch_mode": "queued",
                    "payload": {"command": [sys.executable, "-c", "print('scheduled-remote')"]},
                    "artifact_inputs": [],
                },
                "allow_local": False,
            }
        )
        self.assertEqual(scheduled["status"], "queued")
        self.assertEqual(scheduled["decision"]["selected"]["target_type"], "peer")
        self.assertEqual(scheduled["decision"]["selected"]["peer_id"], "beta-node")
        remote_jobs = beta.mesh.poll_jobs("beta-worker", limit=10)
        self.assertEqual(remote_jobs["jobs"][0]["origin"], "alpha-node")
        self.assertEqual(remote_jobs["jobs"][0]["target"], "beta-node")

    def test_cooperative_task_spreads_child_jobs_across_local_and_remote_peers(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        task = alpha.mesh.launch_cooperative_task(
            name="dual-node-shell",
            request_id="cooperative-shell-1",
            target_peer_ids=["alpha-node", "beta-node"],
            base_job={
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('base')"]},
                "artifact_inputs": [],
            },
            shards=[
                {"label": "local", "payload": {"command": [sys.executable, "-c", "print('local-shard')"]}},
                {"label": "remote", "payload": {"command": [sys.executable, "-c", "print('remote-shard')"]}},
            ],
        )

        self.assertEqual(task["shard_count"], 2)
        self.assertEqual({child["peer_id"] for child in task["children"]}, {"alpha-node", "beta-node"})
        beta_jobs = beta.mesh.poll_jobs("beta-worker", limit=10)
        self.assertEqual(beta_jobs["jobs"][0]["origin"], "alpha-node")
        self.assertEqual(beta_jobs["jobs"][0]["target"], "beta-node")

        alpha.mesh.run_worker_once("alpha-worker")
        beta.mesh.run_worker_once("beta-worker")

        refreshed = alpha.mesh.get_cooperative_task(task["id"])
        self.assertEqual(refreshed["state"], "completed")
        self.assertEqual(refreshed["summary"]["counts"]["completed"], 2)

    def test_scheduler_stay_local_unplaces_when_no_local_worker_exists(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"stay_local": True, "queue_class": "latency_sensitive"},
                "payload": {"command": [sys.executable, "-c", "print('never placed')"]},
                "artifact_inputs": [],
            }
        )
        self.assertEqual(decision["status"], "unplaced")
        self.assertEqual(decision["placement"]["stay_local"], True)
        self.assertIn("stay_local", decision["candidates"][0]["reasons"])

    def test_scheduler_avoid_public_prefers_trusted_peer_for_public_batch_jobs(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        gamma = self.make_stack("gamma")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        _, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="public")

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "public", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"avoid_public": True, "queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('avoid-public')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "beta-node")
        gamma_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "gamma-node")
        self.assertIn("avoid_public", gamma_candidate["reasons"])

    def test_scheduler_latency_sensitive_prefers_local_even_with_remote_preferred_peer(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {
                    "queue_class": "latency_sensitive",
                    "latency_sensitive": True,
                    "preferred_peer_ids": ["beta-node"],
                },
                "payload": {"command": [sys.executable, "-c", "print('latency-local')"]},
                "artifact_inputs": [],
            }
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["target_type"], "local")
        self.assertEqual(decision["selected"]["peer_id"], "alpha-node")
        self.assertIn("queue_class=latency_sensitive", decision["selected"]["reasons"])

    def test_scheduler_avoids_light_phone_for_heavier_compute_jobs(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "light",
                "execution_tier": "light",
                "power_profile": "battery",
                "network_profile": "wifi",
                "mobility": "mobile",
                "form_factor": "phone",
            },
        )
        gamma = self.make_stack(
            "gamma",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "network_profile": "wired",
                "form_factor": "server",
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1, "memory_mb": 1024},
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 4, "memory_mb": 8192},
        )
        _, beta_base_url = self.serve_mesh(beta)
        _, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)
        alpha.mesh.sync_peer("gamma-node", limit=20, refresh_manifest=True)

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"], "resources": {"cpu": 2, "memory_mb": 2048}},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"prefer_low_backlog": True},
                "payload": {"command": [sys.executable, "-c", "print('device-aware')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )

        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "gamma-node")
        beta_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "beta-node")
        self.assertIn("device_cpu_limit", beta_candidate["reasons"])

    def test_scheduler_rejects_intermittent_peer_for_non_resumable_job(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "light",
                "execution_tier": "light",
                "power_profile": "battery",
                "network_profile": "intermittent",
                "mobility": "mobile",
                "form_factor": "phone",
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1, "memory_mb": 1024},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"], "resources": {"cpu": 1}},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "payload": {"command": [sys.executable, "-c", "print('non-resumable')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )

        self.assertEqual(decision["status"], "unplaced")
        beta_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "beta-node")
        self.assertIn("intermittent_requires_resumable_job", beta_candidate["reasons"])

    def test_scheduler_allows_resumable_job_on_intermittent_peer(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "light",
                "execution_tier": "light",
                "power_profile": "battery",
                "network_profile": "intermittent",
                "mobility": "mobile",
                "form_factor": "phone",
                "artifact_mirror_capable": True,
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1, "memory_mb": 1024},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"], "resources": {"cpu": 1}},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "payload": {"command": [sys.executable, "-c", "print('resumable')"]},
                "artifact_inputs": [],
                "metadata": {
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "on_retry": True},
                    "retry_policy": {"max_attempts": 2},
                },
            },
            allow_local=False,
        )

        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "beta-node")
        self.assertIn("intermittent_resume_capable", decision["selected"]["reasons"])

    def test_checkpointed_failure_on_intermittent_node_surfaces_recovery_hint(self):
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "light",
                "execution_tier": "light",
                "power_profile": "battery",
                "network_profile": "intermittent",
                "mobility": "mobile",
                "form_factor": "phone",
                "artifact_mirror_capable": True,
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('mobile-checkpoint')"]},
                "artifact_inputs": [],
                "metadata": {
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "on_retry": False},
                    "retry_policy": {"max_attempts": 1},
                },
            },
            request_id="mobile-checkpointed-failure",
        )
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=submitted["job"]["id"], ttl_seconds=120)
        failed = beta.mesh.fail_job_attempt(
            claimed["attempt"]["id"],
            error="mobile drop",
            retryable=True,
            metadata={"checkpoint": {"cursor": 2, "phase": "saved"}},
        )

        self.assertEqual(failed["status"], "checkpointed")
        self.assertEqual(failed["job"]["status"], "checkpointed")
        self.assertEqual(failed["job"]["recovery"]["recovery_hint"]["strategy"], "resume_on_stable_peer")
        self.assertEqual(failed["job"]["recovery"]["recovery_hint"]["recommended_action"], "resume")
        self.assertEqual(
            failed["job"]["recovery"]["recovery_hint"]["preferred_target_device_classes"],
            ["full", "relay"],
        )

    def test_notifications_surface_compact_mobile_presentation_and_ack_flow(self):
        alpha = self.make_stack("alpha")
        notification = alpha.mesh.publish_notification(
            notification_type="job.summary",
            priority="high",
            title="Long running job completed",
            body="Result bundle is ready for review on the stable relay node and includes logs plus checkpoint lineage.",
            target_peer_id="watch-node",
            target_device_classes=["micro"],
            metadata={"job_id": "job-123"},
        )

        self.assertTrue(notification["presentation"]["compact"])
        self.assertTrue(notification["compact_title"])
        listed = alpha.mesh.list_notifications(limit=10, target_peer_id="watch-node")
        self.assertEqual(listed["count"], 1)
        self.assertEqual(listed["notifications"][0]["status"], "unread")

        acked = alpha.mesh.ack_notification(
            notification["id"],
            actor_peer_id="watch-node",
            actor_agent_id="watch-agent",
            reason="seen",
        )
        self.assertEqual(acked["status"], "acked")
        self.assertEqual(acked["metadata"]["last_actor_peer_id"], "watch-node")

    def test_mesh_exposes_governance_service_for_notifications_and_approvals(self):
        alpha = self.make_stack("alpha-governance")

        notification = alpha.mesh.governance.publish_notification(
            notification_type="job.summary",
            priority="high",
            title="Governance seam",
            body="Governance service can publish notifications directly.",
            target_peer_id="watch-node",
            target_device_classes=["light"],
        )
        self.assertEqual(notification["notification_type"], "job.summary")
        self.assertTrue(notification["presentation"]["compact"])

        approval = alpha.mesh.governance.create_approval_request(
            title="Approve governance seam",
            summary="Verify governance service approval storage.",
            action_type="operator_action",
            target_peer_id="watch-node",
        )
        self.assertEqual(approval["status"], "pending")
        listed = alpha.mesh.list_approvals(limit=10, target_peer_id="watch-node")
        self.assertEqual(listed["count"], 1)

    def test_mesh_governance_service_supports_treaties_and_validation(self):
        alpha = self.make_stack("alpha-governance-treaties")

        treaty = alpha.mesh.governance.propose_treaty(
            treaty_id="treaty/storage-v1",
            title="Storage Continuity Treaty",
            summary="Require witness-backed continuity custody.",
            treaty_type="continuity",
            parties=["alpha-node", "beta-node", "alpha-node"],
            document={
                "witness_required": True,
                "artifact_export": "sealed",
                "allowed_execution_classes": ["relay", "full", "relay"],
            },
        )

        self.assertEqual(treaty["id"], "treaty/storage-v1")
        self.assertEqual(treaty["status"], "active")
        self.assertEqual(treaty["parties"], ["alpha-node", "beta-node"])
        self.assertTrue(treaty["document"]["witness_required"])
        self.assertEqual(treaty["document"]["allowed_execution_classes"], ["relay", "full"])

        listed = alpha.mesh.governance.list_treaties(limit=10, status="active", treaty_type="continuity")
        self.assertEqual(listed["count"], 1)
        fetched = alpha.mesh.governance.get_treaty("treaty/storage-v1")
        self.assertEqual(fetched["title"], "Storage Continuity Treaty")

        validation = alpha.mesh.validate_treaty_requirements(
            ["treaty/storage-v1", "treaty/missing-v1"],
            operation="continuity_export",
        )
        self.assertFalse(validation["satisfied"])
        self.assertEqual(validation["missing"], ["treaty/missing-v1"])
        self.assertEqual(validation["matched"][0]["id"], "treaty/storage-v1")

        audit = alpha.mesh.audit_treaty_requirements(
            ["treaty/storage-v1", "treaty/missing-v1"],
            operation="continuity_restore",
        )
        self.assertEqual(audit["status"], "attention_needed")
        self.assertEqual(audit["validation"]["operation"], "continuity_restore")
        self.assertEqual(audit["posture"]["active_treaty_ids"], ["treaty/storage-v1"])
        self.assertIn("missing", audit["guidance"].lower())

    def test_remote_approval_request_and_resolution_over_http(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "micro",
                "execution_tier": "sensor",
                "power_profile": "battery",
                "network_profile": "intermittent",
                "mobility": "wearable",
                "form_factor": "watch",
                "approval_capable": True,
            },
        )
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        requested = alpha.mesh.request_approval_from_peer(
            "beta-node",
            {
                "request_id": "approval-watch-1",
                "title": "Approve recovery move",
                "summary": "Checkpointed work can resume on a stable relay peer.",
                "action_type": "job.recovery.resume",
                "severity": "high",
                "requested_by_peer_id": "alpha-node",
                "requested_by_agent_id": alpha.agent_id,
                "target_peer_id": "beta-node",
                "target_agent_id": beta.agent_id,
                "target_device_classes": ["micro"],
                "related_job_id": "job-recovery-1",
                "metadata": {"recommended_peer_id": "relay-node"},
            },
            client=beta_client,
        )

        self.assertEqual(requested["status"], "pending")
        self.assertEqual(requested["approval"]["target_peer_id"], "beta-node")
        self.assertEqual(requested["notification"]["notification_type"], "approval.request")

        inbox = beta.mesh.list_approvals(limit=10, status="pending", target_peer_id="beta-node")
        self.assertEqual(inbox["count"], 1)
        self.assertEqual(inbox["approvals"][0]["compact_summary"], "Checkpointed work can resume on a stable relay peer.")

        resolved = beta_client.resolve_approval(
            requested["approval"]["id"],
            decision="approved",
            operator_peer_id="beta-node",
            operator_agent_id=beta.agent_id,
            reason="approve on watch",
        )
        self.assertEqual(resolved["status"], "approved")
        self.assertEqual(resolved["approval"]["status"], "approved")
        resolution_notifications = beta.mesh.list_notifications(limit=10, target_peer_id="alpha-node")
        self.assertEqual(resolution_notifications["count"], 1)
        self.assertEqual(resolution_notifications["notifications"][0]["related_approval_id"], requested["approval"]["id"])

    def test_treaty_endpoints_are_exposed_over_http(self):
        alpha = self.make_stack("alpha-treaties-http")
        alpha_client, _ = self.serve_mesh(alpha)

        proposed = alpha_client.propose_treaty(
            {
                "treaty_id": "treaty/http-v1",
                "title": "HTTP Continuity Treaty",
                "summary": "Round-trip treaty APIs",
                "document": {
                    "witness_required": True,
                    "artifact_export": "sealed",
                    "allowed_execution_classes": ["full", "relay"],
                },
            }
        )

        self.assertEqual(proposed["status"], "ok")
        self.assertEqual(proposed["treaty"]["id"], "treaty/http-v1")
        listed = alpha_client.list_treaties(limit=10, status="active")
        self.assertEqual(listed["count"], 1)
        fetched = alpha_client.get_treaty("treaty/http-v1")
        self.assertEqual(fetched["document"]["artifact_export"], "sealed")
        audit = alpha_client.audit_treaty_requirements(
            {"treaty_requirements": ["treaty/http-v1"], "operation": "continuity_export"}
        )
        self.assertEqual(audit["status"], "ok")
        self.assertTrue(audit["validation"]["satisfied"])

    def test_scheduler_supports_required_device_class_and_artifact_mirror_policy(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "relay",
                "execution_tier": "control",
                "network_profile": "broadband",
                "artifact_mirror_capable": True,
                "accepts_remote_jobs": False,
                "form_factor": "relay",
            },
        )
        gamma = self.make_stack(
            "gamma",
            device_profile={
                "device_class": "full",
                "execution_tier": "standard",
                "network_profile": "wired",
                "artifact_mirror_capable": True,
                "form_factor": "server",
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        _, beta_base_url = self.serve_mesh(beta)
        _, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)
        alpha.mesh.sync_peer("gamma-node", limit=20, refresh_manifest=True)

        denied = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"required_device_classes": ["relay"], "require_artifact_mirror": True},
                "payload": {"command": [sys.executable, "-c", "print('relay-only')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )
        self.assertEqual(denied["status"], "unplaced")
        beta_candidate = next(candidate for candidate in denied["candidates"] if candidate["peer_id"] == "beta-node")
        self.assertIn("device_not_compute_ready", beta_candidate["reasons"])

        placed = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {
                    "required_device_classes": ["full"],
                    "preferred_device_classes": ["full"],
                    "require_artifact_mirror": True,
                    "require_stable_network": True,
                },
                "payload": {"command": [sys.executable, "-c", "print('full-only')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )
        self.assertEqual(placed["status"], "placed")
        self.assertEqual(placed["selected"]["peer_id"], "gamma-node")
        self.assertIn("preferred_device_class", placed["selected"]["reasons"])

    def test_scheduler_trust_floor_and_preferred_trust_tier_shape_remote_selection(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        gamma = self.make_stack("gamma")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        _, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="partner")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="trusted")

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {
                    "trust_floor": "partner",
                    "preferred_trust_tiers": ["trusted"],
                    "queue_class": "batch",
                },
                "payload": {"command": [sys.executable, "-c", "print('trust-route')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "gamma-node")
        gamma_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "gamma-node")
        beta_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "beta-node")
        self.assertIn("preferred_trust_tier", gamma_candidate["reasons"])
        self.assertNotIn("trust_floor_denied", beta_candidate["reasons"])

        strict = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"trust_floor": "trusted"},
                "payload": {"command": [sys.executable, "-c", "print('strict-trust')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )
        beta_candidate = next(candidate for candidate in strict["candidates"] if candidate["peer_id"] == "beta-node")
        self.assertIn("trust_floor_denied", beta_candidate["reasons"])
        self.assertEqual(strict["selected"]["peer_id"], "gamma-node")

    def test_scheduler_continuity_durable_prefers_storage_capable_peer(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "full",
                "execution_tier": "standard",
                "network_profile": "wired",
                "artifact_mirror_capable": True,
                "secure_secret_capable": True,
                "form_factor": "server",
            },
        )
        gamma = self.make_stack(
            "gamma",
            device_profile={
                "device_class": "full",
                "execution_tier": "standard",
                "network_profile": "wired",
                "artifact_mirror_capable": False,
                "secure_secret_capable": False,
                "form_factor": "server",
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        _, beta_base_url = self.serve_mesh(beta)
        _, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)
        alpha.mesh.sync_peer("gamma-node", limit=20, refresh_manifest=True)

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "continuity": {
                    "continuity_class": "durable",
                    "lineage_ref": "lineage/test-alpha",
                    "treaty_requirements": ["treaty/storage-v1"],
                },
                "payload": {"command": [sys.executable, "-c", "print('continuity durable')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )

        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "beta-node")
        self.assertIn("continuity_storage_preferred", decision["selected"]["reasons"])
        self.assertIn("lineage_ref_present", decision["selected"]["reasons"])
        self.assertTrue(decision["selected"]["continuity_alignment"]["active"])

    def test_scheduler_continuity_recovery_hint_prefers_requested_device_class(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack(
            "beta",
            device_profile={
                "device_class": "light",
                "execution_tier": "light",
                "power_profile": "battery",
                "network_profile": "intermittent",
                "mobility": "mobile",
                "form_factor": "phone",
                "artifact_mirror_capable": True,
            },
        )
        gamma = self.make_stack(
            "gamma",
            device_profile={
                "device_class": "full",
                "execution_tier": "standard",
                "network_profile": "wired",
                "form_factor": "server",
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        _, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)
        alpha.mesh.sync_peer("gamma-node", limit=20, refresh_manifest=True)

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"], "resources": {"cpu": 1}},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "payload": {"command": [sys.executable, "-c", "print('continuity class')"]},
                "artifact_inputs": [],
                "metadata": {
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "on_retry": True},
                    "recovery_hint": {
                        "preferred_target_device_classes": ["light"],
                    },
                },
            },
            allow_local=False,
        )

        self.assertEqual(decision["status"], "placed")
        beta_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "beta-node")
        self.assertIn("continuity_preferred_device_class", beta_candidate["reasons"])
        self.assertEqual(
            beta_candidate["continuity_alignment"]["preferred_target_device_classes"],
            ["light"],
        )
        gamma_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "gamma-node")
        self.assertIn("continuity_device_class_miss", gamma_candidate["reasons"])

    def test_scheduler_local_backlog_limit_routes_to_remote_capacity(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
            max_concurrent_jobs=1,
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        for idx in range(2):
            alpha.mesh.submit_local_job(
                {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", f"print('local-backlog-{idx}')"]},
                    "artifact_inputs": [],
                },
                request_id=f"local-backlog-{idx}",
            )

        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)
        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"max_local_queue_depth": 0, "prefer_low_backlog": True},
                "payload": {"command": [sys.executable, "-c", "print('reroute-remote')"]},
                "artifact_inputs": [],
            }
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "beta-node")
        local_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "alpha-node")
        self.assertIn("local_backlog_limit_exceeded", local_candidate["reasons"])

    def test_scheduler_throughput_prefers_remote_peer_with_lower_backlog(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        gamma = self.make_stack("gamma")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
            max_concurrent_jobs=1,
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
            max_concurrent_jobs=2,
        )
        for idx in range(3):
            beta.mesh.submit_local_job(
                {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", f"print('beta-queued-{idx}')"]},
                    "artifact_inputs": [],
                },
                request_id=f"beta-queued-{idx}",
            )

        _, beta_base_url = self.serve_mesh(beta)
        _, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)
        alpha.mesh.sync_peer("gamma-node", limit=20, refresh_manifest=True)

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"execution_class": "throughput", "prefer_low_backlog": True},
                "payload": {"command": [sys.executable, "-c", "print('throughput-route')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "gamma-node")
        beta_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "beta-node")
        gamma_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "gamma-node")
        self.assertIn("low_backlog_preferred", gamma_candidate["reasons"])
        self.assertIn("execution_class_throughput", gamma_candidate["reasons"])
        self.assertIn("remote_queue_depth=3", beta_candidate["reasons"])

    def test_scheduler_isolation_class_prefers_trusted_remote_over_local_and_public(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        gamma = self.make_stack("gamma")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        _, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="public")

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "public", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"execution_class": "isolation"},
                "payload": {"command": [sys.executable, "-c", "print('isolated-route')"]},
                "artifact_inputs": [],
            }
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "beta-node")
        local_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "alpha-node")
        gamma_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "gamma-node")
        self.assertIn("execution_class_isolation_local_penalty", local_candidate["reasons"])
        self.assertIn("execution_class_isolation_public_penalty", gamma_candidate["reasons"])

    def test_scheduler_decisions_are_persisted_with_placement_metadata(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )

        scheduled = alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "latency_sensitive", "stay_local": True},
                "payload": {"command": [sys.executable, "-c", "print('decision-log')"]},
                "artifact_inputs": [],
            },
            request_id="decision-log-1",
        )
        decisions = alpha.mesh.list_scheduler_decisions(limit=10)

        self.assertEqual(decisions["count"], 1)
        decision = decisions["decisions"][0]
        self.assertEqual(decision["request_id"], "decision-log-1")
        self.assertEqual(decision["job_id"], scheduled["job"]["id"])
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["placement"]["queue_class"], "latency_sensitive")
        self.assertEqual(decision["placement"]["stay_local"], True)
        self.assertEqual(decision["selected"]["peer_id"], "alpha-node")

    def test_scheduler_decisions_endpoint_lists_unplaced_and_placed_rows(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        alpha_client, _ = self.serve_mesh(alpha)
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        with self.assertRaises(MeshPolicyError):
            alpha.mesh.schedule_job(
                {
                    "kind": "shell.command",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "dispatch_mode": "queued",
                    "placement": {"stay_local": True},
                    "payload": {"command": [sys.executable, "-c", "print('blocked')"]},
                    "artifact_inputs": [],
                },
                request_id="decision-log-unplaced",
            )

        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch", "preferred_peer_ids": ["beta-node"]},
                "payload": {"command": [sys.executable, "-c", "print('placed')"]},
                "artifact_inputs": [],
            },
            request_id="decision-log-placed",
        )

        over_http = alpha_client.list_scheduler_decisions(limit=10)
        self.assertGreaterEqual(over_http["count"], 2)
        statuses = {item["request_id"]: item["status"] for item in over_http["decisions"]}
        self.assertEqual(statuses["decision-log-unplaced"], "unplaced")
        self.assertEqual(statuses["decision-log-placed"], "placed")

        only_unplaced = alpha_client.list_scheduler_decisions(limit=10, status="unplaced")
        self.assertEqual(only_unplaced["count"], 1)
        self.assertEqual(only_unplaced["decisions"][0]["request_id"], "decision-log-unplaced")

    def test_scheduler_prefers_peer_with_better_reliability_history(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        gamma = self.make_stack("gamma")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        gamma.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gamma.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        beta_client, beta_base_url = self.serve_mesh(beta)
        gamma_client, gamma_base_url = self.serve_mesh(gamma)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="trusted")

        beta_job = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "payload": {"command": [sys.executable, "-c", "print('beta-ok')"]},
                "artifact_inputs": [],
            },
            request_id="beta-reliability-ok",
        )
        self.assertEqual(beta_job["status"], "queued")
        self.assertEqual(beta.mesh.run_worker_once("beta-worker")["status"], "completed")

        gamma_job = gamma.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "payload": {"command": [sys.executable, "-c", "import sys; sys.exit(1)"]},
                "artifact_inputs": [],
                "metadata": {"retry_policy": {"max_attempts": 1}},
            },
            request_id="gamma-reliability-fail",
        )
        self.assertEqual(gamma_job["status"], "queued")
        self.assertEqual(gamma.mesh.run_worker_once("gamma-worker")["status"], "failed")

        alpha.mesh.sync_peer("beta-node", client=beta_client, limit=50)
        alpha.mesh.sync_peer("gamma-node", client=gamma_client, limit=50)

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('reliability-route')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "beta-node")
        beta_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "beta-node")
        gamma_candidate = next(candidate for candidate in decision["candidates"] if candidate["peer_id"] == "gamma-node")
        self.assertIn("reliability_bonus", beta_candidate["reasons"])
        self.assertIn("reliability_penalty", gamma_candidate["reasons"])

    def test_scheduler_scores_route_health_and_artifact_checkpoint_locality(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self._register_default_worker(beta, worker_id="beta-locality-worker")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh._update_peer_record(
            "beta-node",
            metadata={
                "route_health": {"status": "reachable", "best_route": beta_base_url, "checked_at": "2026-01-01T00:00:00Z"},
                "artifact_inventory": {"digests": ["sha256:input"]},
                "checkpoint_inventory": {"artifact_ids": ["checkpoint-artifact"]},
            },
        )
        peer = alpha.mesh.list_peers(limit=10)["peers"][0]
        score, reasons, _ = alpha.mesh.scheduler.peer_candidate_score(
            peer,
            {
                "kind": "python.inline",
                "dispatch_mode": "inline",
                "requirements": {},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('locality')"},
                "artifact_inputs": [{"artifact_id": "input-artifact", "digest": "sha256:input"}],
                "metadata": {"resume_checkpoint_ref": {"artifact_id": "checkpoint-artifact"}},
            },
        )

        self.assertGreater(score, 0)
        self.assertIn("route_last_reachable", reasons)
        self.assertIn("route_probe_reachable", reasons)
        self.assertIn("artifact_locality_match=1", reasons)
        self.assertIn("checkpoint_locality_match=1", reasons)

    def test_list_peers_exposes_remote_reliability_summary(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )

        job = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "payload": {"command": [sys.executable, "-c", "print('peer-ok')"]},
                "artifact_inputs": [],
            },
            request_id="peer-reliability-visible",
        )
        self.assertEqual(job["status"], "queued")
        self.assertEqual(beta.mesh.run_worker_once("beta-worker")["status"], "completed")

        synced = alpha.mesh.sync_peer("beta-node", client=beta_client, limit=50)
        self.assertEqual(synced["status"], "ok")

        peer = alpha.mesh.list_peers(limit=10)["peers"][0]
        self.assertEqual(peer["peer_id"], "beta-node")
        self.assertEqual(peer["reliability"]["source"], "remote_events")
        self.assertEqual(peer["reliability"]["completed"], 1)
        self.assertEqual(peer["reliability"]["failed"], 0)

    def test_connect_peer_over_http_registers_remote_peer_and_logs_sender_event(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, base_url = self.serve_mesh(beta)

        response = alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["peer"]["peer_id"], "beta-node")
        self.assertEqual(response["peer_advisory"]["peer_id"], "beta-node")
        self.assertIn("treaty_compatibility", response["peer_advisory"])
        self.assertIn("treaty posture", response["operator_summary"])
        self.assertIn("recommended_action", response["peer_advisory"])
        self.assertIn("peer_advisory", response["response"])
        alpha_peers = alpha.mesh.list_peers(limit=10)["peers"]
        beta_peers = beta.mesh.list_peers(limit=10)["peers"]
        self.assertEqual(alpha_peers[0]["peer_id"], "beta-node")
        self.assertEqual(beta_peers[0]["peer_id"], "alpha-node")
        events = alpha.mesh.stream_snapshot(limit=20)["events"]
        event_types = [event["event_type"] for event in events]
        self.assertIn("mesh.handshake.sent", event_types)
        sent_event = next(event for event in events if event["event_type"] == "mesh.handshake.sent")
        self.assertEqual(sent_event["payload"]["peer_advisory"]["peer_id"], "beta-node")
        self.assertIn("operator_summary", sent_event["payload"]["peer_advisory"])
        self.assertIn("accepted_peer_advisory", sent_event["payload"])
        control_payload = server.build_control_stream_payload(alpha.mesh, since_seq=0, limit=20)
        connected_advisories = control_payload["peer_advisories"]["connected"]
        self.assertTrue(any(item["peer_id"] == "beta-node" for item in connected_advisories))
        self.assertIn("recommended_action", connected_advisories[0])

    def test_sync_peer_imports_remote_events_updates_cursor_and_heartbeat(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        beta.mesh._record_event(
            "mesh.synthetic.remote",
            peer_id="beta-node",
            payload={"shape": "heartbeat-check"},
        )

        synced = alpha.mesh.sync_peer("beta-node", client=beta_client, limit=50)
        self.assertEqual(synced["status"], "ok")
        self.assertGreaterEqual(synced["imported_events"], 1)
        self.assertGreater(synced["next_cursor"], 0)
        self.assertEqual(synced["heartbeat"]["status"], "active")
        self.assertEqual(synced["peer_advisory"]["peer_id"], "beta-node")
        self.assertIn("treaty_compatibility", synced["peer_advisory"])
        self.assertIn("treaty posture", synced["operator_summary"])

        remote_events = alpha.mesh.list_remote_events("beta-node", limit=20)
        self.assertIn("mesh.synthetic.remote", {event["event_type"] for event in remote_events})
        peer = alpha.mesh.list_peers(limit=10)["peers"][0]
        self.assertEqual(peer["sync_state"]["remote_cursor"], synced["next_cursor"])
        self.assertEqual(peer["heartbeat"]["status"], "active")
        stream = alpha.mesh.stream_snapshot(limit=20)
        synced_event = next(event for event in stream["events"] if event["event_type"] == "mesh.peer.synced")
        heartbeat_event = next(event for event in stream["events"] if event["event_type"] == "mesh.peer.heartbeat")
        self.assertEqual(synced_event["payload"]["peer_advisory"]["peer_id"], "beta-node")
        self.assertEqual(heartbeat_event["payload"]["peer_advisory"]["peer_id"], "beta-node")
        self.assertIn("recommended_action", synced_event["payload"]["peer_advisory"])

        resynced = alpha.mesh.sync_peer("beta-node", client=beta_client, limit=50)
        self.assertEqual(resynced["imported_events"], 0)

    def test_outbound_http_job_handoff_and_artifact_calls_log_sender_events(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")

        job_response = alpha.mesh.dispatch_job_to_peer(
            "beta-node",
            {
                "kind": "agent.echo",
                "origin": "alpha-node",
                "target": "beta-node",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"message": "networked"},
                "artifact_inputs": [],
            },
        )
        self.assertEqual(job_response["status"], "completed")

        artifact_response = alpha.mesh.publish_artifact_to_peer(
            "beta-node",
            {
                "content": "artifact payload",
                "media_type": "text/plain",
                "policy": {"classification": "public", "mode": "batch"},
                "digest": hashlib.sha256(b"artifact payload").hexdigest(),
            },
        )
        self.assertEqual(artifact_response["status"], "published")

        handoff_response = alpha.mesh.handoff_to_peer(
            "beta-node",
            {
                "to_peer_id": "beta-node",
                "from_agent": alpha.agent_id,
                "to_agent": beta.agent_id,
                "summary": "Follow the outbound federation path",
                "intent": "Continue with remote organism work",
                "constraints": {"project_id": "mesh-http"},
                "artifact_refs": [],
            },
        )
        self.assertEqual(handoff_response["status"], "accepted")

        event_types = [event["event_type"] for event in alpha.mesh.stream_snapshot(limit=40)["events"]]
        self.assertIn("mesh.job.sent", event_types)
        self.assertIn("mesh.artifact.sent", event_types)
        self.assertIn("mesh.handoff.sent", event_types)

    def test_dispatch_job_to_peer_uses_extended_timeout_for_inline_runtime_jobs(self):
        alpha = self.make_stack("alpha")
        captured = {}

        class StubClient:
            def submit_job(self, envelope):
                captured["envelope"] = envelope
                return {"status": "queued", "job": {"id": "remote-job-1"}}

        original_resolve = alpha.mesh._resolve_peer_client

        def fake_resolve(peer_id, **kwargs):
            captured["peer_id"] = peer_id
            captured["timeout"] = kwargs.get("timeout")
            return StubClient(), {"peer_id": peer_id, "endpoint_url": "http://beta.example"}

        alpha.mesh._resolve_peer_client = fake_resolve
        try:
            response = alpha.mesh.dispatch_job_to_peer(
                "beta-node",
                {
                    "kind": "python.inline",
                    "dispatch_mode": "inline",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"code": "print('remote proof')"},
                    "artifact_inputs": [],
                },
                request_id="extended-timeout-test",
            )
        finally:
            alpha.mesh._resolve_peer_client = original_resolve

        self.assertEqual(response["status"], "queued")
        self.assertEqual(captured["peer_id"], "beta-node")
        self.assertGreaterEqual(float(captured["timeout"] or 0), 30.0)
        self.assertEqual(captured["envelope"]["request"]["request_id"], "extended-timeout-test")

    def test_server_mesh_peers_sync_handler_runs_single_peer_sync(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=base_url, trust_tier="trusted")
        beta.mesh._record_event("mesh.synthetic.remote", peer_id="beta-node", payload={"kind": "server-handler"})

        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()
        original_resolve = alpha.mesh._resolve_peer_client
        alpha.mesh._resolve_peer_client = lambda peer_id, **kwargs: original_resolve(peer_id, client=beta_client)
        try:
            probe._handle_mesh_peers_sync({"peer_id": "beta-node", "limit": 20})
        finally:
            alpha.mesh._resolve_peer_client = original_resolve

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["status"], "ok")
        self.assertGreaterEqual(probe.payload["imported_events"], 1)

    def test_server_mesh_manifest_handler_returns_manifest(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.propose_treaty(
            treaty_id="treaty/server-v1",
            title="Server Treaty",
            document={"witness_required": True},
        )
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_manifest()

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["protocol"], "Open Compute Protocol")
        self.assertEqual(probe.payload["protocol_short_name"], "OCP")
        self.assertEqual(probe.payload["protocol_release"], "0.1")
        self.assertEqual(probe.payload["implementation"]["name"], "Sovereign Mesh")
        self.assertEqual(probe.payload["organism_card"]["organism_id"], "alpha-node")
        self.assertIn("foundry", probe.payload["habitat_roles"])
        self.assertTrue(probe.payload["continuity_capabilities"]["mission_continuity"])
        self.assertTrue(probe.payload["organism_card"]["continuity_capabilities"]["vessel_export"])
        self.assertTrue(probe.payload["treaty_capabilities"]["treaty_documents"])
        self.assertEqual(probe.payload["governance_summary"]["active_treaty_ids"], ["treaty/server-v1"])

    def test_server_control_page_handler_returns_mobile_html(self):
        alpha = self.make_stack("alpha")
        self._register_default_worker(alpha, worker_id="alpha-control-worker")
        alpha.mesh.submit_local_job(
            {
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "payload": {"code": "print('queued from control deck')"},
            },
            request_id="control-queued-job",
        )
        self._checkpointed_job(alpha, request_id="control-checkpointed-job")
        alpha.mesh.publish_notification(
            notification_type="job.summary",
            priority="high",
            title="Watch review needed",
            body="Compact review body",
            target_peer_id="alpha-node",
            target_device_classes=["micro"],
        )
        alpha.mesh.create_approval_request(
            title="Approve resume",
            summary="Resume on stable relay",
            action_type="job.recovery.resume",
            severity="high",
            target_peer_id="alpha-node",
            target_device_classes=["micro"],
        )
        alpha.mesh.launch_mission(
            title="Probe Control Mission",
            intent="Expose mission layer in rendered control HTML",
            request_id="control-probe-mission",
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('probe control mission')"},
            },
        )
        checkpointed_mission = self._checkpointed_mission(alpha, worker_id="alpha-control-worker", request_id="control-mission-recovery")
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_control_page()

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.content_type, "text/html; charset=utf-8")
        self.assertIn("OCP Control Deck", probe.payload)
        self.assertIn("Watch review needed", probe.payload)
        self.assertIn("Approve resume", probe.payload)
        self.assertIn("Mesh Pulse", probe.payload)
        self.assertIn("Connect Devices", probe.payload)
        self.assertIn("Scan Nearby", probe.payload)
        self.assertIn("Connect Everything", probe.payload)
        self.assertIn("Test Whole Mesh", probe.payload)
        self.assertIn("Send Test Mission", probe.payload)
        self.assertIn("Autonomic Mesh", probe.payload)
        self.assertIn("Activate Autonomic Mesh", probe.payload)
        self.assertIn("/mesh/autonomy/status", probe.payload)
        self.assertIn("Live Mission Stream", probe.payload)
        self.assertIn("Operator Inspect", probe.payload)
        self.assertIn("Recovery + Queue", probe.payload)
        self.assertIn("Mission Layer", probe.payload)
        self.assertIn("Probe Control Mission", probe.payload)
        self.assertIn("Inspect Mission", probe.payload)
        self.assertIn("Inspect Job", probe.payload)
        self.assertIn("Inspect Task", probe.payload)
        self.assertIn("Resume Latest", probe.payload)
        self.assertIn("Resume Checkpoint", probe.payload)
        self.assertIn("Restart Mission", probe.payload)
        self.assertIn("Treaty Posture", probe.payload)
        self.assertIn("custody", probe.payload.lower())
        self.assertIn(checkpointed_mission["mission"]["id"], probe.payload)
        self.assertIn("Cancel Job", probe.payload)
        self.assertIn("/mesh/control/stream", probe.payload)
        self.assertIn("/mesh/notifications", probe.payload)

    def test_server_app_page_handler_returns_unified_phone_shell(self):
        alpha = self.make_stack("alpha")
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_app_page()

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.content_type, "text/html; charset=utf-8")
        self.assertIn("OCP App", probe.payload)
        self.assertIn("One app for the mesh.", probe.payload)
        self.assertIn("OCP Easy Setup", probe.payload)
        self.assertIn("OCP Control Deck", probe.payload)
        self.assertIn("/easy", probe.payload)
        self.assertIn("/control", probe.payload)
        self.assertIn("/mesh/contract", probe.payload)
        self.assertIn("/app.webmanifest", probe.payload)

        manifest_probe = ProbeHandler()
        manifest_probe._handle_app_manifest()

        self.assertEqual(manifest_probe.code, 200)
        self.assertEqual(manifest_probe.content_type, "application/manifest+json")
        self.assertEqual(manifest_probe.payload["short_name"], "OCP")
        self.assertEqual(manifest_probe.payload["start_url"], "/app")
        self.assertEqual(manifest_probe.payload["display"], "standalone")

    def test_raw_mesh_post_routes_require_operator_auth_off_loopback(self):
        alpha = self.make_stack("alpha")
        payload = {"device_profile": {"form_factor": "phone"}}

        probe = ProbeHandler()
        probe.server = SimpleNamespace(mesh=alpha.mesh)
        probe.client_address = ("198.51.100.10", 4444)
        probe.headers = {}
        self.assertTrue(probe._dispatch_post_request("/mesh/device-profile", payload))
        self.assertEqual(probe.code, 401)
        self.assertEqual(probe.payload["error"], "operator authorization required")

        signed_probe = ProbeHandler()
        signed_probe.server = SimpleNamespace(mesh=alpha.mesh)
        signed_probe.client_address = ("198.51.100.10", 4444)
        signed_probe.headers = {}
        self.assertTrue(signed_probe._dispatch_post_request("/mesh/handshake", {}))
        self.assertEqual(signed_probe.code, 400)
        self.assertEqual(signed_probe.payload["error"], "protocol validation failed")

        with mock.patch.dict(os.environ, {"OCP_OPERATOR_TOKEN": "secret-token"}, clear=False):
            authorized = ProbeHandler()
            authorized.server = SimpleNamespace(mesh=alpha.mesh)
            authorized.client_address = ("198.51.100.10", 4444)
            authorized.headers = {"Authorization": "Bearer secret-token"}
            self.assertTrue(authorized._dispatch_post_request("/mesh/device-profile", payload))
            self.assertEqual(authorized.code, 200)
            self.assertEqual(authorized.payload["status"], "ok")

    def test_artifact_http_content_requires_operator_auth_or_public_policy(self):
        beta = self.make_stack("beta")
        private_artifact = beta.mesh.publish_local_artifact(
            {"kind": "private-artifact"},
            media_type="application/json",
            policy={"classification": "trusted", "mode": "batch"},
            metadata={"artifact_kind": "bundle"},
        )
        artifact_path = f"/mesh/artifacts/{private_artifact['id']}"

        spoofed = ProbeHandler()
        spoofed.server = SimpleNamespace(mesh=beta.mesh)
        spoofed.client_address = ("198.51.100.10", 4444)
        spoofed.headers = {}
        self.assertTrue(spoofed._dispatch_get_request(artifact_path, {"peer_id": ["beta-node"]}))
        self.assertEqual(spoofed.code, 401)
        self.assertEqual(spoofed.payload["error"], "operator authorization required")

        metadata_only = ProbeHandler()
        metadata_only.server = SimpleNamespace(mesh=beta.mesh)
        metadata_only.client_address = ("198.51.100.10", 4444)
        metadata_only.headers = {}
        self.assertTrue(metadata_only._dispatch_get_request(artifact_path, {"include_content": ["0"]}))
        self.assertEqual(metadata_only.code, 200)
        self.assertNotIn("content_base64", metadata_only.payload)

        with mock.patch.dict(os.environ, {"OCP_OPERATOR_TOKEN": "secret-token"}, clear=False):
            authorized = ProbeHandler()
            authorized.server = SimpleNamespace(mesh=beta.mesh)
            authorized.client_address = ("198.51.100.10", 4444)
            authorized.headers = {"X-OCP-Operator-Token": "secret-token"}
            self.assertTrue(authorized._dispatch_get_request(artifact_path, {}))
            self.assertEqual(authorized.code, 200)
            self.assertIn("content_base64", authorized.payload)

        public_artifact = beta.mesh.publish_local_artifact(
            {"kind": "public-artifact"},
            media_type="application/json",
            policy={"classification": "public", "mode": "batch"},
            metadata={"artifact_kind": "bundle"},
        )
        public_probe = ProbeHandler()
        public_probe.server = SimpleNamespace(mesh=beta.mesh)
        public_probe.client_address = ("198.51.100.10", 4444)
        public_probe.headers = {}
        self.assertTrue(public_probe._dispatch_get_request(f"/mesh/artifacts/{public_artifact['id']}", {}))
        self.assertEqual(public_probe.code, 200)
        self.assertIn("content_base64", public_probe.payload)

    def test_server_easy_page_handler_returns_human_friendly_html(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        server.server_context["mesh"] = alpha.mesh
        alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="trusted")
        probe = ProbeHandler()

        probe._handle_easy_page()

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.content_type, "text/html; charset=utf-8")
        self.assertIn("OCP Easy Setup", probe.payload)
        self.assertIn("Connect two computers without becoming the network department.", probe.payload)
        self.assertIn("Nearby Computers", probe.payload)
        self.assertIn("Scan Nearby", probe.payload)
        self.assertIn("Connect Everything", probe.payload)
        self.assertIn("Test Whole Mesh", probe.payload)
        self.assertIn("Send Test Mission", probe.payload)
        self.assertIn("Copy My Easy Link", probe.payload)
        self.assertIn("Share This Easy Link", probe.payload)
        self.assertIn("Scan This QR Code", probe.payload)
        self.assertIn("qrcode.min.js", probe.payload)
        self.assertIn("Open Advanced Deck", probe.payload)
        self.assertIn("beta-node", probe.payload)
        self.assertIn("TREATY AWARE", probe.payload)
        self.assertIn("127.0.0.1", probe.payload)
        self.assertIn("share_url", probe.payload)
        self.assertIn("sharing_mode", probe.payload)

    def test_server_mesh_device_profile_handlers_round_trip_profile(self):
        alpha = self.make_stack("alpha")
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_device_profile_update(
            {
                "device_profile": {
                    "device_class": "light",
                    "execution_tier": "light",
                    "power_profile": "battery",
                    "network_profile": "wifi",
                    "mobility": "mobile",
                    "form_factor": "phone",
                }
            }
        )
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["device_profile"]["device_class"], "light")
        self.assertEqual(probe.payload["device_profile"]["form_factor"], "phone")

        probe = ProbeHandler()
        probe._handle_mesh_device_profile()
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["device_profile"]["network_profile"], "wifi")
        self.assertTrue(probe.payload["device_profile"]["battery_powered"])

    def test_server_discovery_handlers_round_trip(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_discovery_seek({"base_urls": [beta_base_url], "auto_connect": True, "trust_tier": "trusted"})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["connected"], 1)

        probe = ProbeHandler()
        probe._handle_mesh_discovery_candidates({"limit": ["10"], "status": [""]})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)
        self.assertEqual(probe.payload["candidates"][0]["peer_id"], "beta-node")

    def test_server_connect_surface_handlers_round_trip(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        server.server_context["mesh"] = alpha.mesh

        original = alpha.mesh.suggest_local_scan_urls
        alpha.mesh.suggest_local_scan_urls = lambda **_: [beta_base_url]
        try:
            probe = ProbeHandler()
            probe._handle_mesh_discovery_scan_local({"timeout": 0.5, "limit": 12})
            self.assertEqual(probe.code, 200)
            self.assertEqual(probe.payload["discovered"], 1)
            self.assertEqual(probe.payload["results"][0]["peer_id"], "beta-node")
        finally:
            alpha.mesh.suggest_local_scan_urls = original

        probe = ProbeHandler()
        probe._handle_mesh_peers_connect({"base_url": beta_base_url, "trust_tier": "trusted"})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["status"], "ok")
        self.assertEqual(probe.payload["peer"]["peer_id"], "beta-node")

        original = alpha.mesh.suggest_local_scan_urls
        alpha.mesh.suggest_local_scan_urls = lambda **_: [beta_base_url]
        try:
            probe = ProbeHandler()
            probe._handle_mesh_peers_connect_all({"trust_tier": "trusted", "limit": 12})
            self.assertEqual(probe.code, 200)
            self.assertEqual(probe.payload["status"], "ok")
            self.assertGreaterEqual(probe.payload["already_connected"] + probe.payload["connected"], 1)
            self.assertIn("mesh", probe.payload)
        finally:
            alpha.mesh.suggest_local_scan_urls = original

        probe = ProbeHandler()
        probe._handle_mesh_connectivity_diagnostics()
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["status"], "ok")
        self.assertIn("local_ipv4", probe.payload)
        self.assertIn("lan_urls", probe.payload)
        self.assertIn("scan_urls", probe.payload)
        self.assertIn("share_url", probe.payload)
        self.assertIn("sharing_mode", probe.payload)
        self.assertIn("share_advice", probe.payload)

        probe = ProbeHandler()
        probe._handle_mesh_mission_test_launch({"peer_id": "beta-node"})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["status"], "ok")
        self.assertEqual(probe.payload["peer_id"], "beta-node")
        self.assertEqual(probe.payload["mission"]["summary"]["cooperative_task_count"], 1)
        self.assertEqual(probe.payload["mission"]["summary"]["job_count"], 1)

        probe = ProbeHandler()
        probe._handle_mesh_mission_test_mesh_launch({"include_local": True, "limit": 12})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["status"], "ok")
        self.assertIn("mesh", probe.payload)
        self.assertGreaterEqual(probe.payload["mesh"]["peer_count"], 2)
        self.assertEqual(probe.payload["mission"]["summary"]["cooperative_task_count"], 1)
        self.assertEqual(probe.payload["mission"]["summary"]["job_count"], 2)

    def test_autonomic_route_candidates_prefer_proven_routes_before_advertised_endpoint(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_card = dict(beta.mesh.get_manifest()["organism_card"])
        beta_card["endpoint_url"] = "http://advertised.invalid:8421"
        peer = alpha.mesh.remember_peer_card(
            beta_card,
            trust_tier="trusted",
            status="connected",
            metadata={
                "last_reachable_base_url": "http://last-good.local:8421",
                "route_candidates": [{"base_url": "http://history.local:8421", "source": "history", "status": "reachable"}],
            },
        )
        alpha.mesh._remember_discovery_candidate(
            base_url="http://discovery.local:8421",
            peer_id="beta-node",
            display_name="Beta",
            endpoint_url="http://discovery.local:8421",
            status="discovered",
            trust_tier="trusted",
        )

        candidates = alpha.mesh.autonomy.route_candidates_for_peer(peer, base_url="http://explicit.local:8421")

        self.assertEqual([item["source"] for item in candidates[:5]], ["explicit", "last_reachable", "history", "discovery", "advertised"])
        self.assertEqual(candidates[0]["base_url"], "http://explicit.local:8421")
        self.assertEqual(candidates[-1]["base_url"], "http://advertised.invalid:8421")

    def test_route_probe_records_last_reachable_route_health(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="trusted")

        probe = alpha.mesh.probe_routes(peer_id="beta-node", base_url=beta_base_url, timeout=2.0)

        self.assertEqual(probe["status"], "ok")
        self.assertEqual(probe["best_route"], beta_base_url)
        peer = alpha.mesh.list_peers(limit=10)["peers"][0]
        metadata = peer["metadata"]
        self.assertEqual(metadata["last_reachable_base_url"], beta_base_url)
        self.assertEqual(metadata["route_health"]["status"], "reachable")
        self.assertEqual(metadata["route_candidates"][0]["base_url"], beta_base_url)
        self.assertEqual(metadata["route_candidates"][0]["freshness"], "fresh")

    def test_route_probe_failure_records_operator_hint_and_backoff(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_card = dict(beta.mesh.get_manifest()["organism_card"])
        beta_card["endpoint_url"] = "http://127.0.0.1:1"
        alpha.mesh.remember_peer_card(beta_card, trust_tier="trusted", status="connected")

        probe = alpha.mesh.probe_routes(peer_id="beta-node", timeout=0.2, limit=1)

        self.assertEqual(probe["status"], "attention_needed")
        self.assertEqual(probe["reachable"], 0)
        self.assertIn("not listening", probe["operator_hint"])
        candidate = probe["candidates"][0]
        self.assertEqual(candidate["failure_count"], 1)
        self.assertTrue(candidate["next_probe_after"])
        self.assertEqual(candidate["freshness"], "failed")
        route = alpha.mesh.routes_health(limit=10)["routes"][0]
        self.assertEqual(route["failure_count"], 1)
        self.assertEqual(route["freshness"], "failed")
        self.assertIn("not listening", route["operator_hint"])

    def test_route_health_marks_stale_reachable_proofs_without_forgetting_route(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh._update_peer_record(
            "beta-node",
            metadata={
                "last_reachable_base_url": beta_base_url,
                "route_health": {
                    "status": "reachable",
                    "best_route": beta_base_url,
                    "checked_at": "2026-01-01T00:00:00Z",
                    "last_success_at": "2026-01-01T00:00:00Z",
                },
            },
        )

        route = alpha.mesh.routes_health(limit=10)["routes"][0]

        self.assertEqual(route["status"], "reachable")
        self.assertEqual(route["freshness"], "stale")
        self.assertEqual(route["best_route"], beta_base_url)
        self.assertIn("stale", route["operator_summary"])
        health = alpha.mesh.routes_health(limit=10)
        self.assertEqual(health["healthy"], 0)
        self.assertNotIn("strong", alpha.mesh.autonomy_status()["operator_summary"].lower())

    def test_dispatch_uses_proven_route_when_advertised_endpoint_is_poisoned(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="trusted")
        with alpha.mesh._conn() as conn:
            conn.execute("UPDATE mesh_peers SET endpoint_url=? WHERE peer_id=?", ("http://127.0.0.1:1", "beta-node"))
            conn.commit()

        result = alpha.mesh.launch_test_mission(peer_id="beta-node")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["mission"]["summary"]["cooperative_task_count"], 1)
        self.assertEqual(result["mission"]["summary"]["job_count"], 1)

    def test_autonomic_activation_connects_probes_enlists_and_runs_proof(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        original = alpha.mesh.suggest_local_scan_urls
        alpha.mesh.suggest_local_scan_urls = lambda **_: [beta_base_url]
        try:
            result = alpha.mesh.activate_autonomic_mesh(
                limit=12,
                scan_timeout=0.1,
                timeout=2.0,
                max_enlist=1,
                run_proof=True,
                request_id="autonomic-activation-test",
            )
        finally:
            alpha.mesh.suggest_local_scan_urls = original

        self.assertIn(result["status"], {"completed", "partial"})
        self.assertGreaterEqual(result["routes"]["healthy"], 1)
        self.assertTrue(any(action["kind"] == "route_probe" for action in result["actions"]))
        self.assertTrue(any(action["kind"] == "whole_mesh_proof" for action in result["actions"]))
        self.assertTrue(result["helpers"]["enlisted"])
        self.assertEqual(alpha.mesh.autonomy.latest_run()["request_id"], "autonomic-activation-test")

    def test_autonomic_activation_reuses_duplicate_request_id(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        original = alpha.mesh.suggest_local_scan_urls
        alpha.mesh.suggest_local_scan_urls = lambda **_: [beta_base_url]
        try:
            first = alpha.mesh.activate_autonomic_mesh(
                limit=12,
                scan_timeout=0.1,
                timeout=2.0,
                max_enlist=1,
                run_proof=False,
                request_id="autonomic-duplicate-request",
            )
            second = alpha.mesh.activate_autonomic_mesh(
                limit=12,
                scan_timeout=0.1,
                timeout=2.0,
                max_enlist=1,
                run_proof=False,
                request_id="autonomic-duplicate-request",
            )
        finally:
            alpha.mesh.suggest_local_scan_urls = original

        self.assertTrue(second["deduped"])
        self.assertEqual(first["run"]["id"], second["run"]["id"])
        self.assertEqual(second["request_id"], "autonomic-duplicate-request")

    def test_autonomic_activation_does_not_enlist_candidate_with_failed_route(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self._register_default_worker(beta, worker_id="beta-route-failed-worker")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh._update_peer_record(
            "beta-node",
            metadata={
                "last_reachable_base_url": beta_base_url,
                "route_health": {
                    "status": "unreachable",
                    "best_route": beta_base_url,
                    "checked_at": "2026-04-22T00:00:00Z",
                    "last_error": "timed out",
                    "freshness": "failed",
                    "failure_count": 1,
                },
            },
        )
        original_scan = alpha.mesh.suggest_local_scan_urls
        original_probe = alpha.mesh.autonomy.probe_routes
        alpha.mesh.suggest_local_scan_urls = lambda **_: []
        alpha.mesh.autonomy.probe_routes = lambda **_: {
            "status": "attention_needed",
            "peer_id": "beta-node",
            "checked": 1,
            "reachable": 0,
            "best_route": "",
            "candidates": [],
            "operator_summary": "No working route found for beta-node.",
        }
        try:
            result = alpha.mesh.activate_autonomic_mesh(
                limit=12,
                scan_timeout=0.1,
                timeout=0.2,
                max_enlist=1,
                run_proof=False,
                request_id="autonomic-failed-route-helper-test",
            )
        finally:
            alpha.mesh.suggest_local_scan_urls = original_scan
            alpha.mesh.autonomy.probe_routes = original_probe

        self.assertFalse(result["helpers"]["enlisted"])
        self.assertTrue(any(item["reason"] == "route_not_usable" for item in result["helpers"]["skipped"]))

    def test_autonomic_partner_helper_requires_approval_and_rejection_learns_deny(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="partner")
        original = alpha.mesh.suggest_local_scan_urls
        alpha.mesh.suggest_local_scan_urls = lambda **_: [beta_base_url]
        try:
            result = alpha.mesh.activate_autonomic_mesh(
                limit=12,
                scan_timeout=0.1,
                timeout=2.0,
                max_enlist=1,
                run_proof=False,
                request_id="autonomic-partner-approval-test",
            )
        finally:
            alpha.mesh.suggest_local_scan_urls = original

        self.assertEqual(result["status"], "approval_requested")
        approval = result["approvals"][0]["approval"]
        rejected = alpha.mesh.resolve_approval(
            approval["id"],
            decision="rejected",
            operator_peer_id="alpha-node",
            operator_agent_id="test-ui",
            reason="test reject",
        )
        self.assertEqual(rejected["status"], "rejected")
        preferences = alpha.mesh.list_offload_preferences(peer_id="beta-node", workload_class="connectivity_test")
        self.assertEqual(preferences["preferences"][0]["preference"], "deny")

    def test_autonomic_route_repair_retries_transport_timeout_once(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="trusted")
        calls = {"proof": 0, "probe": 0, "sync": 0}
        original_launch = alpha.mesh.launch_mesh_test_mission
        original_probe = alpha.mesh.autonomy.probe_routes
        original_sync = alpha.mesh.sync_peer
        original_scan = alpha.mesh.suggest_local_scan_urls

        def fake_launch(**kwargs):
            calls["proof"] += 1
            if calls["proof"] == 1:
                return {
                    "status": "ok",
                    "mission": {"id": "failed-proof", "status": "failed", "metadata": {"launch_error": "<urlopen error timed out>"}},
                }
            return {"status": "ok", "mission": {"id": "repaired-proof", "status": "completed", "metadata": {}}}

        def fake_probe(**kwargs):
            calls["probe"] += 1
            return {
                "status": "ok",
                "peer_id": kwargs.get("peer_id") or "beta-node",
                "checked": 1,
                "reachable": 1,
                "best_route": beta_base_url,
                "candidates": [],
                "operator_summary": "Beta is reachable after repair.",
            }

        def fake_sync(*args, **kwargs):
            calls["sync"] += 1
            return {"status": "ok", "peer_id": args[0] if args else kwargs.get("peer_id", "beta-node")}

        alpha.mesh.launch_mesh_test_mission = fake_launch
        alpha.mesh.autonomy.probe_routes = fake_probe
        alpha.mesh.sync_peer = fake_sync
        alpha.mesh.suggest_local_scan_urls = lambda **_: [beta_base_url]
        try:
            result = alpha.mesh.activate_autonomic_mesh(
                limit=12,
                timeout=2.0,
                max_enlist=0,
                run_proof=True,
                repair=True,
                request_id="autonomic-repair-test",
            )
        finally:
            alpha.mesh.launch_mesh_test_mission = original_launch
            alpha.mesh.autonomy.probe_routes = original_probe
            alpha.mesh.sync_peer = original_sync
            alpha.mesh.suggest_local_scan_urls = original_scan

        self.assertEqual(calls["proof"], 2)
        self.assertGreaterEqual(calls["probe"], 1)
        self.assertGreaterEqual(calls["sync"], 1)
        self.assertTrue(any(action["kind"] == "whole_mesh_proof_retry" for action in result["actions"]))

    def test_server_autonomic_mesh_handlers_round_trip(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        server.server_context["mesh"] = alpha.mesh
        alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="trusted")

        probe = ProbeHandler()
        probe._handle_mesh_routes_probe({"peer_id": "beta-node", "timeout": 2.0})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["status"], "ok")
        self.assertEqual(probe.payload["best_route"], beta_base_url)

        probe = ProbeHandler()
        probe._handle_mesh_routes_health()
        self.assertEqual(probe.code, 200)
        self.assertGreaterEqual(probe.payload["healthy"], 1)

        probe = ProbeHandler()
        probe._handle_mesh_autonomy_status()
        self.assertEqual(probe.code, 200)
        self.assertIn("operator_summary", probe.payload)

        probe = ProbeHandler()
        original = alpha.mesh.suggest_local_scan_urls
        alpha.mesh.suggest_local_scan_urls = lambda **_: [beta_base_url]
        try:
            probe._handle_mesh_autonomy_activate({"run_proof": False, "max_enlist": 1, "request_id": "autonomic-handler-test"})
            self.assertEqual(probe.code, 200)
            self.assertIn(probe.payload["status"], {"completed", "partial"})
        finally:
            alpha.mesh.suggest_local_scan_urls = original

    def test_server_connect_module_exposes_easy_and_connect_surface(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)

        connected = server_connect.connect_peer(alpha.mesh, {"base_url": beta_base_url, "trust_tier": "trusted"})
        self.assertEqual(connected["status"], "ok")
        self.assertEqual(connected["peer"]["peer_id"], "beta-node")

        synced = server_connect.sync_peer(alpha.mesh, {"peer_id": "beta-node", "limit": 12})
        self.assertEqual(synced["status"], "ok")
        self.assertEqual(synced["peer"]["peer_id"], "beta-node")

        diagnostics = server_connect.connectivity_diagnostics(alpha.mesh)
        self.assertEqual(diagnostics["status"], "ok")
        self.assertIn("scan_urls", diagnostics)

        launched = server_connect.launch_test_mission(alpha.mesh, {"peer_id": "beta-node"})
        self.assertEqual(launched["status"], "ok")
        self.assertEqual(launched["peer_id"], "beta-node")

        whole_mesh = server_connect.launch_mesh_test_mission(alpha.mesh, {"include_local": True, "limit": 12})
        self.assertEqual(whole_mesh["status"], "ok")
        self.assertGreaterEqual(whole_mesh["mesh"]["peer_count"], 2)

        bootstrap = server_connect.build_easy_bootstrap(alpha.mesh)
        self.assertIn('"control_stream"', bootstrap)
        self.assertIn('"connectivity"', bootstrap)

        markup = server_connect.build_easy_page(alpha.mesh)
        self.assertIn("OCP Easy Setup", markup)
        self.assertIn("Connect Everything", markup)
        self.assertIn("beta-node", markup)

    def test_server_app_module_exposes_unified_app_surface(self):
        alpha = self.make_stack("alpha")

        manifest = server_app.build_app_manifest(alpha.mesh)
        self.assertEqual(manifest["short_name"], "OCP")
        self.assertEqual(manifest["start_url"], "/app")
        self.assertEqual(manifest["scope"], "/")
        self.assertEqual(manifest["display"], "standalone")

        markup = server_app.build_app_page(alpha.mesh)
        self.assertIn("OCP App", markup)
        self.assertIn("OCP Easy Setup", markup)
        self.assertIn("OCP Control Deck", markup)
        self.assertIn("/easy", markup)
        self.assertIn("/control", markup)
        self.assertIn("/mesh/manifest", markup)
        self.assertIn("/mesh/contract", markup)
        self.assertIn("Install this app", markup)

    def test_server_control_page_module_renders_detached_control_shell(self):
        alpha = self.make_stack("alpha")

        markup = server_control_page.build_control_page(alpha.mesh)
        self.assertIn("OCP Control Deck", markup)
        self.assertIn("/mesh/control/stream", markup)
        self.assertIn("Mesh Pulse", markup)

    def test_server_missions_module_exposes_operator_action_surface(self):
        alpha = self.make_stack("alpha")
        self._register_default_worker(alpha, worker_id="alpha-module-worker")

        launched = server_missions.launch_mission(
            alpha.mesh,
            {
                "title": "Module Mission",
                "intent": "Exercise mission server module",
                "request_id": "module-mission-1",
                "job": {
                    "kind": "python.inline",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"code": "print('module mission')"},
                },
            },
        )
        self.assertEqual(launched["status"], "waiting")

        listed = server_missions.list_missions(alpha.mesh, limit=10)
        self.assertEqual(listed["count"], 1)

        fetched = server_missions.get_mission(alpha.mesh, launched["id"])
        self.assertEqual(fetched["id"], launched["id"])

        continuity = server_missions.get_mission_continuity(alpha.mesh, launched["id"])
        self.assertEqual(continuity["mission_id"], launched["id"])

        cancelled = server_missions.cancel_mission(alpha.mesh, launched["id"], {"operator_id": "module-ui"})
        self.assertEqual(cancelled["mission"]["status"], "cancelled")

        state = self._checkpointed_mission(alpha, worker_id="alpha-module-worker", request_id="module-mission-recovery")
        resumed = server_missions.resume_mission(alpha.mesh, state["mission"]["id"], {"operator_id": "module-ui"})
        self.assertEqual(resumed["mission"]["metadata"]["last_control_action"], "resume_latest")

        task = server_missions.launch_cooperative_task(
            alpha.mesh,
            {
                "name": "module-task",
                "request_id": "module-task-1",
                "target_peer_ids": ["alpha-node"],
                "base_job": {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", "print('module-base')"]},
                    "artifact_inputs": [],
                },
                "shards": [
                    {"label": "local", "payload": {"command": [sys.executable, "-c", "print('module-local')"]}},
                ],
            },
        )
        self.assertEqual(task["shard_count"], 1)
        self.assertEqual(server_missions.get_cooperative_task(alpha.mesh, task["id"])["id"], task["id"])

    def test_server_ops_module_exposes_operator_runtime_surface(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-node", limit=20, refresh_manifest=True)

        worker = server_ops.register_worker(
            alpha.mesh,
            {
                "worker_id": "alpha-ops-worker",
                "agent_id": alpha.agent_id,
                "capabilities": ["worker-runtime", "shell"],
                "resources": {"cpu": 1},
            },
        )
        self.assertEqual(worker["status"], "ok")
        self.assertEqual(server_ops.list_workers(alpha.mesh, limit=10)["count"], 1)

        enlisted = server_ops.enlist_helper(alpha.mesh, {"peer_id": "beta-node"})
        self.assertEqual(enlisted["state"], "enlisted")
        self.assertGreaterEqual(server_ops.list_helpers(alpha.mesh, limit=10)["count"], 1)

        preference = server_ops.set_offload_preference(
            alpha.mesh,
            {
                "peer_id": "beta-node",
                "workload_class": "gpu_inference",
                "preference": "prefer",
            },
        )
        self.assertEqual(preference["preference"], "prefer")
        self.assertEqual(
            server_ops.list_offload_preferences(alpha.mesh, limit=10, workload_class="gpu_inference")["count"],
            1,
        )

        notification = server_ops.publish_notification(
            alpha.mesh,
            {
                "notification_type": "job.summary",
                "title": "Ops alert",
                "body": "Needs attention",
                "target_peer_id": "watch-node",
            },
        )
        self.assertEqual(notification["status"], "ok")
        listed_notifications = server_ops.list_notifications(alpha.mesh, limit=10, target_peer_id="watch-node")
        self.assertEqual(listed_notifications["count"], 1)
        acked = server_ops.ack_notification(
            alpha.mesh,
            notification["notification"]["id"],
            {"status": "acked", "actor_peer_id": "watch-node"},
        )
        self.assertEqual(acked["notification"]["status"], "acked")

        approval = server_ops.create_approval_request(
            alpha.mesh,
            {
                "title": "Approve ops",
                "summary": "Approve operation",
                "target_peer_id": "watch-node",
            },
        )
        self.assertEqual(server_ops.list_approvals(alpha.mesh, limit=10, target_peer_id="watch-node")["count"], 1)
        resolved = server_ops.resolve_approval(
            alpha.mesh,
            approval["approval"]["id"],
            {"decision": "approved", "operator_peer_id": "watch-node"},
        )
        self.assertEqual(resolved["approval"]["status"], "approved")

        treaty = server_ops.propose_treaty(
            alpha.mesh,
            {
                "treaty_id": "treaty/server-ops-v1",
                "title": "Server Ops Treaty",
                "summary": "Operator treaty",
                "treaty_type": "continuity",
            },
        )
        self.assertEqual(treaty["status"], "ok")
        self.assertEqual(server_ops.list_treaties(alpha.mesh, limit=10, treaty_type="continuity")["count"], 1)
        audit = server_ops.audit_treaty_requirements(
            alpha.mesh,
            {"treaty_requirements": ["treaty/server-ops-v1"], "operation": "continuity_export"},
        )
        self.assertEqual(audit["status"], "ok")
        self.assertTrue(audit["validation"]["satisfied"])

        secret = server_ops.put_secret(
            alpha.mesh,
            {"name": "ops-token", "scope": "mesh.ops", "value": "secret"},
        )
        self.assertEqual(secret["status"], "ok")
        self.assertEqual(server_ops.list_secrets(alpha.mesh, limit=10, scope="mesh.ops")["count"], 1)

        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('ops queue')"]},
                "artifact_inputs": [],
            },
            request_id="ops-module-job",
        )
        self.assertEqual(server_ops.list_queue_messages(alpha.mesh, limit=10, status="queued")["count"], 1)
        self.assertEqual(server_ops.queue_metrics(alpha.mesh)["counts"]["queued"], 1)
        self.assertEqual(server_ops.list_scheduler_decisions(alpha.mesh, limit=10, status="placed")["count"], 1)

    def test_server_runtime_module_exposes_core_protocol_surface(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        self._register_default_worker(beta, worker_id="beta-runtime-worker")

        manifest = server_runtime.get_manifest(alpha.mesh)
        self.assertEqual(manifest["protocol_short_name"], "OCP")

        updated = server_runtime.update_device_profile(
            alpha.mesh,
            {"device_profile": {"device_class": "light", "network_profile": "wifi", "form_factor": "phone"}},
        )
        self.assertEqual(updated["device_profile"]["device_class"], "light")
        self.assertEqual(server_runtime.get_device_profile(alpha.mesh)["device_profile"]["form_factor"], "phone")

        alpha_manifest = alpha.mesh.get_manifest()
        peer_card = dict(alpha_manifest["organism_card"])
        peer_card["trust_tier"] = "trusted"
        handshake = alpha.mesh.build_signed_envelope(
            "/mesh/handshake",
            {"peer_card": peer_card, "manifest": alpha_manifest, "request_id": "server-runtime-handshake"},
        )
        accepted = server_runtime.accept_handshake(beta.mesh, handshake)
        self.assertEqual(accepted["status"], "ok")
        self.assertEqual(server_runtime.list_peers(beta.mesh, limit=10)["count"], 1)

        lease = server_runtime.acquire_lease(
            beta.mesh,
            {"peer_id": "alpha-node", "resource": "runtime/test", "ttl_seconds": 120},
        )
        self.assertEqual(lease["status"], "active")
        lease = server_runtime.heartbeat_lease(beta.mesh, {"lease_id": lease["id"], "ttl_seconds": 180})
        self.assertEqual(lease["ttl_seconds"], 180)
        lease = server_runtime.release_lease(beta.mesh, {"lease_id": lease["id"], "status": "released"})
        self.assertEqual(lease["status"], "released")

        submitted = beta.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('runtime module')"]},
                "artifact_inputs": [],
            },
            request_id="runtime-module-job",
        )
        claimed = beta.mesh.claim_next_job("beta-runtime-worker", job_id=submitted["job"]["id"], ttl_seconds=120)
        heartbeat = server_runtime.heartbeat_attempt(
            beta.mesh,
            claimed["attempt"]["id"],
            {"ttl_seconds": 120, "metadata": {"phase": "runtime-module"}},
        )
        self.assertEqual(heartbeat["status"], "ok")
        completed = server_runtime.complete_attempt(
            beta.mesh,
            claimed["attempt"]["id"],
            {
                "result": {"stdout": "runtime module\n", "stderr": "", "exit_code": 0},
                "executor": "runtime-module",
            },
        )
        self.assertEqual(completed["status"], "completed")

        handoff = alpha.mesh.build_signed_envelope(
            "/mesh/agents/handoff",
            {
                "handoff": {
                    "to_peer_id": "beta-node",
                    "from_agent": alpha.agent_id,
                    "to_agent": beta.agent_id,
                    "summary": "Runtime module handoff",
                    "intent": "Continue runtime module verification",
                    "constraints": {"project_id": "runtime-module"},
                    "artifact_refs": [],
                }
            },
        )
        accepted_handoff = server_runtime.accept_handoff(beta.mesh, handoff)
        self.assertEqual(accepted_handoff["status"], "accepted")

        stream = server_runtime.stream_snapshot(beta.mesh, limit=20)
        self.assertGreaterEqual(len(stream["events"]), 1)
        self.assertTrue(stream["generated_at"])

    def test_server_artifacts_module_exposes_artifact_transport_surface(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        artifact = beta.mesh.publish_local_artifact(
            {"kind": "artifact-module"},
            media_type="application/json",
            metadata={"artifact_kind": "bundle", "job_id": "artifact-module-job"},
        )
        listed = server_artifacts.list_artifacts(beta.mesh, limit=10, artifact_kind="bundle", job_id="artifact-module-job")
        self.assertEqual(listed["count"], 1)
        fetched = server_artifacts.get_artifact(beta.mesh, artifact["id"], include_content=False)
        self.assertEqual(fetched["id"], artifact["id"])

        replicated = server_artifacts.replicate_artifact(
            alpha.mesh,
            {"peer_id": "beta-node", "artifact_id": artifact["id"], "pin": True},
        )
        self.assertEqual(replicated["status"], "replicated")

        verified = server_artifacts.verify_artifact_mirror(
            alpha.mesh,
            {
                "artifact_id": replicated["artifact"]["id"],
                "peer_id": "beta-node",
                "source_artifact_id": artifact["id"],
            },
        )
        self.assertEqual(verified["status"], "verified")

        pinned = server_artifacts.set_artifact_pin(
            alpha.mesh,
            {"artifact_id": replicated["artifact"]["id"], "pinned": True, "reason": "module-pin"},
        )
        self.assertEqual(pinned["status"], "ok")

        ephemeral = beta.mesh.publish_local_artifact(
            "ephemeral artifact",
            metadata={"artifact_kind": "log", "retention_class": "ephemeral", "retention_seconds": 60},
        )
        with beta.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_artifacts SET retention_deadline_at=? WHERE id=?",
                ("2000-01-01T00:00:00Z", ephemeral["id"]),
            )
            conn.commit()
        purged = server_artifacts.purge_expired_artifacts(beta.mesh, {"limit": 10})
        self.assertGreaterEqual(purged["purged"], 1)

    def test_server_routes_module_dispatches_grouped_http_routes(self):
        class RouteProbe:
            def __init__(self):
                self.calls = []

            def _handle_app_page(self):
                self.calls.append(("app",))

            def _handle_app_manifest(self):
                self.calls.append(("app_manifest",))

            def _handle_easy_page(self):
                self.calls.append(("easy",))

            def _handle_control_stream(self, params):
                self.calls.append(("control_stream", dict(params)))

            def _handle_mesh_contract(self):
                self.calls.append(("contract",))

            def _handle_mesh_app_status(self):
                self.calls.append(("app_status",))

            def _handle_mesh_manifest(self):
                self.calls.append(("manifest",))

            def _handle_mesh_mission_continuity_get(self, path):
                self.calls.append(("mission_continuity", path))

            def _handle_mesh_mission_get(self, path):
                self.calls.append(("mission_get", path))

            def _handle_mesh_artifact_get(self, path, params):
                self.calls.append(("artifact_get", path, dict(params)))

            def _handle_mesh_notification_ack(self, path, data):
                self.calls.append(("notification_ack", path, dict(data)))

            def _handle_mesh_handoff(self, data):
                self.calls.append(("handoff", dict(data)))

        probe = RouteProbe()

        self.assertIn("missions", server_routes.GET_ROUTE_GROUPS)
        self.assertIn("runtime", server_routes.POST_ROUTE_GROUPS)
        self.assertEqual(
            server_routes.resolve_get_route("/mesh/missions/mission-1/continuity").handler_name,
            "_handle_mesh_mission_continuity_get",
        )
        self.assertEqual(server_routes.resolve_get_route("/").handler_name, "_handle_app_page")
        self.assertEqual(server_routes.resolve_get_route("/app").handler_name, "_handle_app_page")
        self.assertEqual(server_routes.resolve_get_route("/app.webmanifest").handler_name, "_handle_app_manifest")
        self.assertEqual(server_routes.resolve_get_route("/easy").handler_name, "_handle_easy_page")
        self.assertEqual(server_routes.resolve_get_route("/mesh/contract").handler_name, "_handle_mesh_contract")
        self.assertEqual(server_routes.resolve_get_route("/mesh/app/status").handler_name, "_handle_mesh_app_status")
        self.assertEqual(server_routes.resolve_get_route("/mesh/autonomy/status").handler_name, "_handle_mesh_autonomy_status")
        self.assertEqual(server_routes.resolve_get_route("/mesh/routes/health").handler_name, "_handle_mesh_routes_health")
        self.assertEqual(server_routes.resolve_post_route("/mesh/autonomy/activate").handler_name, "_handle_mesh_autonomy_activate")
        self.assertEqual(server_routes.resolve_post_route("/mesh/routes/probe").handler_name, "_handle_mesh_routes_probe")
        self.assertEqual(
            server_routes.resolve_post_route("/mesh/notifications/n-1/ack").handler_name,
            "_handle_mesh_notification_ack",
        )

        self.assertTrue(server_routes.dispatch_get(probe, "/", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/app", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/app.webmanifest", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/easy", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/mesh/control/stream", {"since": ["4"]}))
        self.assertTrue(server_routes.dispatch_get(probe, "/mesh/contract", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/mesh/app/status", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/mesh/manifest", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/mesh/missions/mission-1/continuity", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/mesh/missions/mission-1", {}))
        self.assertTrue(server_routes.dispatch_get(probe, "/mesh/artifacts/artifact-1", {"include_content": ["0"]}))
        self.assertTrue(server_routes.dispatch_post(probe, "/mesh/notifications/n-1/ack", {"status": "acked"}))
        self.assertTrue(server_routes.dispatch_post(probe, "/mesh/agents/handoff", {"handoff": {"summary": "route"}}))
        self.assertFalse(server_routes.dispatch_get(probe, "/mesh/unknown", {}))
        self.assertFalse(server_routes.dispatch_post(probe, "/mesh/unknown", {}))

        self.assertEqual(
            probe.calls,
            [
                ("app",),
                ("app",),
                ("app_manifest",),
                ("easy",),
                ("control_stream", {"since": ["4"]}),
                ("contract",),
                ("app_status",),
                ("manifest",),
                ("mission_continuity", "/mesh/missions/mission-1/continuity"),
                ("mission_get", "/mesh/missions/mission-1"),
                ("artifact_get", "/mesh/artifacts/artifact-1", {"include_content": ["0"]}),
                ("notification_ack", "/mesh/notifications/n-1/ack", {"status": "acked"}),
                ("handoff", {"handoff": {"summary": "route"}}),
            ],
        )

        alpha = self.make_stack("alpha")
        probe_handler = ProbeHandler()
        probe_handler.server = SimpleNamespace(mesh=alpha.mesh)

        handled = probe_handler._dispatch_get_request("/mesh/manifest", {})
        self.assertTrue(handled)
        self.assertEqual(probe_handler.payload["protocol_short_name"], "OCP")

        handled = probe_handler._dispatch_post_request(
            "/mesh/device-profile",
            {"device_profile": {"device_class": "light", "network_profile": "wifi"}},
        )
        self.assertTrue(handled)
        self.assertEqual(probe_handler.payload["device_profile"]["device_class"], "light")
        self.assertFalse(probe_handler._dispatch_get_request("/mesh/not-real", {}))

    def test_server_contract_module_exposes_mesh_route_contract(self):
        snapshot = server_contract.build_contract_snapshot()
        endpoints = snapshot["endpoints"]
        endpoint_ids = {endpoint["id"] for endpoint in endpoints}

        self.assertEqual(snapshot["status"], "ok")
        self.assertEqual(snapshot["protocol_surface"], "/mesh/*")
        self.assertEqual(snapshot["endpoint_count"], len(endpoints))
        self.assertEqual(snapshot["schema_version"], SCHEMA_VERSION)
        self.assertGreaterEqual(snapshot["schema_count"], 12)
        self.assertIn("MeshManifest", snapshot["schemas"])
        self.assertIn("SignedEnvelope", snapshot["schemas"])
        self.assertIn("JobSubmission", snapshot["schemas"])
        self.assertIn("ArtifactDescriptor", snapshot["schemas"])
        self.assertIn("MissionContinuitySummary", snapshot["schemas"])
        self.assertIn("TreatyAudit", snapshot["schemas"])
        self.assertIn("AutonomicActivateRequest", snapshot["schemas"])
        self.assertIn("AutonomicRun", snapshot["schemas"])
        self.assertIn("RouteHealth", snapshot["schemas"])
        self.assertIn("AppStatus", snapshot["schemas"])
        self.assertIn("ProtocolConformanceSnapshot", snapshot["schemas"])
        self.assertIn("runtime", snapshot["groups"])
        self.assertIn("missions", snapshot["groups"])
        self.assertIn("ops", snapshot["groups"])
        self.assertIn("artifacts", snapshot["groups"])
        self.assertIn("get:/mesh/contract", endpoint_ids)
        self.assertIn("get:/mesh/app/status", endpoint_ids)
        self.assertIn("get:/mesh/autonomy/status", endpoint_ids)
        self.assertIn("get:/mesh/routes/health", endpoint_ids)
        self.assertIn("post:/mesh/autonomy/activate", endpoint_ids)
        self.assertIn("post:/mesh/routes/probe", endpoint_ids)
        self.assertIn("get:/mesh/missions/{mission_id}/continuity", endpoint_ids)
        self.assertIn("post:/mesh/jobs/{job_id}/resume-from-checkpoint", endpoint_ids)
        self.assertIn("post:/mesh/notifications/{notification_id}/ack", endpoint_ids)

        mission_continuity = server_contract.contract_for("GET", "/mesh/missions/mission-1/continuity")
        self.assertEqual(mission_continuity["path"], "/mesh/missions/{mission_id}/continuity")
        self.assertEqual(mission_continuity["request"]["path"]["mission_id"], "string")
        self.assertEqual(mission_continuity["response"]["schema_ref"], "MissionContinuitySummary")
        self.assertTrue(mission_continuity["response"]["schema_available"])

        queue_events = server_contract.contract_for("GET", "/mesh/queue/events")
        self.assertEqual(queue_events["request"]["query"]["since"], "integer")
        self.assertEqual(queue_events["request"]["query"]["since_seq"], "integer")
        self.assertEqual(queue_events["response"]["schema_ref"], "QueueEventList")

        ack_deadline = server_contract.contract_for("POST", "/mesh/queue/ack-deadline")
        self.assertEqual(ack_deadline["request"]["body"]["ttl_seconds"], "integer")
        self.assertEqual(ack_deadline["request"]["body"]["ack_deadline_seconds"], "integer")

        manifest_contract = server_contract.contract_for("GET", "/mesh/manifest")
        self.assertEqual(manifest_contract["response"]["schema_ref"], "MeshManifest")
        self.assertTrue(manifest_contract["response"]["schema_available"])

        app_status_contract = server_contract.contract_for("GET", "/mesh/app/status")
        self.assertEqual(app_status_contract["response"]["schema_ref"], "AppStatus")
        self.assertTrue(app_status_contract["response"]["schema_available"])

        activate_contract = server_contract.contract_for("POST", "/mesh/autonomy/activate")
        self.assertEqual(activate_contract["request"]["schema_ref"], "AutonomicActivateRequest")
        self.assertEqual(activate_contract["response"]["schema_ref"], "AutonomicRun")
        self.assertTrue(activate_contract["response"]["schema_available"])

        routes_contract = server_contract.contract_for("GET", "/mesh/routes/health")
        self.assertEqual(routes_contract["response"]["schema_ref"], "RouteHealthList")
        self.assertTrue(routes_contract["response"]["schema_available"])

        handshake_contract = server_contract.contract_for("POST", "/mesh/handshake")
        self.assertEqual(handshake_contract["request"]["schema_ref"], "SignedEnvelope")

        submit_contract = server_contract.contract_for("POST", "/mesh/jobs/submit")
        self.assertEqual(submit_contract["request"]["schema_ref"], "SignedEnvelope")

        replicate_contract = server_contract.contract_for("POST", "/mesh/artifacts/replicate")
        self.assertEqual(replicate_contract["request"]["body"]["digest"], "string")

        ack_contract = server_contract.contract_for("POST", "/mesh/notifications/n-1/ack")
        self.assertEqual(ack_contract["request"]["path"]["notification_id"], "string")
        self.assertEqual(ack_contract["request"]["body"]["status"], "string")
        self.assertIsNone(server_contract.contract_for("PATCH", "/mesh/manifest"))

        manifest_schema = get_protocol_schema("MeshManifest")
        self.assertIn("protocol_version", manifest_schema["required"])
        self.assertIn("organism_card", manifest_schema["properties"])
        self.assertIn("TreatyAudit", list_protocol_schemas())
        self.assertIn("AutonomicMeshStatus", list_protocol_schemas())
        self.assertIn("AppStatus", list_protocol_schemas())
        self.assertIn("RouteProbeResult", list_protocol_schemas())
        self.assertIn("ProtocolConformanceSnapshot", list_protocol_schemas())

        conformance = snapshot["conformance"]
        self.assertEqual(conformance["status"], "ok")
        self.assertGreaterEqual(conformance["fixture_count"], 8)
        self.assertEqual(conformance["invalid_fixture_count"], 0)
        self.assertTrue(all(item["validation"]["status"] == "ok" for item in conformance["fixtures"]))

        direct_conformance = build_protocol_conformance_snapshot()
        self.assertEqual(direct_conformance["status"], "ok")
        self.assertEqual(direct_conformance["invalid_fixture_count"], 0)
        self.assertIn(
            "signed-envelope-minimal",
            {fixture["id"] for fixture in direct_conformance["fixtures"]},
        )
        self.assertIn(
            "autonomic-activate-request",
            {fixture["id"] for fixture in direct_conformance["fixtures"]},
        )
        self.assertIn(
            "app-status-operator-home",
            {fixture["id"] for fixture in direct_conformance["fixtures"]},
        )

        valid_audit = validate_protocol_object(
            "TreatyAuditRequest",
            {"treaty_requirements": ["treaty/alpha"], "operation": "continuity_export"},
        )
        self.assertEqual(valid_audit["status"], "ok")

        valid_autonomic = validate_protocol_object(
            "AutonomicActivateRequest",
            {"mode": "assisted", "run_proof": True, "repair": True},
        )
        self.assertEqual(valid_autonomic["status"], "ok")

        valid_app_status = validate_protocol_object(
            "AppStatus",
            {
                "status": "ok",
                "node": {},
                "app_urls": {},
                "mesh_quality": {},
                "route_health": {"status": "ok", "routes": []},
                "next_actions": ["Activate Autonomic Mesh."],
            },
        )
        self.assertEqual(valid_app_status["status"], "ok")

        invalid_envelope = validate_protocol_object("SignedEnvelope", {"request": {}})
        self.assertEqual(invalid_envelope["status"], "invalid")
        self.assertTrue(any(issue["path"] == "$.body" for issue in invalid_envelope["issues"]))
        self.assertTrue(any(issue["path"] == "$.request.node_id" for issue in invalid_envelope["issues"]))

        alpha = self.make_stack("alpha")
        probe_handler = ProbeHandler()
        probe_handler.server = SimpleNamespace(mesh=alpha.mesh)
        self.assertTrue(probe_handler._dispatch_get_request("/mesh/contract", {}))
        self.assertEqual(probe_handler.payload["contract_version"], server_contract.CONTRACT_VERSION)
        self.assertEqual(probe_handler.payload["schema_version"], SCHEMA_VERSION)
        self.assertEqual(probe_handler.payload["endpoint_count"], snapshot["endpoint_count"])
        self.assertIn("ArtifactDescriptor", probe_handler.payload["schemas"])
        self.assertEqual(probe_handler.payload["conformance"]["invalid_fixture_count"], 0)

        rejected = ProbeHandler()
        rejected.server = SimpleNamespace(mesh=alpha.mesh)
        self.assertTrue(rejected._dispatch_post_request("/mesh/handshake", {"request": {}}))
        self.assertEqual(rejected.code, 400)
        self.assertEqual(rejected.payload["error"], "protocol validation failed")
        self.assertEqual(rejected.payload["protocol_validation"]["schema_ref"], "SignedEnvelope")

        audit_validation = server_contract.validate_route_request(
            "POST",
            "/mesh/treaties/audit",
            {"treaty_requirements": ["treaty/alpha"], "operation": "continuity_restore"},
        )
        self.assertEqual(audit_validation["status"], "ok")
        invalid_audit = server_contract.validate_route_request(
            "POST",
            "/mesh/treaties/audit",
            {"treaty_requirements": "treaty/alpha"},
        )
        self.assertEqual(invalid_audit["status"], "invalid")

    def test_mesh_protocol_package_imports_cleanly_in_fresh_python_process(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from mesh_protocol import MeshPolicyError, MeshProtocolService, "
                    "build_protocol_conformance_snapshot; "
                    "snapshot = build_protocol_conformance_snapshot(); "
                    "print(MeshPolicyError.__name__, MeshProtocolService.__name__, snapshot['status'])"
                ),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
        self.assertIn("MeshPolicyError MeshProtocolService ok", result.stdout.strip())

    def test_protocol_conformance_check_script_passes(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "check_protocol_conformance.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
        self.assertIn("Protocol conformance snapshot:", result.stdout)
        self.assertIn("Protocol conformance OK", result.stdout)

    def test_device_profile_endpoint_is_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        alpha_client, _ = self.serve_mesh(alpha)

        updated = alpha_client.update_device_profile(
            {
                "device_class": "micro",
                "execution_tier": "sensor",
                "power_profile": "battery",
                "network_profile": "intermittent",
                "mobility": "wearable",
                "form_factor": "watch",
            }
        )
        fetched = alpha_client.device_profile()

        self.assertEqual(updated["device_profile"]["device_class"], "micro")
        self.assertEqual(fetched["device_profile"]["form_factor"], "watch")
        self.assertFalse(fetched["device_profile"]["compute_ready"])

    def test_discovery_endpoints_are_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha_client, _ = self.serve_mesh(alpha)
        _, beta_base_url = self.serve_mesh(beta)

        sought = alpha_client.seek_peers({"base_urls": [beta_base_url], "auto_connect": True, "trust_tier": "trusted"})
        self.assertEqual(sought["connected"], 1)

        candidates = alpha_client.list_discovery_candidates(limit=10)
        self.assertEqual(candidates["count"], 1)
        self.assertEqual(candidates["candidates"][0]["peer_id"], "beta-node")

    def test_easy_connect_endpoints_are_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha_client, _ = self.serve_mesh(alpha)
        _, beta_base_url = self.serve_mesh(beta)

        connected = alpha_client.connect_peer({"base_url": beta_base_url, "trust_tier": "trusted"})
        self.assertEqual(connected["status"], "ok")
        self.assertEqual(connected["peer"]["peer_id"], "beta-node")
        self.assertEqual(connected["peer_advisory"]["peer_id"], "beta-node")
        self.assertIn("treaty_compatibility", connected["peer_advisory"])
        self.assertIn("operator_summary", connected)

        original = alpha.mesh.suggest_local_scan_urls
        alpha.mesh.suggest_local_scan_urls = lambda **_: [beta_base_url]
        try:
            connected_all = alpha_client.connect_all_peers({"trust_tier": "trusted", "limit": 12})
        finally:
            alpha.mesh.suggest_local_scan_urls = original
        self.assertEqual(connected_all["status"], "ok")
        self.assertGreaterEqual(connected_all["already_connected"] + connected_all["connected"], 1)
        self.assertIn("mesh", connected_all)
        self.assertIn("operator_summary", connected_all)

        diagnostics = alpha_client.connectivity_diagnostics()
        self.assertEqual(diagnostics["status"], "ok")
        self.assertIn("scan_urls", diagnostics)

        launched = alpha_client.launch_test_mission({"peer_id": "beta-node"})
        self.assertEqual(launched["status"], "ok")
        self.assertEqual(launched["peer_id"], "beta-node")
        self.assertEqual(launched["mission"]["summary"]["cooperative_task_count"], 1)
        self.assertEqual(launched["mission"]["summary"]["job_count"], 1)

        whole_mesh = alpha_client.launch_mesh_test_mission({"include_local": True, "limit": 12})
        self.assertEqual(whole_mesh["status"], "ok")
        self.assertGreaterEqual(whole_mesh["mesh"]["peer_count"], 2)
        self.assertEqual(whole_mesh["mission"]["summary"]["cooperative_task_count"], 1)
        self.assertEqual(whole_mesh["mission"]["summary"]["job_count"], 2)

    def test_connect_peer_uses_reachable_base_url_when_remote_manifest_advertises_wildcard(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)
        beta.mesh.base_url = "http://0.0.0.0:8431"

        connected = alpha.mesh.connect_device(base_url=beta_base_url, trust_tier="trusted")

        self.assertEqual(connected["status"], "ok")
        self.assertEqual(connected["peer"]["peer_id"], "beta-node")
        self.assertTrue(str(connected["peer"]["endpoint_url"]).startswith("http://"))
        self.assertNotIn("0.0.0.0", connected["peer"]["endpoint_url"])

    def test_connect_peer_records_last_reachable_base_url_when_remote_manifest_advertises_other_endpoint(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)

        original_get_manifest = beta.mesh.get_manifest

        def manifest_with_virtual_endpoint():
            manifest = dict(original_get_manifest())
            card = dict(manifest.get("organism_card") or {})
            card["endpoint_url"] = "http://198.51.100.42:8421"
            card["stream_url"] = "http://198.51.100.42:8421/mesh/stream"
            manifest["organism_card"] = card
            return manifest

        beta.mesh.get_manifest = manifest_with_virtual_endpoint
        try:
            connected = alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        finally:
            beta.mesh.get_manifest = original_get_manifest

        self.assertEqual(connected["status"], "ok")
        peer = alpha.mesh.list_peers(limit=10)["peers"][0]
        self.assertEqual(peer["endpoint_url"], "http://198.51.100.42:8421")
        self.assertEqual(peer["metadata"]["last_reachable_base_url"], beta_base_url)

        resolved_client, resolved_peer = alpha.mesh._resolve_peer_client("beta-node")
        self.assertEqual(resolved_peer["peer_id"], "beta-node")
        self.assertEqual(resolved_client.base_url, beta_base_url)

    def test_sync_peer_refresh_records_last_reachable_base_url(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        beta_client, beta_base_url = self.serve_mesh(beta)

        original_get_manifest = beta.mesh.get_manifest

        def manifest_with_virtual_endpoint():
            manifest = dict(original_get_manifest())
            card = dict(manifest.get("organism_card") or {})
            card["endpoint_url"] = "http://198.51.100.43:8421"
            card["stream_url"] = "http://198.51.100.43:8421/mesh/stream"
            manifest["organism_card"] = card
            return manifest

        remote_card = dict(beta.mesh.get_manifest()["organism_card"])
        remote_card["endpoint_url"] = "http://198.51.100.43:8421"
        remote_card["stream_url"] = "http://198.51.100.43:8421/mesh/stream"
        alpha.mesh.remember_peer_card(remote_card, trust_tier="trusted", status="connected")

        beta.mesh.get_manifest = manifest_with_virtual_endpoint
        try:
            synced = alpha.mesh.sync_peer(
                "beta-node",
                client=beta_client,
                base_url=beta_base_url,
                limit=20,
                refresh_manifest=True,
            )
        finally:
            beta.mesh.get_manifest = original_get_manifest

        self.assertEqual(synced["status"], "ok")
        self.assertEqual(synced["peer"]["metadata"]["last_reachable_base_url"], beta_base_url)

    def test_dispatch_job_to_peer_prefers_last_reachable_base_url_over_advertised_endpoint(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        _, beta_base_url = self.serve_mesh(beta)

        original_get_manifest = beta.mesh.get_manifest

        def manifest_with_virtual_endpoint():
            manifest = dict(original_get_manifest())
            card = dict(manifest.get("organism_card") or {})
            card["endpoint_url"] = "http://198.51.100.44:8421"
            card["stream_url"] = "http://198.51.100.44:8421/mesh/stream"
            manifest["organism_card"] = card
            return manifest

        beta.mesh.get_manifest = manifest_with_virtual_endpoint
        try:
            alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        finally:
            beta.mesh.get_manifest = original_get_manifest

        captured = {}

        class RecordingClient:
            def __init__(self, base_url, *, timeout=8.0):
                captured["base_url"] = base_url
                captured["timeout"] = timeout

            def submit_job(self, envelope):
                captured["request_id"] = envelope["request"]["request_id"]
                captured["job_kind"] = envelope["body"]["job"]["kind"]
                return {"status": "queued", "job": {"id": "remote-job-1"}}

        with mock.patch("mesh.sovereign.MeshPeerClient", RecordingClient):
            response = alpha.mesh.dispatch_job_to_peer(
                "beta-node",
                {
                    "kind": "agent.echo",
                    "origin": "alpha-node",
                    "target": "beta-node",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"message": "networked"},
                    "artifact_inputs": [],
                },
                request_id="reachable-dispatch-test",
            )

        self.assertEqual(response["status"], "queued")
        self.assertEqual(captured["base_url"], beta_base_url)
        self.assertEqual(captured["request_id"], "reachable-dispatch-test")
        self.assertEqual(captured["job_kind"], "agent.echo")

    def test_notification_and_approval_endpoints_are_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        alpha_client, _ = self.serve_mesh(alpha)

        notification = alpha_client.publish_notification(
            {
                "notification_type": "job.summary",
                "priority": "high",
                "title": "Relay job ready",
                "body": "Review on watch",
                "target_peer_id": "watch-node",
                "target_device_classes": ["micro"],
            }
        )
        self.assertEqual(notification["status"], "ok")
        notification_id = notification["notification"]["id"]

        acked = alpha_client.ack_notification(notification_id, actor_peer_id="watch-node", reason="seen")
        self.assertEqual(acked["notification"]["status"], "acked")

        approval = alpha_client.request_approval(
            {
                "title": "Approve resume",
                "summary": "Resume checkpoint on relay node",
                "action_type": "job.recovery.resume",
                "severity": "high",
                "target_peer_id": "watch-node",
                "target_device_classes": ["micro"],
            }
        )
        self.assertEqual(approval["status"], "pending")
        approval_id = approval["approval"]["id"]

        approvals = alpha_client.list_approvals(limit=10, target_peer_id="watch-node")
        self.assertEqual(approvals["count"], 1)

        resolved = alpha_client.resolve_approval(approval_id, decision="approved", operator_peer_id="watch-node")
        self.assertEqual(resolved["approval"]["status"], "approved")

    def test_control_page_is_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        self._register_default_worker(alpha, worker_id="alpha-control-worker")
        alpha.mesh.submit_local_job(
            {
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "payload": {"code": "print('queued from control deck')"},
            },
            request_id="control-http-queued-job",
        )
        alpha.mesh.publish_notification(
            notification_type="job.summary",
            priority="high",
            title="Relay status ready",
            body="Phone controller can see this.",
            target_peer_id="alpha-node",
            target_device_classes=["light"],
        )
        alpha.mesh.launch_mission(
            title="Control Mission",
            intent="Expose mission visibility in cockpit",
            request_id="control-http-mission",
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('control mission')"},
                "metadata": {
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual"},
                },
            },
        )
        alpha.mesh.run_worker_once("alpha-control-worker")
        alpha_client, base_url = self.serve_mesh(alpha)

        with urlopen(f"{base_url}/control") as response:
            markup = response.read().decode("utf-8")
            content_type = response.headers.get("Content-Type")

        self.assertEqual(content_type, "text/html; charset=utf-8")
        self.assertIn("OCP Control Deck", markup)
        self.assertIn("Relay status ready", markup)
        self.assertIn("Mesh Pulse", markup)
        self.assertIn("Connect Devices", markup)
        self.assertIn("Scan Nearby", markup)
        self.assertIn("Connect Everything", markup)
        self.assertIn("Test Whole Mesh", markup)
        self.assertIn("Send Test Mission", markup)
        self.assertIn("Live Mission Stream", markup)
        self.assertIn("Operator Inspect", markup)
        self.assertIn("Recovery + Queue", markup)
        self.assertIn("Mission Layer", markup)
        self.assertIn("Control Mission", markup)
        self.assertIn("Inspect Mission", markup)
        self.assertIn("Inspect Job", markup)
        self.assertIn("Inspect Task", markup)
        self.assertIn("Primary Job", markup)
        self.assertIn("Result Bundle", markup)
        self.assertIn("/mesh/artifacts/", markup)
        self.assertIn("/mesh/jobs/", markup)
        self.assertIn("/mesh/control/stream", markup)
        self.assertIn("Cancel Job", markup)
        self.assertIn("Refresh Deck", markup)
        self.assertIn("ocp-mobile-ui", markup)

    def test_easy_page_is_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha_client, base_url = self.serve_mesh(alpha)
        _, beta_base_url = self.serve_mesh(beta)
        alpha_client.connect_peer({"base_url": beta_base_url, "trust_tier": "trusted"})

        with urlopen(f"{base_url}/") as response:
            markup = response.read().decode("utf-8")
            content_type = response.headers.get("Content-Type")

        self.assertEqual(content_type, "text/html; charset=utf-8")
        self.assertIn("OCP App", markup)
        self.assertIn("One app for the mesh.", markup)
        self.assertIn("OCP Today", markup)
        self.assertIn("Activate Autonomic Mesh", markup)
        self.assertIn("Phone Link + QR", markup)
        self.assertIn("/mesh/app/status", markup)
        self.assertIn("ocp_operator_token", markup)
        self.assertIn("X-OCP-Operator-Token", markup)
        self.assertIn("if (!response.ok)", markup)
        self.assertIn("OCP Easy Setup", markup)
        self.assertIn("OCP Control Deck", markup)
        self.assertIn("/easy", markup)
        self.assertIn("/control", markup)
        self.assertIn("/app.webmanifest", markup)

        with urlopen(f"{base_url}/app") as response:
            app_markup = response.read().decode("utf-8")

        self.assertIn("OCP App", app_markup)

        with urlopen(f"{base_url}/mesh/app/status") as response:
            app_status = json.loads(response.read().decode("utf-8"))

        self.assertEqual(app_status["status"], "ok")
        self.assertEqual(app_status["node"]["node_id"], "alpha-node")
        self.assertIn("mesh_quality", app_status)
        self.assertIn("route_health", app_status)
        self.assertIn("latest_proof", app_status)
        self.assertIn("next_actions", app_status)
        self.assertIn("/app", app_status["app_urls"]["app_url"])

        with urlopen(f"{base_url}/app.webmanifest") as response:
            manifest = json.loads(response.read().decode("utf-8"))

        self.assertEqual(manifest["short_name"], "OCP")
        self.assertEqual(manifest["start_url"], "/app")

        with urlopen(f"{base_url}/easy") as response:
            easy_markup = response.read().decode("utf-8")

        self.assertIn("OCP Easy Setup", easy_markup)
        self.assertIn("Connect two computers without becoming the network department.", easy_markup)
        self.assertIn("Nearby Computers", easy_markup)
        self.assertIn("Scan Nearby", easy_markup)
        self.assertIn("Connect Everything", easy_markup)
        self.assertIn("Test Whole Mesh", easy_markup)
        self.assertIn("Send Test Mission", easy_markup)
        self.assertIn("Copy My Easy Link", easy_markup)
        self.assertIn("Share This Easy Link", easy_markup)
        self.assertIn("Scan This QR Code", easy_markup)
        self.assertIn("qrcode.min.js", easy_markup)
        self.assertIn("X-OCP-Operator-Token", easy_markup)
        self.assertIn("Open Advanced Deck", easy_markup)
        self.assertIn("beta-node", easy_markup)

    def test_start_ocp_easy_helpers_prefer_local_browser_url_for_wildcard_hosts(self):
        self.assertEqual(start_ocp_easy.display_host_for_browser("0.0.0.0"), "127.0.0.1")
        self.assertEqual(start_ocp_easy.display_host_for_browser("::"), "127.0.0.1")
        self.assertEqual(start_ocp_easy.display_host_for_browser("172.20.10.11"), "172.20.10.11")
        self.assertEqual(start_ocp_easy.build_open_url("0.0.0.0", 8421), "http://127.0.0.1:8421/")
        self.assertEqual(start_ocp_easy.build_open_url("172.20.10.11", 8431, "/control"), "http://172.20.10.11:8431/control")

    @mock.patch.object(start_ocp_easy, "discover_local_ipv4_addresses", return_value=["192.168.1.44", "10.0.0.21"])
    def test_start_ocp_easy_helpers_surface_share_urls(self, _discover_local_ipv4_addresses):
        self.assertEqual(
            start_ocp_easy.share_urls_for_host("0.0.0.0", 8421),
            ["http://192.168.1.44:8421/", "http://10.0.0.21:8421/"],
        )
        self.assertEqual(start_ocp_easy.share_urls_for_host("127.0.0.1", 8421), [])
        self.assertEqual(
            start_ocp_easy.share_urls_for_host("172.20.10.11", 8431),
            ["http://172.20.10.11:8431/"],
        )

    def test_startup_helpers_resolve_state_paths_and_server_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp) / "repo"
            repo_root.mkdir()
            state_dir = Path(tmp) / "state"
            profile = ocp_startup.profile_from_values(
                repo_root,
                host="0.0.0.0",
                port=8555,
                node_id="alpha-node",
                display_name="Alpha",
                state_dir=state_dir,
                create_paths=True,
            )
            command = ocp_startup.server_command(profile, repo_root, python_executable="python3")

            self.assertEqual(profile.db_path, state_dir / "ocp.db")
            self.assertTrue(profile.identity_dir.exists())
            self.assertTrue(profile.workspace_root.exists())
            self.assertEqual(command[0], "python3")
            self.assertIn(str(repo_root / "server.py"), command)
            self.assertIn("--host", command)
            self.assertIn("0.0.0.0", command)
            self.assertIn("--db-path", command)
            self.assertIn(str(state_dir / "ocp.db"), command)
            self.assertEqual(ocp_startup.health_url("0.0.0.0", 8555), "http://127.0.0.1:8555/mesh/manifest")

            custom_db = Path(tmp) / "custom" / "demo.db"
            custom_profile = ocp_startup.profile_from_values(
                repo_root,
                db_path=custom_db,
                node_id="custom-node",
                create_paths=True,
            )
            self.assertEqual(custom_profile.db_path, custom_db)
            self.assertEqual(custom_profile.identity_dir, custom_db.parent / "identity")
            self.assertEqual(custom_profile.workspace_root, custom_db.parent / "workspace")
            self.assertTrue(custom_profile.identity_dir.exists())
            self.assertTrue(custom_profile.workspace_root.exists())

    def test_desktop_launcher_builds_local_and_mesh_launch_plans(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp) / "repo"
            repo_root.mkdir()
            home = Path(tmp) / "home"
            config = {
                "port": 8666,
                "node_id": "desktop-alpha",
                "display_name": "Desktop Alpha",
            }
            local_plan = ocp_launcher.build_launch_plan("local", config, repo_root, home=home)
            mesh_plan = ocp_launcher.build_launch_plan("mesh", config, repo_root, home=home)

            self.assertEqual(local_plan.profile.host, "127.0.0.1")
            self.assertEqual(local_plan.app_url, "http://127.0.0.1:8666/")
            self.assertEqual(local_plan.share_urls, [])
            self.assertEqual(mesh_plan.profile.host, "0.0.0.0")
            self.assertIn("--display-name", mesh_plan.command)
            self.assertIn("Desktop Alpha", mesh_plan.command)
            self.assertTrue(str(home / "Library" / "Application Support" / "OCP" / "state") in str(mesh_plan.profile.db_path))
            self.assertEqual(mesh_plan.config_path, ocp_startup.default_launcher_config_path(home=home))
            self.assertEqual(
                ocp_launcher.operator_app_url("http://192.168.1.44:8666/", "secret token"),
                "http://192.168.1.44:8666/app#ocp_operator_token=secret%20token",
            )

    def test_desktop_launcher_config_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "launcher.json"
            saved = ocp_launcher.save_launcher_config(
                {"port": "8777", "node_id": "alpha-node", "display_name": "Alpha"},
                path,
            )
            loaded = ocp_launcher.load_launcher_config(path)

            self.assertEqual(saved["port"], 8777)
            self.assertEqual(loaded["port"], 8777)
            self.assertEqual(loaded["node_id"], "alpha-node")
            self.assertEqual(loaded["display_name"], "Alpha")
            self.assertIn("operator_token", loaded)

    def test_desktop_launcher_close_stops_running_server(self):
        class FakeProcess:
            def __init__(self):
                self.terminated = False
                self.killed = False

            def poll(self):
                return None

            def terminate(self):
                self.terminated = True

            def wait(self, timeout=None):
                return 0

            def kill(self):
                self.killed = True

        class FakeStatus:
            def __init__(self):
                self.value = ""

            def set(self, value):
                self.value = value

        class FakeRoot:
            def __init__(self):
                self.destroyed = False

            def destroy(self):
                self.destroyed = True

        app = ocp_launcher.OCPLauncherApp.__new__(ocp_launcher.OCPLauncherApp)
        app.process = FakeProcess()
        app.status = FakeStatus()
        app.root = FakeRoot()
        app.closing = False

        app.close()

        self.assertTrue(app.closing)
        self.assertTrue(app.process.terminated)
        self.assertFalse(app.process.killed)
        self.assertTrue(app.root.destroyed)

    def test_macos_app_builder_creates_unsigned_bundle_and_excludes_local_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp) / "repo"
            repo_root.mkdir()
            (repo_root / "server.py").write_text("print('server')\n", encoding="utf-8")
            (repo_root / "ocp_startup.py").write_text("# startup\n", encoding="utf-8")
            (repo_root / "ocp_desktop").mkdir()
            (repo_root / "ocp_desktop" / "__init__.py").write_text("", encoding="utf-8")
            (repo_root / "ocp_desktop" / "launcher.py").write_text("# launcher\n", encoding="utf-8")
            (repo_root / ".git").mkdir()
            (repo_root / ".git" / "config").write_text("secret\n", encoding="utf-8")
            (repo_root / ".local").mkdir()
            (repo_root / ".local" / "ocp.db").write_text("db\n", encoding="utf-8")
            (repo_root / ".mesh-alpha").mkdir()
            (repo_root / ".mesh-alpha" / "identity").write_text("key\n", encoding="utf-8")
            (repo_root / ".env").write_text("SECRET=1\n", encoding="utf-8")
            (repo_root / ".env.local").write_text("SECRET=2\n", encoding="utf-8")
            (repo_root / "secret.pem").write_text("pem\n", encoding="utf-8")
            (repo_root / "secret.key").write_text("key\n", encoding="utf-8")
            (repo_root / "secret.crt").write_text("crt\n", encoding="utf-8")
            (repo_root / "secret.p12").write_text("p12\n", encoding="utf-8")
            (repo_root / "secret.der").write_text("der\n", encoding="utf-8")
            (repo_root / "identity").mkdir()
            (repo_root / "identity" / "local.key").write_text("identity-key\n", encoding="utf-8")
            (repo_root / "cache.pyc").write_bytes(b"pyc")
            dist_dir = Path(tmp) / "dist-out"

            result = ocp_macos_app.build_macos_app(repo_root, dist_dir=dist_dir)
            app_path = Path(result["app_path"])
            bundled_repo = Path(result["bundled_repo"])
            executable = Path(result["executable"])
            executable_script = executable.read_text(encoding="utf-8")

            self.assertTrue((app_path / "Contents" / "Info.plist").exists())
            self.assertTrue(executable.exists())
            self.assertTrue(os.access(executable, os.X_OK))
            self.assertIn("cd \"$REPO_ROOT\"", executable_script)
            self.assertIn("-m ocp_desktop.launcher", executable_script)
            self.assertTrue((bundled_repo / "server.py").exists())
            self.assertFalse((bundled_repo / ".git").exists())
            self.assertFalse((bundled_repo / ".local").exists())
            self.assertFalse((bundled_repo / ".mesh-alpha").exists())
            self.assertFalse((bundled_repo / ".env").exists())
            self.assertFalse((bundled_repo / ".env.local").exists())
            self.assertFalse((bundled_repo / "secret.pem").exists())
            self.assertFalse((bundled_repo / "secret.key").exists())
            self.assertFalse((bundled_repo / "secret.crt").exists())
            self.assertFalse((bundled_repo / "secret.p12").exists())
            self.assertFalse((bundled_repo / "secret.der").exists())
            self.assertFalse((bundled_repo / "identity").exists())
            self.assertFalse((bundled_repo / "cache.pyc").exists())

    def test_connectivity_diagnostics_surface_share_urls_and_bind_guidance(self):
        alpha = self.make_stack("alpha")
        with mock.patch("mesh.sovereign._discover_local_ipv4_addresses", return_value=["192.168.1.44", "10.0.0.21"]):
            alpha.mesh.network_bind_host = "127.0.0.1"
            local_only = alpha.mesh.connectivity_diagnostics()
            self.assertEqual(local_only["sharing_mode"], "local")
            self.assertEqual(local_only["lan_urls"], [])
            self.assertEqual(local_only["share_url"], alpha.mesh.base_url)
            self.assertIn("OCP_HOST=0.0.0.0", local_only["share_advice"])

            alpha.mesh.network_bind_host = "0.0.0.0"
            lan_ready = alpha.mesh.connectivity_diagnostics()
            self.assertEqual(lan_ready["sharing_mode"], "lan")
            self.assertEqual(
                lan_ready["lan_urls"],
                ["http://192.168.1.44:8421", "http://10.0.0.21:8421"],
            )
            self.assertEqual(lan_ready["share_url"], "http://192.168.1.44:8421")
            self.assertEqual(lan_ready["share_advice"], "")

    def test_control_stream_payload_includes_state_and_recent_events(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.launch_mission(
            title="Stream Mission",
            intent="Verify control stream payload",
            request_id="control-stream-payload",
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('stream mission')"},
            },
        )

        payload = server.build_control_stream_payload(alpha.mesh, since_seq=0, limit=20)
        direct_payload = server_control.build_control_stream_payload(alpha.mesh, since_seq=0, limit=20)
        bootstrap = server_control.build_control_bootstrap(alpha.mesh)

        self.assertEqual(payload["type"], "control_state")
        self.assertEqual(direct_payload["type"], "control_state")
        self.assertEqual(payload["cursor"], direct_payload["cursor"])
        self.assertIn('"control_stream"', bootstrap)
        self.assertIn("Stream Mission", bootstrap)
        self.assertIn("peer_advisories", payload)
        self.assertIn("state", payload)
        self.assertIn("control_stream", payload["state"])
        self.assertEqual(payload["state"]["control_stream"]["route"], "/mesh/control/stream")
        self.assertGreaterEqual(payload["cursor"], 1)
        self.assertTrue(any(event["event_type"] == "mesh.mission.launched" for event in payload["events"]))
        self.assertEqual(payload["state"]["missions"]["missions"][0]["title"], "Stream Mission")
        self.assertIn("connected", payload["peer_advisories"])
        self.assertIn("counts", payload["peer_advisories"])

    def test_control_stream_is_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.launch_mission(
            title="HTTP Stream Mission",
            intent="Expose control stream state over HTTP",
            request_id="control-stream-http",
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('stream http mission')"},
            },
        )
        alpha_client, base_url = self.serve_mesh(alpha)
        self.assertIsNotNone(alpha_client)

        with urlopen(f"{base_url}/mesh/control/stream?since=0&limit=20&once=1") as response:
            body = response.read().decode("utf-8")
            content_type = response.headers.get("Content-Type")

        self.assertEqual(content_type, "text/event-stream")
        self.assertIn("event: stream-open", body)
        self.assertIn("event: control-state", body)
        self.assertIn("HTTP Stream Mission", body)
        self.assertIn("mesh.mission.launched", body)

    def test_control_stream_once_closes_cleanly_on_real_http_handler(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.launch_mission(
            title="Real Handler Stream Mission",
            intent="Verify one-shot SSE closes cleanly",
            request_id="control-stream-real-handler",
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('real handler mission')"},
            },
        )
        httpd = server.build_http_server(alpha.mesh, host="127.0.0.1", port=0)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{port}/mesh/control/stream?since=0&limit=20&once=1") as response:
                body = response.read().decode("utf-8")
                content_type = response.headers.get("Content-Type")
            self.assertEqual(content_type, "text/event-stream")
            self.assertIn("event: stream-open", body)
            self.assertIn("event: control-state", body)
            self.assertIn("Real Handler Stream Mission", body)
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=5)

    def test_client_disconnect_helper_matches_common_socket_errors(self):
        self.assertTrue(server._is_client_disconnect(ConnectionResetError()))
        self.assertTrue(server._is_client_disconnect(BrokenPipeError()))
        self.assertTrue(server._is_client_disconnect(OSError(54, "Connection reset by peer")))
        self.assertFalse(server._is_client_disconnect(RuntimeError("boom")))

    def test_mission_launch_wraps_local_job_and_tracks_completion(self):
        beta = self.make_stack("beta")
        self._register_default_worker(beta)

        mission = beta.mesh.launch_mission(
            title="Local Mission",
            intent="Run one queued job under mission control",
            request_id="mission-local-1",
            priority="high",
            workload_class="cpu_bound",
            continuity={"resumable": True},
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('mission-local')"},
                "artifact_inputs": [],
                "metadata": {
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual"},
                },
            },
        )

        self.assertEqual(mission["status"], "waiting")
        self.assertEqual(mission["title"], "Local Mission")
        self.assertEqual(len(mission["child_job_ids"]), 1)
        child_job = beta.mesh.get_job(mission["child_job_ids"][0])
        self.assertEqual(child_job["mission"]["mission_id"], mission["id"])

        executed = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(executed["status"], "completed")

        completed_mission = beta.mesh.get_mission(mission["id"])
        self.assertEqual(completed_mission["status"], "completed")
        self.assertTrue(completed_mission["result_ref"]["id"])
        self.assertTrue(completed_mission["result_bundle_ref"]["id"])
        self.assertEqual(completed_mission["lineage"]["jobs"][0]["id"], child_job["id"])
        self.assertEqual(completed_mission["lineage"]["result_bundle_ref"]["id"], completed_mission["result_bundle_ref"]["id"])

    def test_mission_continuity_overlay_fields_are_normalized_and_preserved(self):
        beta = self.make_stack("beta")

        mission = beta.mesh.launch_mission(
            title="Continuity Overlay Mission",
            intent="Carry explicit continuity metadata",
            request_id="mission-overlay-1",
            continuity={
                "resumable": True,
                "continuity_class": "durable",
                "lineage_ref": "lineage/habitat-alpha",
                "epoch_tolerance": "long_dormancy_ok",
                "dormancy_ok": True,
                "treaty_requirements": ["treaty/health-alliance-v3", "treaty/health-alliance-v3"],
            },
            job={
                "kind": "agent.echo",
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"message": "overlay mission"},
                "artifact_inputs": [],
            },
        )

        stored = beta.mesh.get_mission(mission["id"])
        self.assertEqual(stored["continuity"]["continuity_class"], "durable")
        self.assertEqual(stored["continuity"]["lineage_ref"], "lineage/habitat-alpha")
        self.assertEqual(stored["continuity"]["epoch_tolerance"], "long_dormancy_ok")
        self.assertTrue(stored["continuity"]["dormancy_ok"])
        self.assertEqual(stored["continuity"]["treaty_requirements"], ["treaty/health-alliance-v3"])

    def test_mission_reflects_checkpointed_child_state(self):
        beta = self.make_stack("beta")
        self._register_default_worker(beta)

        mission = beta.mesh.launch_mission(
            title="Checkpoint Mission",
            intent="Hold continuity across failure",
            request_id="mission-checkpoint-1",
            continuity={"resumable": True, "checkpoint_strategy": "manual"},
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('mission-checkpoint')"},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual", "on_retry": False},
                },
            },
        )

        claimed = beta.mesh.claim_next_job("beta-worker", job_id=mission["child_job_ids"][0], ttl_seconds=120)
        failed = beta.mesh.fail_job_attempt(
            claimed["attempt"]["id"],
            error="mission checkpoint failure",
            retryable=False,
            metadata={"checkpoint": {"cursor": 7, "phase": "saved"}},
        )
        self.assertEqual(failed["job"]["status"], "checkpointed")

        checkpointed_mission = beta.mesh.get_mission(mission["id"])
        self.assertEqual(checkpointed_mission["status"], "checkpointed")
        self.assertTrue(checkpointed_mission["continuity"]["checkpoint_ready"])
        self.assertTrue(checkpointed_mission["latest_checkpoint_ref"]["id"])
        self.assertTrue(checkpointed_mission["continuity"]["resumable"])
        self.assertEqual(
            checkpointed_mission["lineage"]["latest_checkpoint_ref"]["id"],
            checkpointed_mission["latest_checkpoint_ref"]["id"],
        )

    def test_mission_continuity_summary_uses_plain_language_and_safe_devices(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_mission(beta, request_id="mission-continuity-summary")
        mission = state["mission"]

        continuity = beta.mesh.get_mission_continuity(mission["id"])

        self.assertEqual(continuity["mission_id"], mission["id"])
        self.assertEqual(continuity["continuity_state"], "ready_to_continue")
        self.assertEqual(continuity["recommended_action"], "resume")
        self.assertEqual(continuity["recommended_action_label"], "Continue Mission")
        self.assertIn("safe checkpoint", continuity["headline"].lower())
        self.assertTrue(continuity["recovery"]["recoverable"])
        self.assertTrue(continuity["artifacts"]["checkpoint"]["available"])
        self.assertTrue(continuity["safe_devices"])
        self.assertEqual(continuity["available_actions"][0]["action"], "resume")
        self.assertTrue(continuity["treaty_validation"]["satisfied"])
        self.assertTrue(continuity["safe_devices"][0]["treaty_capabilities"]["treaty_documents"])
        self.assertIn("advisory_state", continuity["safe_devices"][0]["treaty_compatibility"])
        self.assertIsInstance(continuity["recommended_treaty_device"], dict)

    def test_mission_continuity_summary_surfaces_treaty_audit(self):
        beta = self.make_stack("beta")
        self._register_default_worker(beta)
        mission = beta.mesh.launch_mission(
            title="Treaty Audit Mission",
            intent="Show treaty audit in continuity summary",
            request_id="mission-continuity-treaty-audit",
            continuity={"resumable": True, "checkpoint_strategy": "manual", "treaty_requirements": ["treaty/audit-v1"]},
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('treaty audit mission')"},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual", "on_retry": False},
                },
            },
        )
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=mission["child_job_ids"][0], ttl_seconds=120)
        beta.mesh.fail_job_attempt(
            claimed["attempt"]["id"],
            error="treaty audit checkpoint failure",
            retryable=False,
            metadata={"checkpoint": {"cursor": 31, "phase": "saved"}},
        )

        continuity = beta.mesh.get_mission_continuity(mission["id"])

        self.assertFalse(continuity["treaty_validation"]["satisfied"])
        self.assertEqual(continuity["treaty_validation"]["missing"], ["treaty/audit-v1"])
        self.assertEqual(continuity["governance"]["treaty_requirements"], ["treaty/audit-v1"])
        self.assertEqual(continuity["governance"]["treaty_audit"]["status"], "attention_needed")

    def test_mesh_exposes_mission_service_for_continuity_queries(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_mission(beta, request_id="mission-service-summary")
        mission = state["mission"]

        continuity = beta.mesh.missions.get_mission_continuity(mission["id"])

        self.assertEqual(continuity["mission_id"], mission["id"])
        self.assertEqual(continuity["continuity_state"], "ready_to_continue")
        self.assertEqual(continuity["recommended_action"], "resume")

    def test_mission_service_can_plan_and_export_continuity_vessel(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_mission(beta, request_id="mission-vessel-export")
        mission = state["mission"]

        planned = beta.mesh.missions.export_continuity_vessel(
            mission["id"],
            dry_run=True,
            operator_id="operator-vessel",
            reason="plan continuity export",
        )
        self.assertEqual(planned["status"], "planned")
        self.assertTrue(planned["dry_run"])
        self.assertEqual(planned["mission_id"], mission["id"])
        self.assertEqual(planned["artifact_kind"], "vessel")
        self.assertTrue(planned["vessel"]["continuity"]["checkpoint_ready"])
        self.assertFalse(planned["vessel_ref"])
        self.assertFalse(planned["witness_ref"])

        exported = beta.mesh.missions.export_continuity_vessel(
            mission["id"],
            dry_run=False,
            operator_id="operator-vessel",
            reason="seal continuity export",
        )
        self.assertEqual(exported["status"], "exported")
        self.assertTrue(exported["vessel_ref"]["id"])
        self.assertTrue(exported["witness_ref"]["id"])

        vessel_artifact = beta.mesh.get_artifact(exported["vessel_ref"]["id"])
        vessel_payload = json.loads(base64.b64decode(vessel_artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(vessel_artifact["artifact_kind"], "vessel")
        self.assertEqual(vessel_payload["mission"]["id"], mission["id"])

        witness_artifact = beta.mesh.get_artifact(exported["witness_ref"]["id"])
        witness_payload = json.loads(base64.b64decode(witness_artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(witness_artifact["artifact_kind"], "witness")
        self.assertEqual(witness_payload["subject_artifact_id"], exported["vessel_ref"]["id"])

    def test_mission_service_can_verify_vessel_and_plan_restore(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_mission(beta, request_id="mission-vessel-verify")
        mission = state["mission"]
        exported = beta.mesh.missions.export_continuity_vessel(
            mission["id"],
            dry_run=False,
            operator_id="operator-verify",
            reason="seal for verification",
        )

        verified = beta.mesh.missions.verify_continuity_vessel(exported["vessel_ref"]["id"])
        self.assertEqual(verified["status"], "verified")
        self.assertEqual(verified["mission_id"], mission["id"])
        self.assertTrue(verified["witnesses"])
        self.assertTrue(verified["artifact_availability"]["checkpoint_ref"]["available"])

        restore_plan = beta.mesh.missions.plan_continuity_restore(
            exported["vessel_ref"]["id"],
            operator_id="operator-restore-plan",
            reason="dry-run restore",
        )
        self.assertEqual(restore_plan["status"], "ready")
        self.assertTrue(restore_plan["dry_run"])
        self.assertEqual(restore_plan["mission_id"], mission["id"])
        self.assertEqual(restore_plan["recommended_action"], "resume")
        self.assertTrue(restore_plan["artifacts"]["checkpoint"]["available"])
        self.assertTrue(restore_plan["artifact_readiness"]["checkpoint"]["available"])
        self.assertEqual(restore_plan["warnings"], [])

    def test_continuity_treaty_validation_surfaces_in_export_verify_and_restore(self):
        beta = self.make_stack("beta")
        self._register_default_worker(beta)
        mission = beta.mesh.launch_mission(
            title="Treaty Continuity Mission",
            intent="Test treaty-aware continuity export",
            request_id="mission-vessel-treaty",
            continuity={
                "resumable": True,
                "checkpoint_strategy": "manual",
                "treaty_requirements": ["treaty/storage-v1"],
            },
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('treaty continuity mission')"},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual", "on_retry": False},
                },
            },
        )
        claimed = beta.mesh.claim_next_job("beta-worker", job_id=mission["child_job_ids"][0], ttl_seconds=120)
        beta.mesh.fail_job_attempt(
            claimed["attempt"]["id"],
            error="mission treaty checkpoint failure",
            retryable=False,
            metadata={"checkpoint": {"cursor": 21, "phase": "saved"}},
        )

        planned = beta.mesh.missions.export_continuity_vessel(mission["id"], dry_run=True)
        self.assertFalse(planned["treaty_validation"]["satisfied"])
        self.assertEqual(planned["treaty_validation"]["missing"], ["treaty/storage-v1"])

        with self.assertRaises(MeshPolicyError):
            beta.mesh.missions.export_continuity_vessel(mission["id"], dry_run=False)

        beta.mesh.propose_treaty(
            treaty_id="treaty/storage-v1",
            title="Storage Continuity Treaty",
            document={"witness_required": True, "artifact_export": "sealed"},
        )
        exported = beta.mesh.missions.export_continuity_vessel(mission["id"], dry_run=False)

        vessel_artifact = beta.mesh.get_artifact(exported["vessel_ref"]["id"])
        vessel_payload = json.loads(base64.b64decode(vessel_artifact["content_base64"]).decode("utf-8"))
        self.assertEqual(vessel_payload["governance"]["treaty_requirements"], ["treaty/storage-v1"])
        self.assertTrue(vessel_payload["governance"]["treaty_validation"]["satisfied"])
        self.assertEqual(vessel_artifact["metadata"]["treaty_requirements"], ["treaty/storage-v1"])

        verified = beta.mesh.missions.verify_continuity_vessel(exported["vessel_ref"]["id"])
        self.assertEqual(verified["status"], "verified")
        self.assertTrue(verified["treaty_validation"]["satisfied"])

        restore_plan = beta.mesh.missions.plan_continuity_restore(exported["vessel_ref"]["id"])
        self.assertEqual(restore_plan["status"], "ready")
        self.assertTrue(restore_plan["treaty_validation"]["satisfied"])
        self.assertTrue(restore_plan["governance"]["treaty_validation"]["satisfied"])
        self.assertTrue(restore_plan["recommended_treaty_device"])

    def test_treaty_bound_restore_requires_custody_review_capable_target(self):
        beta = self.make_stack(
            "beta-light",
            device_profile={
                "device_class": "full",
                "execution_tier": "standard",
                "network_profile": "broadband",
                "approval_capable": True,
                "secure_secret_capable": False,
            },
        )
        self._register_default_worker(beta, worker_id="beta-light-worker")
        mission = beta.mesh.launch_mission(
            title="Treaty Restore Mission",
            intent="Block restore on non-custody-capable target",
            request_id="mission-vessel-treaty-custody",
            continuity={
                "resumable": True,
                "checkpoint_strategy": "manual",
                "treaty_requirements": ["treaty/custody-v1"],
            },
            job={
                "kind": "python.inline",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["python"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"code": "print('treaty custody mission')"},
                "artifact_inputs": [],
                "metadata": {
                    "retry_policy": {"max_attempts": 1},
                    "resumability": {"enabled": True},
                    "checkpoint_policy": {"enabled": True, "mode": "manual", "on_retry": False},
                },
            },
        )
        claimed = beta.mesh.claim_next_job("beta-light-worker", job_id=mission["child_job_ids"][0], ttl_seconds=120)
        beta.mesh.fail_job_attempt(
            claimed["attempt"]["id"],
            error="mission custody checkpoint failure",
            retryable=False,
            metadata={"checkpoint": {"cursor": 22, "phase": "saved"}},
        )
        beta.mesh.propose_treaty(
            treaty_id="treaty/custody-v1",
            title="Custody Continuity Treaty",
            document={"witness_required": True, "artifact_export": "sealed"},
        )
        exported = beta.mesh.missions.export_continuity_vessel(mission["id"], dry_run=False)

        restore_plan = beta.mesh.missions.plan_continuity_restore(exported["vessel_ref"]["id"])
        self.assertEqual(restore_plan["status"], "blocked")
        self.assertTrue(any("custody review" in blocker.lower() for blocker in restore_plan["blockers"]))

    def test_mission_resume_latest_recovers_checkpointed_child_job(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_mission(beta, request_id="mission-resume-latest")
        mission = state["mission"]

        resumed = beta.mesh.resume_mission(
            mission["id"],
            operator_id="operator-mission",
            reason="resume mission latest",
        )
        self.assertEqual(resumed["mission"]["metadata"]["last_control_action"], "resume_latest")
        self.assertEqual(len(resumed["jobs"]), 1)
        self.assertEqual(resumed["jobs"][0]["status"], "retry_wait")

        executed = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(executed["status"], "completed")
        completed_mission = beta.mesh.get_mission(mission["id"])
        self.assertEqual(completed_mission["status"], "completed")

    def test_mission_resume_from_checkpoint_supports_single_child_override(self):
        beta = self.make_stack("beta")
        state = self._checkpointed_mission(beta, request_id="mission-resume-explicit")
        mission = state["mission"]
        child_job = beta.mesh.get_job(mission["child_job_ids"][0])
        alternate_checkpoint = beta.mesh.publish_local_artifact(
            {"cursor": 99, "phase": "manual-override"},
            media_type="application/json",
            policy=child_job["policy"],
            metadata={"artifact_kind": "checkpoint", "job_id": child_job["id"], "retention_class": "durable"},
        )

        resumed = beta.mesh.resume_mission_from_checkpoint(
            mission["id"],
            operator_id="operator-mission",
            reason="resume mission checkpoint",
            checkpoint_artifact_id=alternate_checkpoint["id"],
        )
        self.assertEqual(resumed["mission"]["metadata"]["last_control_action"], "resume_checkpoint")
        self.assertEqual(len(resumed["jobs"]), 1)

        executed = beta.mesh.run_worker_once("beta-worker")
        self.assertEqual(executed["status"], "completed")
        completed_job = beta.mesh.get_job(child_job["id"])
        self.assertEqual(
            completed_job["attempts"][1]["metadata"]["resumed_from_checkpoint_ref"]["id"],
            alternate_checkpoint["id"],
        )

    def test_mission_wraps_cooperative_task_launch(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")

        mission = alpha.mesh.launch_mission(
            title="Cooperative Mission",
            intent="Launch a distributed cooperative task under one mission",
            request_id="mission-coop-1",
            priority="high",
            target_strategy="cooperative_spread",
            cooperative_task={
                "name": "mission-coop-task",
                "strategy": "spread",
                "target_peer_ids": ["alpha-node", "beta-node"],
                "base_job": {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", "print('mission-base')"]},
                    "artifact_inputs": [],
                    "metadata": {"workload_class": "mixed"},
                },
                "shards": [
                    {"label": "local", "payload": {"command": [sys.executable, "-c", "print('mission-local')"]}},
                    {"label": "remote", "payload": {"command": [sys.executable, "-c", "print('mission-remote')"]}},
                ],
            },
        )

        self.assertEqual(mission["status"], "waiting")
        self.assertEqual(len(mission["cooperative_task_ids"]), 1)
        self.assertEqual(len(mission["child_job_ids"]), 2)
        self.assertEqual(mission["summary"]["cooperative_task_count"], 1)
        self.assertEqual(mission["summary"]["job_count"], 2)
        self.assertEqual(
            {job["mission"]["mission_id"] for job in mission["child_jobs"] if job.get("mission")},
            {mission["id"]},
        )
        self.assertEqual(len(mission["lineage"]["cooperative_tasks"]), 1)

    def test_cooperative_task_endpoints_are_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        alpha_client, _ = self.serve_mesh(alpha)
        _, beta_base_url = self.serve_mesh(beta)

        alpha_client.seek_peers({"base_urls": [beta_base_url], "auto_connect": True, "trust_tier": "trusted"})
        launched = alpha_client.launch_cooperative_task(
            {
                "name": "http-cooperative",
                "request_id": "http-cooperative-1",
                "target_peer_ids": ["alpha-node", "beta-node"],
                "base_job": {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", "print('base-http')"]},
                    "artifact_inputs": [],
                },
                "shards": [
                    {"label": "local", "payload": {"command": [sys.executable, "-c", "print('local-http')"]}},
                    {"label": "remote", "payload": {"command": [sys.executable, "-c", "print('remote-http')"]}},
                ],
            }
        )
        self.assertEqual(launched["shard_count"], 2)
        task_id = launched["id"]

        listed = alpha_client.list_cooperative_tasks(limit=10)
        self.assertEqual(listed["count"], 1)
        fetched = alpha_client.get_cooperative_task(task_id)
        self.assertEqual(fetched["id"], task_id)
        self.assertEqual({child["peer_id"] for child in fetched["children"]}, {"alpha-node", "beta-node"})

    def test_mission_endpoints_are_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        self._register_default_worker(alpha, worker_id="alpha-http-worker")
        alpha_client, _ = self.serve_mesh(alpha)

        launched = alpha_client.launch_mission(
            {
                "title": "HTTP Mission",
                "intent": "Round-trip mission APIs",
                "request_id": "http-mission-1",
                "priority": "high",
                "continuity": {"resumable": True},
                "job": {
                    "kind": "python.inline",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"code": "print('http mission')"},
                    "metadata": {
                        "resumability": {"enabled": True},
                        "checkpoint_policy": {"enabled": True, "mode": "manual"},
                    },
                },
            }
        )
        mission_id = launched["id"]
        self.assertEqual(launched["status"], "waiting")

        listed = alpha_client.list_missions(limit=10)
        self.assertEqual(listed["count"], 1)
        fetched = alpha_client.get_mission(mission_id)
        self.assertEqual(fetched["id"], mission_id)
        continuity = alpha_client.get_mission_continuity(mission_id)
        self.assertEqual(continuity["mission_id"], mission_id)
        self.assertTrue(continuity["available_actions"])
        export_plan = alpha_client.export_mission_continuity_vessel(
            mission_id,
            {"dry_run": True, "operator_id": "operator-http", "reason": "http export plan"},
        )
        self.assertEqual(export_plan["status"], "planned")
        self.assertTrue(export_plan["dry_run"])
        self.assertEqual(export_plan["artifact_kind"], "vessel")
        self.assertEqual(export_plan["mission_id"], mission_id)
        exported = alpha_client.export_mission_continuity_vessel(
            mission_id,
            {"dry_run": False, "operator_id": "operator-http", "reason": "http seal export"},
        )
        self.assertEqual(exported["status"], "exported")
        verified = alpha_client.verify_continuity_vessel({"artifact_id": exported["vessel_ref"]["id"]})
        self.assertEqual(verified["status"], "verified")
        restore_plan = alpha_client.plan_continuity_restore(
            {
                "artifact_id": exported["vessel_ref"]["id"],
                "operator_id": "operator-http",
                "reason": "http restore plan",
            }
        )
        self.assertEqual(restore_plan["status"], "ready")
        self.assertEqual(restore_plan["mission_id"], mission_id)

        cancelled = alpha_client.cancel_mission(mission_id, reason="http cancel")
        self.assertEqual(cancelled["mission"]["id"], mission_id)
        self.assertEqual(cancelled["mission"]["status"], "cancelled")

    def test_mission_recovery_endpoints_are_exposed_over_http(self):
        alpha = self.make_stack("alpha")
        state = self._checkpointed_mission(alpha, worker_id="alpha-http-worker", request_id="http-mission-recovery")
        alpha_client, _ = self.serve_mesh(alpha)
        mission_id = state["mission"]["id"]

        resumed = alpha_client.resume_mission(mission_id, reason="http mission resume")
        self.assertEqual(resumed["mission"]["metadata"]["last_control_action"], "resume_latest")
        executed = alpha.mesh.run_worker_once("alpha-http-worker")
        self.assertEqual(executed["status"], "completed")

        state = self._checkpointed_mission(alpha, worker_id="alpha-http-worker", request_id="http-mission-checkpoint")
        mission_id = state["mission"]["id"]
        resumed_checkpoint = alpha_client.resume_mission_from_checkpoint(
            mission_id,
            reason="http mission checkpoint",
        )
        self.assertEqual(resumed_checkpoint["mission"]["metadata"]["last_control_action"], "resume_checkpoint")

    def test_server_mission_handlers_round_trip(self):
        alpha = self.make_stack("alpha")
        self._register_default_worker(alpha, worker_id="alpha-probe-worker")
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_mission_launch(
            {
                "title": "Probe Mission",
                "intent": "Launch through server handlers",
                "request_id": "probe-mission-1",
                "job": {
                    "kind": "python.inline",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["python"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"code": "print('probe mission')"},
                },
            }
        )
        self.assertEqual(probe.code, 200)
        mission_id = probe.payload["id"]

        probe = ProbeHandler()
        probe._handle_mesh_missions({"limit": ["10"], "status": [""]})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)

        probe = ProbeHandler()
        probe._handle_mesh_mission_continuity_get(f"/mesh/missions/{mission_id}/continuity")
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["mission_id"], mission_id)

        probe = ProbeHandler()
        probe._handle_mesh_mission_get(f"/mesh/missions/{mission_id}")
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["id"], mission_id)

        probe = ProbeHandler()
        probe._handle_mesh_mission_cancel(
            f"/mesh/missions/{mission_id}/cancel",
            {"operator_id": "probe-ui", "reason": "probe cancel"},
        )
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["mission"]["status"], "cancelled")

    def test_server_mission_recovery_handlers_round_trip(self):
        alpha = self.make_stack("alpha")
        state = self._checkpointed_mission(alpha, worker_id="alpha-probe-worker", request_id="probe-mission-recovery")
        mission_id = state["mission"]["id"]
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_mission_resume(
            f"/mesh/missions/{mission_id}/resume",
            {"operator_id": "probe-ui", "reason": "probe resume"},
        )
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["mission"]["metadata"]["last_control_action"], "resume_latest")

        state = self._checkpointed_mission(alpha, worker_id="alpha-probe-worker", request_id="probe-mission-recovery-checkpoint")
        mission_id = state["mission"]["id"]
        probe = ProbeHandler()
        probe._handle_mesh_mission_resume_from_checkpoint(
            f"/mesh/missions/{mission_id}/resume-from-checkpoint",
            {"operator_id": "probe-ui", "reason": "probe checkpoint"},
        )
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["mission"]["metadata"]["last_control_action"], "resume_checkpoint")

    def test_server_mesh_worker_handlers_register_and_list_workers(self):
        alpha = self.make_stack("alpha")
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_worker_register(
            {
                "worker_id": "alpha-server-worker",
                "agent_id": alpha.agent_id,
                "capabilities": ["worker-runtime", "shell"],
                "resources": {"cpu": 1},
                "labels": ["server-handler"],
                "max_concurrent_jobs": 1,
            }
        )
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["status"], "ok")
        self.assertEqual(probe.payload["worker"]["id"], "alpha-server-worker")

        probe = ProbeHandler()
        probe._handle_mesh_workers({"limit": ["10"]})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)
        self.assertEqual(probe.payload["workers"][0]["id"], "alpha-server-worker")

    def test_server_mesh_notification_and_approval_handlers_round_trip(self):
        alpha = self.make_stack("alpha")
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_notification_publish(
            {
                "notification_type": "job.summary",
                "priority": "high",
                "title": "Watch review needed",
                "body": "Compact review body",
                "target_peer_id": "watch-node",
                "target_device_classes": ["micro"],
            }
        )
        self.assertEqual(probe.code, 200)
        notification_id = probe.payload["notification"]["id"]

        probe = ProbeHandler()
        probe._handle_mesh_notifications({"limit": ["10"], "target_peer_id": ["watch-node"]})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)

        probe = ProbeHandler()
        probe._handle_mesh_notification_ack(
            f"/mesh/notifications/{notification_id}/ack",
            {"status": "acked", "actor_peer_id": "watch-node"},
        )
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["notification"]["status"], "acked")

        probe = ProbeHandler()
        probe._handle_mesh_approval_request(
            {
                "title": "Approve resume",
                "summary": "Resume on stable relay",
                "action_type": "job.recovery.resume",
                "severity": "high",
                "target_peer_id": "watch-node",
                "target_device_classes": ["micro"],
            }
        )
        self.assertEqual(probe.code, 200)
        approval_id = probe.payload["approval"]["id"]

        probe = ProbeHandler()
        probe._handle_mesh_approvals({"limit": ["10"], "status": ["pending"], "target_peer_id": ["watch-node"]})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)

        probe = ProbeHandler()
        probe._handle_mesh_approval_resolve(
            f"/mesh/approvals/{approval_id}/resolve",
            {"decision": "approved", "operator_peer_id": "watch-node"},
        )
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["approval"]["status"], "approved")

    def test_server_cooperative_task_handlers_round_trip(self):
        alpha = self.make_stack("alpha")
        beta = self.make_stack("beta")
        alpha.mesh.register_worker(
            worker_id="alpha-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_cooperative_task_launch(
            {
                "name": "probe-group",
                "request_id": "probe-group-1",
                "target_peer_ids": ["alpha-node", "beta-node"],
                "base_job": {
                    "kind": "shell.command",
                    "dispatch_mode": "queued",
                    "requirements": {"capabilities": ["shell"]},
                    "policy": {"classification": "trusted", "mode": "batch"},
                    "payload": {"command": [sys.executable, "-c", "print('probe')"]},
                    "artifact_inputs": [],
                },
                "shards": [
                    {"label": "local", "payload": {"command": [sys.executable, "-c", "print('probe-local')"]}},
                    {"label": "remote", "payload": {"command": [sys.executable, "-c", "print('probe-remote')"]}},
                ],
            }
        )
        self.assertEqual(probe.code, 200)
        task_id = probe.payload["id"]

        probe = ProbeHandler()
        probe._handle_mesh_cooperative_tasks({"limit": ["10"], "state": [""]})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)

        probe = ProbeHandler()
        probe._handle_mesh_cooperative_task_get(f"/mesh/cooperative-tasks/{task_id}")
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["id"], task_id)

    def test_server_mesh_secret_handlers_store_and_list_redacted_metadata(self):
        alpha = self.make_stack("alpha")
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_secret_put(
            {
                "name": "api-token",
                "scope": "mesh.ops",
                "value": "server-secret-token",
                "metadata": {"origin": "server-handler"},
            }
        )
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["status"], "ok")
        self.assertEqual(probe.payload["secret"]["name"], "api-token")
        self.assertTrue(probe.payload["secret"]["value_present"])
        self.assertNotIn("value", probe.payload["secret"])

        probe = ProbeHandler()
        probe._handle_mesh_secrets({"limit": ["10"], "scope": ["mesh.ops"]})
        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)
        self.assertEqual(probe.payload["secrets"][0]["scope"], "mesh.ops")
        self.assertEqual(probe.payload["secrets"][0]["name"], "api-token")
        self.assertNotIn("value", probe.payload["secrets"][0])

    def test_server_mesh_queue_handler_lists_durable_messages(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('queue-handler')"]},
                "artifact_inputs": [],
                "metadata": {"dedupe_key": "queue-handler"},
            },
            request_id="queue-handler-job",
        )
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_queue({"limit": ["10"], "status": ["queued"]})

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)
        self.assertEqual(probe.payload["messages"][0]["status"], "queued")
        self.assertEqual(probe.payload["messages"][0]["dedupe_key"], "queue-handler")

    def test_server_mesh_queue_metrics_handler_reports_pressure(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.register_worker(
            worker_id="alpha-server-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        alpha.mesh.submit_local_job(
            {
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('queue-metrics')"]},
                "artifact_inputs": [],
            },
            request_id="queue-metrics-handler",
        )
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_queue_metrics()

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["counts"]["queued"], 1)
        self.assertEqual(probe.payload["workers"]["registered"], 1)

    def test_server_mesh_scheduler_decisions_handler_lists_persisted_rows(self):
        alpha = self.make_stack("alpha")
        alpha.mesh.register_worker(
            worker_id="alpha-server-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
        )
        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('server-handler')"]},
                "artifact_inputs": [],
            },
            request_id="server-handler-decision",
        )
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_scheduler_decisions({"limit": ["10"], "status": ["placed"]})

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["count"], 1)
        self.assertEqual(probe.payload["decisions"][0]["request_id"], "server-handler-decision")

    # ------------------------------------------------------------------
    # GPU modeling + helper enlistment + GPU-aware cooperative tests
    # ------------------------------------------------------------------
    def test_device_profile_normalises_gpu_compute_and_helper_state(self):
        alpha = self.make_stack(
            "alpha-gpu",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {
                    "cpu_cores": 32,
                    "memory_mb": 131072,
                    "gpu_count": 2,
                    "gpu_class": "cuda",
                    "gpu_vram_mb": 24576,
                    "supports_workload_classes": ["gpu_training", "mixed"],
                },
                "helper_state": "active",
                "helper_role": "helper",
            },
        )
        profile = alpha.mesh.device_profile
        compute = profile.get("compute_profile") or {}
        self.assertTrue(compute.get("gpu_capable"))
        self.assertEqual(compute.get("gpu_class"), "cuda")
        self.assertIn("gpu_training", compute.get("supports_workload_classes") or [])
        self.assertIn("gpu", compute.get("compute_tags") or [])
        self.assertIn("large_gpu", compute.get("compute_tags") or [])
        self.assertEqual(profile.get("helper_state"), "active")
        cards = alpha.mesh.capability_cards()
        names = {card.get("name") for card in cards}
        self.assertIn("gpu-runtime", names)
        self.assertIn("helper-enlistment", names)
        gpu_card = next(card for card in cards if card.get("name") == "gpu-runtime")
        self.assertTrue(gpu_card.get("available"))
        self.assertEqual(gpu_card.get("metadata", {}).get("gpu_count"), 2)

    def test_device_profile_normalises_offload_policy(self):
        alpha = self.make_stack(
            "alpha-policy",
            device_profile={
                "device_class": "full",
                "offload_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "pressure_threshold": "saturated",
                    "max_auto_enlist": 3,
                    "allowed_trust_tiers": ["trusted"],
                    "allowed_device_classes": ["full", "relay"],
                    "approval_for_gpu_helpers": False,
                },
            },
        )
        policy = dict(alpha.mesh.device_profile.get("offload_policy") or {})
        self.assertTrue(policy.get("enabled"))
        self.assertEqual(policy.get("mode"), "auto")
        self.assertEqual(policy.get("pressure_threshold"), "saturated")
        self.assertEqual(policy.get("max_auto_enlist"), 3)
        self.assertEqual(policy.get("allowed_trust_tiers"), ["trusted"])
        self.assertFalse(policy.get("approval_for_gpu_helpers"))

    def test_offload_preference_persists_by_peer_and_workload(self):
        alpha = self.make_stack("alpha-pref")
        beta = self.make_stack("beta-pref")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-pref-node", limit=20, refresh_manifest=True)

        stored = alpha.mesh.set_offload_preference(
            "beta-pref-node",
            workload_class="gpu_inference",
            preference="prefer",
            source="operator",
            metadata={"note": "always use this GPU box"},
        )

        self.assertEqual(stored["peer_id"], "beta-pref-node")
        self.assertEqual(stored["workload_class"], "gpu_inference")
        self.assertEqual(stored["preference"], "prefer")
        listed = alpha.mesh.list_offload_preferences(workload_class="gpu_inference")
        self.assertEqual(listed["count"], 1)
        self.assertEqual(listed["preferences"][0]["metadata"]["note"], "always use this GPU box")

    def test_mesh_exposes_helper_service_for_preferences_and_pressure(self):
        alpha = self.make_stack("alpha-helper-service")
        beta = self.make_stack("beta-helper-service")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-helper-service-node", limit=20, refresh_manifest=True)

        stored = alpha.mesh.helpers.set_offload_preference(
            "beta-helper-service-node",
            workload_class="gpu_inference",
            preference="prefer",
            source="service-test",
        )

        self.assertEqual(stored["preference"], "prefer")
        listed = alpha.mesh.list_offload_preferences(workload_class="gpu_inference")
        self.assertEqual(listed["preferences"][0]["source"], "service-test")
        pressure = alpha.mesh.helpers.mesh_pressure()
        self.assertEqual(pressure["peer_id"], alpha.mesh.node_id)
        self.assertIn("pressure", pressure)

    def test_scheduler_requires_gpu_when_job_declares_gpu_required(self):
        alpha = self.make_stack("alpha-ctl")
        cpu_peer = self.make_stack(
            "beta-cpu",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {"cpu_cores": 16, "memory_mb": 32768},
            },
        )
        gpu_peer = self.make_stack(
            "gamma-gpu",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {
                    "cpu_cores": 32,
                    "memory_mb": 65536,
                    "gpu_count": 1,
                    "gpu_class": "cuda",
                    "gpu_vram_mb": 16384,
                },
            },
        )
        cpu_peer.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=cpu_peer.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 4},
        )
        gpu_peer.mesh.register_worker(
            worker_id="gamma-worker",
            agent_id=gpu_peer.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 4},
        )
        _, beta_base_url = self.serve_mesh(cpu_peer)
        _, gamma_base_url = self.serve_mesh(gpu_peer)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gamma_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-cpu-node", limit=20, refresh_manifest=True)
        alpha.mesh.sync_peer("gamma-gpu-node", limit=20, refresh_manifest=True)

        decision = alpha.mesh.select_execution_target(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {
                    "workload_class": "gpu_inference",
                    "gpu_required": True,
                    "min_gpu_vram_mb": 8192,
                },
                "payload": {"command": [sys.executable, "-c", "print('gpu')"]},
                "artifact_inputs": [],
            },
            allow_local=False,
        )
        self.assertEqual(decision["status"], "placed")
        self.assertEqual(decision["selected"]["peer_id"], "gamma-gpu-node")
        cpu_candidate = next(
            item for item in decision["candidates"] if item["peer_id"] == "beta-cpu-node"
        )
        self.assertIn("gpu_required_not_available", cpu_candidate["reasons"])

    def test_helper_enlistment_lifecycle_persists_state(self):
        alpha = self.make_stack("alpha-hub")
        beta = self.make_stack(
            "beta-helper",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {
                    "cpu_cores": 16,
                    "memory_mb": 32768,
                    "gpu_count": 1,
                    "gpu_class": "cuda",
                    "gpu_vram_mb": 16384,
                },
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 4},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-helper-node", limit=20, refresh_manifest=True)

        helpers_before = alpha.mesh.list_helpers()
        self.assertEqual(helpers_before["count"], 1)
        self.assertEqual(helpers_before["helpers"][0]["state"], "unenlisted")

        enlisted = alpha.mesh.enlist_helper(
            "beta-helper-node", mode="on_demand", role="gpu_helper", reason="test_enlist"
        )
        self.assertEqual(enlisted["state"], "enlisted")
        self.assertEqual(enlisted["role"], "gpu_helper")
        self.assertEqual(enlisted["mode"], "on_demand")

        drained = alpha.mesh.drain_helper("beta-helper-node", drain_reason="test_drain")
        self.assertEqual(drained["state"], "draining")
        self.assertEqual(drained["drain_reason"], "test_drain")

        retired = alpha.mesh.retire_helper("beta-helper-node", reason="test_retire")
        self.assertEqual(retired["state"], "unenlisted")
        self.assertEqual(retired["mode"], "idle")

        history = retired["history"]
        self.assertGreaterEqual(len(history), 3)
        self.assertEqual(history[0]["reason"], "test_enlist")
        self.assertEqual(history[-1]["reason"], "test_retire")

    def test_mesh_pressure_flags_saturation_with_no_workers(self):
        alpha = self.make_stack("alpha-pressure")
        alpha.mesh.register_worker(
            worker_id="alpha-pressure-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
            status="busy",
        )
        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('pressure')"]},
                "artifact_inputs": [],
            },
            request_id="pressure-request",
        )
        pressure = alpha.mesh.mesh_pressure()
        self.assertEqual(pressure["pressure"], "saturated")
        self.assertGreaterEqual(pressure["queued"], 1)
        self.assertTrue(pressure["needs_help"])
        self.assertIn("queue_saturated", pressure["reasons"])

    def test_plan_helper_enlistment_prefers_gpu_peer_for_gpu_workload(self):
        alpha = self.make_stack("alpha-plan")
        cpu_peer = self.make_stack(
            "beta-cpu2",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {"cpu_cores": 8, "memory_mb": 16384},
            },
        )
        gpu_peer = self.make_stack(
            "gamma-gpu2",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {
                    "cpu_cores": 16,
                    "memory_mb": 32768,
                    "gpu_count": 1,
                    "gpu_class": "cuda",
                    "gpu_vram_mb": 24576,
                },
            },
        )
        cpu_peer.mesh.register_worker(
            worker_id="beta-w2", agent_id=cpu_peer.agent_id,
            capabilities=["worker-runtime", "shell"], resources={"cpu": 2},
        )
        gpu_peer.mesh.register_worker(
            worker_id="gamma-w2", agent_id=gpu_peer.agent_id,
            capabilities=["worker-runtime", "shell"], resources={"cpu": 2},
        )
        _, cpu_base_url = self.serve_mesh(cpu_peer)
        _, gpu_base_url = self.serve_mesh(gpu_peer)
        alpha.mesh.connect_peer(base_url=cpu_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gpu_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-cpu2-node", limit=20, refresh_manifest=True)
        alpha.mesh.sync_peer("gamma-gpu2-node", limit=20, refresh_manifest=True)

        plan = alpha.mesh.plan_helper_enlistment(
            job={
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {
                    "workload_class": "gpu_training",
                    "gpu_required": True,
                    "min_gpu_vram_mb": 16384,
                },
                "payload": {"command": [sys.executable, "-c", "print('plan')"]},
                "artifact_inputs": [],
            },
        )
        self.assertGreaterEqual(plan["candidate_count"], 1)
        self.assertEqual(plan["candidates"][0]["peer_id"], "gamma-gpu2-node")
        self.assertIn("gpu_capable", plan["candidates"][0]["reasons"])

    def test_run_autonomous_offload_auto_enlists_trusted_helper(self):
        alpha = self.make_stack("alpha-auto")
        alpha.mesh.update_device_profile(
            {
                "offload_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "pressure_threshold": "elevated",
                    "max_auto_enlist": 1,
                    "allowed_trust_tiers": ["trusted"],
                    "allowed_device_classes": ["full"],
                    "approval_trust_tiers": [],
                    "approval_device_classes": [],
                    "approval_for_gpu_helpers": False,
                }
            }
        )
        alpha.mesh.register_worker(
            worker_id="alpha-auto-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
            status="busy",
        )
        beta = self.make_stack("beta-auto")
        beta.mesh.register_worker(
            worker_id="beta-auto-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-auto-node", limit=20, refresh_manifest=True)
        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('auto')"]},
                "artifact_inputs": [],
            },
            request_id="auto-offload-job",
        )

        result = alpha.mesh.run_autonomous_offload(actor_agent_id="test-autonomy")

        self.assertEqual(result["status"], "auto_enlisted")
        self.assertEqual(len(result["auto_seek"]["enlisted"]), 1)
        helpers = alpha.mesh.list_helpers()
        helper = next(item for item in helpers["helpers"] if item["peer_id"] == "beta-auto-node")
        self.assertEqual(helper["state"], "enlisted")

    def test_run_autonomous_offload_requests_approval_and_applies_on_approve(self):
        alpha = self.make_stack("alpha-approval")
        alpha.mesh.update_device_profile(
            {
                "offload_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "pressure_threshold": "elevated",
                    "max_auto_enlist": 1,
                    "allowed_trust_tiers": ["trusted", "partner"],
                    "allowed_device_classes": ["full"],
                    "approval_trust_tiers": ["partner"],
                    "approval_device_classes": [],
                    "approval_for_gpu_helpers": False,
                }
            }
        )
        alpha.mesh.register_worker(
            worker_id="alpha-approval-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
            status="busy",
        )
        beta = self.make_stack("beta-approval")
        beta.mesh.register_worker(
            worker_id="beta-approval-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="partner")
        alpha.mesh.sync_peer("beta-approval-node", limit=20, refresh_manifest=True)
        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('approval')"]},
                "artifact_inputs": [],
            },
            request_id="approval-offload-job",
        )

        result = alpha.mesh.run_autonomous_offload(actor_agent_id="test-autonomy")

        self.assertEqual(result["status"], "approval_requested")
        approval = result["approval"]["approval"]
        self.assertEqual(approval["status"], "pending")

        resolved = alpha.mesh.resolve_approval(
            approval["id"],
            decision="approved",
            operator_peer_id=alpha.mesh.node_id,
            operator_agent_id="test-ui",
            reason="approve_offload",
        )
        self.assertEqual(resolved["status"], "approved")
        self.assertEqual(resolved["automation"]["status"], "applied")
        helpers = alpha.mesh.list_helpers()
        helper = next(item for item in helpers["helpers"] if item["peer_id"] == "beta-approval-node")
        self.assertEqual(helper["state"], "enlisted")
        prefs = alpha.mesh.list_offload_preferences(peer_id="beta-approval-node", workload_class="default")
        self.assertEqual(prefs["preferences"][0]["preference"], "allow")

    def test_autonomous_offload_respects_workload_policy_and_preferences(self):
        alpha = self.make_stack("alpha-pref-policy")
        alpha.mesh.update_device_profile(
            {
                "offload_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "pressure_threshold": "elevated",
                    "max_auto_enlist": 2,
                    "allowed_trust_tiers": ["trusted"],
                    "allowed_device_classes": ["full"],
                    "allowed_workload_classes": ["gpu_inference"],
                    "approval_workload_classes": ["gpu_training"],
                    "approval_trust_tiers": [],
                    "approval_device_classes": [],
                    "approval_for_gpu_helpers": False,
                }
            }
        )
        alpha.mesh.register_worker(
            worker_id="alpha-pref-policy-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
            status="busy",
        )
        beta = self.make_stack(
            "beta-pref-policy",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {
                    "cpu_cores": 16,
                    "memory_mb": 32768,
                    "gpu_count": 1,
                    "gpu_class": "cuda",
                    "gpu_vram_mb": 24576,
                },
            },
        )
        beta.mesh.register_worker(
            worker_id="beta-pref-policy-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-pref-policy-node", limit=20, refresh_manifest=True)
        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('pref-policy')"]},
                "artifact_inputs": [],
            },
            request_id="pref-policy-job",
        )
        alpha.mesh.set_offload_preference(
            "beta-pref-policy-node",
            workload_class="gpu_inference",
            preference="prefer",
            source="operator",
        )

        allowed_eval = alpha.mesh.evaluate_autonomous_offload(
            job={
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"workload_class": "gpu_inference", "gpu_required": True},
                "payload": {"command": [sys.executable, "-c", "print('gpu')"]},
                "artifact_inputs": [],
            }
        )
        self.assertEqual(allowed_eval["decision"], "auto_enlist")
        self.assertEqual(allowed_eval["eligible_candidates"][0]["peer_id"], "beta-pref-policy-node")
        self.assertIn("preference_prefer", allowed_eval["eligible_candidates"][0]["reasons"])

        blocked_eval = alpha.mesh.evaluate_autonomous_offload(
            job={
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"workload_class": "cpu_bound"},
                "payload": {"command": [sys.executable, "-c", "print('cpu')"]},
                "artifact_inputs": [],
            }
        )
        self.assertEqual(blocked_eval["decision"], "noop")
        self.assertIn("workload_not_allowed_by_policy", blocked_eval["reasons"])

    def test_rejected_autonomous_offload_learns_deny_preference(self):
        alpha = self.make_stack("alpha-reject")
        alpha.mesh.update_device_profile(
            {
                "offload_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "pressure_threshold": "elevated",
                    "max_auto_enlist": 1,
                    "allowed_trust_tiers": ["trusted", "partner"],
                    "allowed_device_classes": ["full"],
                    "approval_trust_tiers": ["partner"],
                    "approval_device_classes": [],
                    "approval_for_gpu_helpers": False,
                }
            }
        )
        alpha.mesh.register_worker(
            worker_id="alpha-reject-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
            status="busy",
        )
        beta = self.make_stack("beta-reject")
        beta.mesh.register_worker(
            worker_id="beta-reject-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="partner")
        alpha.mesh.sync_peer("beta-reject-node", limit=20, refresh_manifest=True)
        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('reject')"]},
                "artifact_inputs": [],
            },
            request_id="reject-offload-job",
        )

        result = alpha.mesh.run_autonomous_offload(actor_agent_id="test-autonomy")
        approval = result["approval"]["approval"]
        rejected = alpha.mesh.resolve_approval(
            approval["id"],
            decision="rejected",
            operator_peer_id=alpha.mesh.node_id,
            operator_agent_id="test-ui",
            reason="reject_offload",
        )
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["automation"]["preference"], "deny")
        prefs = alpha.mesh.list_offload_preferences(peer_id="beta-reject-node", workload_class="default")
        self.assertEqual(prefs["preferences"][0]["preference"], "deny")

    def test_cooperative_task_places_gpu_shard_on_gpu_helper(self):
        alpha = self.make_stack("alpha-coop")
        alpha.mesh.register_worker(
            worker_id="alpha-coop-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        cpu_peer = self.make_stack(
            "beta-coop-cpu",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {"cpu_cores": 16, "memory_mb": 32768},
            },
        )
        gpu_peer = self.make_stack(
            "gamma-coop-gpu",
            device_profile={
                "device_class": "full",
                "execution_tier": "heavy",
                "compute_profile": {
                    "cpu_cores": 16,
                    "memory_mb": 65536,
                    "gpu_count": 2,
                    "gpu_class": "cuda",
                    "gpu_vram_mb": 24576,
                },
            },
        )
        cpu_peer.mesh.register_worker(
            worker_id="beta-coop-w",
            agent_id=cpu_peer.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        gpu_peer.mesh.register_worker(
            worker_id="gamma-coop-w",
            agent_id=gpu_peer.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        _, cpu_base_url = self.serve_mesh(cpu_peer)
        _, gpu_base_url = self.serve_mesh(gpu_peer)
        alpha.mesh.connect_peer(base_url=cpu_base_url, trust_tier="trusted")
        alpha.mesh.connect_peer(base_url=gpu_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-coop-cpu-node", limit=20, refresh_manifest=True)
        alpha.mesh.sync_peer("gamma-coop-gpu-node", limit=20, refresh_manifest=True)

        task = alpha.mesh.launch_cooperative_task(
            name="gpu-aware",
            request_id="gpu-aware-coop-1",
            strategy="gpu-aware",
            target_peer_ids=["alpha-coop-node", "beta-coop-cpu-node", "gamma-coop-gpu-node"],
            base_job={
                "kind": "shell.command",
                "dispatch_mode": "queued",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('coop')"]},
                "artifact_inputs": [],
            },
            shards=[
                {
                    "label": "gpu-shard",
                    "placement": {
                        "workload_class": "gpu_inference",
                        "gpu_required": True,
                        "min_gpu_vram_mb": 8192,
                    },
                    "payload": {"command": [sys.executable, "-c", "print('gpu')"]},
                },
                {
                    "label": "cpu-shard",
                    "placement": {"workload_class": "cpu_bound"},
                    "payload": {"command": [sys.executable, "-c", "print('cpu')"]},
                },
            ],
        )
        shards_by_label = {child["label"]: child for child in task["children"]}
        self.assertEqual(shards_by_label["gpu-shard"]["peer_id"], "gamma-coop-gpu-node")
        self.assertTrue(shards_by_label["gpu-shard"]["placement"]["target_gpu_capable"])
        self.assertNotEqual(shards_by_label["cpu-shard"]["peer_id"], "gamma-coop-gpu-node")

    def test_server_helpers_endpoints_round_trip_enlistment(self):
        alpha = self.make_stack("alpha-srv-helpers")
        beta = self.make_stack("beta-srv-helpers")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-srv-helpers-node", limit=20, refresh_manifest=True)
        server.server_context["mesh"] = alpha.mesh

        probe = ProbeHandler()
        probe._handle_mesh_helpers({"limit": ["10"]})
        self.assertEqual(probe.code, 200)
        self.assertGreaterEqual(probe.payload["count"], 1)

        enlist_probe = ProbeHandler()
        enlist_probe._handle_mesh_helpers_enlist({"peer_id": "beta-srv-helpers-node", "role": "helper"})
        self.assertEqual(enlist_probe.code, 200)
        self.assertEqual(enlist_probe.payload["state"], "enlisted")

        pressure_probe = ProbeHandler()
        pressure_probe._handle_mesh_pressure()
        self.assertEqual(pressure_probe.code, 200)
        self.assertIn("pressure", pressure_probe.payload)

        retire_probe = ProbeHandler()
        retire_probe._handle_mesh_helpers_retire({"peer_id": "beta-srv-helpers-node"})
        self.assertEqual(retire_probe.code, 200)
        self.assertEqual(retire_probe.payload["state"], "unenlisted")

    def test_server_helpers_autonomy_endpoints_round_trip(self):
        alpha = self.make_stack("alpha-srv-auto")
        alpha.mesh.update_device_profile(
            {
                "offload_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "pressure_threshold": "elevated",
                    "max_auto_enlist": 1,
                    "allowed_trust_tiers": ["trusted"],
                    "allowed_device_classes": ["full"],
                    "approval_trust_tiers": [],
                    "approval_device_classes": [],
                    "approval_for_gpu_helpers": False,
                }
            }
        )
        alpha.mesh.register_worker(
            worker_id="alpha-srv-auto-worker",
            agent_id=alpha.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 1},
            status="busy",
        )
        beta = self.make_stack("beta-srv-auto")
        beta.mesh.register_worker(
            worker_id="beta-srv-auto-worker",
            agent_id=beta.agent_id,
            capabilities=["worker-runtime", "shell"],
            resources={"cpu": 2},
        )
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-srv-auto-node", limit=20, refresh_manifest=True)
        alpha.mesh.schedule_job(
            {
                "kind": "shell.command",
                "requirements": {"capabilities": ["shell"]},
                "policy": {"classification": "trusted", "mode": "batch"},
                "dispatch_mode": "queued",
                "placement": {"queue_class": "batch"},
                "payload": {"command": [sys.executable, "-c", "print('srv-auto')"]},
                "artifact_inputs": [],
            },
            request_id="srv-auto-job",
        )
        server.server_context["mesh"] = alpha.mesh

        eval_probe = ProbeHandler()
        eval_probe._handle_mesh_helpers_autonomy()
        self.assertEqual(eval_probe.code, 200)
        self.assertIn(eval_probe.payload["decision"], {"auto_enlist", "request_approval", "suggest", "noop"})

        run_probe = ProbeHandler()
        run_probe._handle_mesh_helpers_autonomy_run({"actor_agent_id": "test-ui"})
        self.assertEqual(run_probe.code, 200)
        self.assertEqual(run_probe.payload["status"], "auto_enlisted")

    def test_server_helper_preferences_endpoints_round_trip(self):
        alpha = self.make_stack("alpha-srv-pref")
        beta = self.make_stack("beta-srv-pref")
        _, beta_base_url = self.serve_mesh(beta)
        alpha.mesh.connect_peer(base_url=beta_base_url, trust_tier="trusted")
        alpha.mesh.sync_peer("beta-srv-pref-node", limit=20, refresh_manifest=True)
        server.server_context["mesh"] = alpha.mesh

        set_probe = ProbeHandler()
        set_probe._handle_mesh_helpers_preferences_set(
            {
                "peer_id": "beta-srv-pref-node",
                "workload_class": "gpu_inference",
                "preference": "prefer",
                "source": "operator",
            }
        )
        self.assertEqual(set_probe.code, 200)
        self.assertEqual(set_probe.payload["preference"], "prefer")

        list_probe = ProbeHandler()
        list_probe._handle_mesh_helpers_preferences({"limit": ["10"], "workload_class": ["gpu_inference"]})
        self.assertEqual(list_probe.code, 200)
        self.assertEqual(list_probe.payload["count"], 1)


if __name__ == "__main__":
    unittest.main()
