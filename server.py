"""
Standalone HTTP host for the Sovereign Mesh OCP reference implementation.
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from mesh import SovereignMesh
from runtime import OCPRegistry, OCPStore

server_context = {
    "mesh": None,
    "runtime": None,
    "ready": False,
}


class OCPHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

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

    def _handle_mesh_manifest(self):
        self._send_json(self._mesh().get_manifest())

    def _handle_mesh_device_profile(self):
        self._send_json({"status": "ok", "device_profile": dict(self._mesh().device_profile)})

    def _handle_mesh_device_profile_update(self, data):
        self._send_json(self._mesh().update_device_profile(dict(data.get("device_profile") or {})))

    def _handle_mesh_peers(self, params):
        self._send_json(self._mesh().list_peers(limit=int(params.get("limit", ["25"])[0])))

    def _handle_mesh_peers_sync(self, data):
        self._send_json(
            self._mesh().sync_peer(
                (data.get("peer_id") or "").strip(),
                limit=int(data.get("limit") or 50),
            )
        )

    def _handle_mesh_stream(self, params):
        self._send_json(
            self._mesh().stream_snapshot(
                since_seq=int(params.get("since", ["0"])[0]),
                limit=int(params.get("limit", ["50"])[0]),
            )
        )

    def _handle_mesh_handshake(self, data):
        self._send_json(self._mesh().accept_handshake(data))

    def _handle_mesh_lease_acquire(self, data):
        self._send_json(self._mesh().acquire_lease(**dict(data or {})))

    def _handle_mesh_lease_heartbeat(self, data):
        self._send_json(self._mesh().heartbeat_lease(**dict(data or {})))

    def _handle_mesh_lease_release(self, data):
        self._send_json(self._mesh().release_lease(**dict(data or {})))

    def _handle_mesh_job_submit(self, data):
        self._send_json(self._mesh().accept_job_submission(data))

    def _handle_mesh_job_schedule(self, data):
        self._send_json(
            self._mesh().schedule_job(
                dict(data.get("job") or {}),
                request_id=(data.get("request_id") or "").strip() or None,
                preferred_peer_id=(data.get("preferred_peer_id") or "").strip(),
                allow_local=bool(data.get("allow_local", True)),
                allow_remote=bool(data.get("allow_remote", True)),
            )
        )

    def _handle_mesh_job_get(self, path: str):
        self._send_json(self._mesh().get_job(path.split("/mesh/jobs/", 1)[1]))

    def _handle_mesh_job_resume(self, path: str, data):
        job_id = path[len("/mesh/jobs/"):-len("/resume")].strip("/")
        self._send_json(
            self._mesh().resume_job(
                job_id,
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "operator_resume_latest").strip(),
            )
        )

    def _handle_mesh_job_resume_from_checkpoint(self, path: str, data):
        job_id = path[len("/mesh/jobs/"):-len("/resume-from-checkpoint")].strip("/")
        self._send_json(
            self._mesh().resume_job_from_checkpoint(
                job_id,
                checkpoint_artifact_id=(data.get("checkpoint_artifact_id") or "").strip(),
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "operator_resume_checkpoint").strip(),
            )
        )

    def _handle_mesh_job_restart(self, path: str, data):
        job_id = path[len("/mesh/jobs/"):-len("/restart")].strip("/")
        self._send_json(
            self._mesh().restart_job(
                job_id,
                operator_id=(data.get("operator_id") or "").strip(),
                reason=(data.get("reason") or "operator_restart").strip(),
            )
        )

    def _handle_mesh_workers(self, params):
        self._send_json(self._mesh().list_workers(limit=int(params.get("limit", ["25"])[0])))

    def _handle_mesh_notifications(self, params):
        self._send_json(
            self._mesh().list_notifications(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_peer_id=params.get("target_peer_id", [""])[0],
                target_agent_id=params.get("target_agent_id", [""])[0],
            )
        )

    def _handle_mesh_notification_publish(self, data):
        self._send_json(
            {
                "status": "ok",
                "notification": self._mesh().publish_notification(
                    notification_type=(data.get("notification_type") or "info").strip(),
                    priority=(data.get("priority") or "normal").strip(),
                    title=(data.get("title") or "").strip(),
                    body=(data.get("body") or "").strip(),
                    compact_title=(data.get("compact_title") or "").strip(),
                    compact_body=(data.get("compact_body") or "").strip(),
                    target_peer_id=(data.get("target_peer_id") or "").strip(),
                    target_agent_id=(data.get("target_agent_id") or "").strip(),
                    target_device_classes=list(data.get("target_device_classes") or []),
                    related_job_id=(data.get("related_job_id") or "").strip(),
                    related_approval_id=(data.get("related_approval_id") or "").strip(),
                    metadata=dict(data.get("metadata") or {}),
                ),
            }
        )

    def _handle_mesh_notification_ack(self, path: str, data):
        notification_id = path[len("/mesh/notifications/"):-len("/ack")].strip("/")
        self._send_json(
            {
                "status": "ok",
                "notification": self._mesh().ack_notification(
                    notification_id,
                    status=(data.get("status") or "acked").strip(),
                    actor_peer_id=(data.get("actor_peer_id") or "").strip(),
                    actor_agent_id=(data.get("actor_agent_id") or "").strip(),
                    reason=(data.get("reason") or "").strip(),
                ),
            }
        )

    def _handle_mesh_approvals(self, params):
        self._send_json(
            self._mesh().list_approvals(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_peer_id=params.get("target_peer_id", [""])[0],
                target_agent_id=params.get("target_agent_id", [""])[0],
            )
        )

    def _handle_mesh_approval_request(self, data):
        self._send_json(
            self._mesh().create_approval_request(
                title=(data.get("title") or "").strip(),
                summary=(data.get("summary") or "").strip(),
                action_type=(data.get("action_type") or "operator_action").strip(),
                severity=(data.get("severity") or "normal").strip(),
                request_id=(data.get("request_id") or "").strip(),
                requested_by_peer_id=(data.get("requested_by_peer_id") or "").strip(),
                requested_by_agent_id=(data.get("requested_by_agent_id") or "").strip(),
                target_peer_id=(data.get("target_peer_id") or "").strip(),
                target_agent_id=(data.get("target_agent_id") or "").strip(),
                target_device_classes=list(data.get("target_device_classes") or []),
                related_job_id=(data.get("related_job_id") or "").strip(),
                expires_at=(data.get("expires_at") or "").strip(),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_approval_resolve(self, path: str, data):
        approval_id = path[len("/mesh/approvals/"):-len("/resolve")].strip("/")
        self._send_json(
            self._mesh().resolve_approval(
                approval_id,
                decision=(data.get("decision") or "").strip(),
                operator_peer_id=(data.get("operator_peer_id") or "").strip(),
                operator_agent_id=(data.get("operator_agent_id") or "").strip(),
                reason=(data.get("reason") or "").strip(),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_secrets(self, params):
        self._send_json(
            self._mesh().list_secrets(
                limit=int(params.get("limit", ["25"])[0]),
                scope=params.get("scope", [""])[0],
            )
        )

    def _handle_mesh_secret_put(self, data):
        self._send_json(
            {
                "status": "ok",
                "secret": self._mesh().put_secret(
                    (data.get("name") or "").strip(),
                    data.get("value"),
                    scope=(data.get("scope") or "").strip(),
                    metadata=dict(data.get("metadata") or {}),
                ),
            }
        )

    def _handle_mesh_queue(self, params):
        self._send_json(
            self._mesh().list_queue_messages(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
            )
        )

    def _handle_mesh_queue_events(self, params):
        self._send_json(
            self._mesh().list_queue_events(
                since_seq=int(params.get("since", ["0"])[0]),
                limit=int(params.get("limit", ["50"])[0]),
                queue_message_id=params.get("queue_message_id", [""])[0],
                job_id=params.get("job_id", [""])[0],
            )
        )

    def _handle_mesh_queue_metrics(self):
        self._send_json(self._mesh().queue_metrics())

    def _handle_mesh_queue_replay(self, data):
        self._send_json(
            self._mesh().replay_queue_message(
                queue_message_id=(data.get("queue_message_id") or "").strip(),
                job_id=(data.get("job_id") or "").strip(),
                reason=(data.get("reason") or "operator_replay").strip(),
            )
        )

    def _handle_mesh_queue_ack_deadline(self, data):
        self._send_json(
            {
                "status": "ok",
                "queue_message": self._mesh().set_queue_ack_deadline(
                    queue_message_id=(data.get("queue_message_id") or "").strip(),
                    attempt_id=(data.get("attempt_id") or "").strip(),
                    ttl_seconds=int(data.get("ttl_seconds") or 0),
                    reason=(data.get("reason") or "operator_ack_deadline_update").strip(),
                ),
            }
        )

    def _handle_mesh_scheduler_decisions(self, params):
        self._send_json(
            self._mesh().list_scheduler_decisions(
                limit=int(params.get("limit", ["25"])[0]),
                status=params.get("status", [""])[0],
                target_type=params.get("target_type", [""])[0],
            )
        )

    def _handle_mesh_worker_register(self, data):
        self._send_json(
            {
                "status": "ok",
                "worker": self._mesh().register_worker(
                    worker_id=(data.get("worker_id") or "").strip(),
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

    def _handle_mesh_worker_heartbeat(self, path: str, data):
        worker_id = path[len("/mesh/workers/"):-len("/heartbeat")].strip("/")
        self._send_json(
            {
                "status": "ok",
                "worker": self._mesh().heartbeat_worker(
                    worker_id,
                    status=(data.get("status") or "").strip(),
                    metadata=dict(data.get("metadata") or {}),
                ),
            }
        )

    def _handle_mesh_worker_poll(self, path: str, data):
        worker_id = path[len("/mesh/workers/"):-len("/poll")].strip("/")
        self._send_json(self._mesh().poll_jobs(worker_id, limit=int(data.get("limit") or 10)))

    def _handle_mesh_worker_claim(self, path: str, data):
        worker_id = path[len("/mesh/workers/"):-len("/claim")].strip("/")
        self._send_json(
            self._mesh().claim_next_job(
                worker_id,
                job_id=(data.get("job_id") or "").strip(),
                ttl_seconds=int(data.get("ttl_seconds") or 0),
            )
        )

    def _handle_mesh_attempt_heartbeat(self, path: str, data):
        attempt_id = path[len("/mesh/jobs/attempts/"):-len("/heartbeat")].strip("/")
        self._send_json(
            {
                "status": "ok",
                "attempt": self._mesh().heartbeat_job_attempt(
                    attempt_id,
                    ttl_seconds=int(data.get("ttl_seconds") or 300),
                    metadata=dict(data.get("metadata") or {}),
                ),
            }
        )

    def _handle_mesh_attempt_complete(self, path: str, data):
        attempt_id = path[len("/mesh/jobs/attempts/"):-len("/complete")].strip("/")
        self._send_json(
            self._mesh().complete_job_attempt(
                attempt_id,
                data.get("result"),
                media_type=(data.get("media_type") or "application/json").strip(),
                executor=(data.get("executor") or "").strip(),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_attempt_fail(self, path: str, data):
        attempt_id = path[len("/mesh/jobs/attempts/"):-len("/fail")].strip("/")
        self._send_json(
            self._mesh().fail_job_attempt(
                attempt_id,
                error=(data.get("error") or "job attempt failed").strip(),
                retryable=bool(data.get("retryable", True)),
                metadata=dict(data.get("metadata") or {}),
            )
        )

    def _handle_mesh_job_cancel(self, path: str, data):
        job_id = path[len("/mesh/jobs/"):-len("/cancel")].strip("/")
        self._send_json({"status": "cancelled", "job": self._mesh().cancel_job(job_id, reason=(data.get("reason") or "").strip())})

    def _handle_mesh_artifact_publish(self, data):
        self._send_json(self._mesh().accept_artifact_publish(data))

    def _handle_mesh_artifact_list(self, params):
        self._send_json(
            self._mesh().list_artifacts(
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

    def _handle_mesh_artifact_get(self, path: str, params):
        artifact_id = path.split("/mesh/artifacts/", 1)[1]
        self._send_json(
            self._mesh().get_artifact(
                artifact_id,
                requester_peer_id=params.get("peer_id", [""])[0],
                include_content=params.get("include_content", ["1"])[0] != "0",
            )
        )

    def _handle_mesh_artifact_purge(self, data):
        self._send_json(self._mesh().purge_expired_artifacts(limit=int(data.get("limit") or 100)))

    def _handle_mesh_artifact_replicate(self, data):
        self._send_json(
            self._mesh().replicate_artifact_from_peer(
                (data.get("peer_id") or "").strip(),
                artifact_id=(data.get("artifact_id") or "").strip(),
                digest=(data.get("digest") or "").strip(),
                pin=bool(data.get("pin", False)),
            )
        )

    def _handle_mesh_artifact_replicate_graph(self, data):
        self._send_json(
            self._mesh().replicate_artifact_graph_from_peer(
                (data.get("peer_id") or "").strip(),
                artifact_id=(data.get("artifact_id") or "").strip(),
                digest=(data.get("digest") or "").strip(),
                pin=bool(data.get("pin", False)),
            )
        )

    def _handle_mesh_artifact_pin(self, data):
        self._send_json(
            {
                "status": "ok",
                "artifact": self._mesh().set_artifact_pin(
                    (data.get("artifact_id") or "").strip(),
                    pinned=bool(data.get("pinned", True)),
                    reason=(data.get("reason") or "operator_pin").strip(),
                ),
            }
        )

    def _handle_mesh_artifact_verify_mirror(self, data):
        self._send_json(
            self._mesh().verify_artifact_mirror(
                (data.get("artifact_id") or "").strip(),
                peer_id=(data.get("peer_id") or "").strip(),
                source_artifact_id=(data.get("source_artifact_id") or "").strip(),
                digest=(data.get("digest") or "").strip(),
            )
        )

    def _handle_mesh_handoff(self, data):
        self._send_json(self._mesh().accept_handoff(data))

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        try:
            if path == "/mesh/manifest":
                return self._handle_mesh_manifest()
            if path == "/mesh/device-profile":
                return self._handle_mesh_device_profile()
            if path == "/mesh/peers":
                return self._handle_mesh_peers(params)
            if path == "/mesh/stream":
                return self._handle_mesh_stream(params)
            if path == "/mesh/workers":
                return self._handle_mesh_workers(params)
            if path == "/mesh/notifications":
                return self._handle_mesh_notifications(params)
            if path == "/mesh/approvals":
                return self._handle_mesh_approvals(params)
            if path == "/mesh/secrets":
                return self._handle_mesh_secrets(params)
            if path == "/mesh/queue":
                return self._handle_mesh_queue(params)
            if path == "/mesh/queue/events":
                return self._handle_mesh_queue_events(params)
            if path == "/mesh/queue/metrics":
                return self._handle_mesh_queue_metrics()
            if path == "/mesh/scheduler/decisions":
                return self._handle_mesh_scheduler_decisions(params)
            if path == "/mesh/artifacts":
                return self._handle_mesh_artifact_list(params)
            if path.startswith("/mesh/artifacts/"):
                return self._handle_mesh_artifact_get(path, params)
            if path.startswith("/mesh/jobs/"):
                return self._handle_mesh_job_get(path)
            self._send_json({"error": "unknown endpoint"}, 404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 400)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            data = self._read_json()
            if path == "/mesh/handshake":
                return self._handle_mesh_handshake(data)
            if path == "/mesh/device-profile":
                return self._handle_mesh_device_profile_update(data)
            if path == "/mesh/peers/sync":
                return self._handle_mesh_peers_sync(data)
            if path == "/mesh/lease/acquire":
                return self._handle_mesh_lease_acquire(data)
            if path == "/mesh/lease/heartbeat":
                return self._handle_mesh_lease_heartbeat(data)
            if path == "/mesh/lease/release":
                return self._handle_mesh_lease_release(data)
            if path == "/mesh/jobs/submit":
                return self._handle_mesh_job_submit(data)
            if path == "/mesh/jobs/schedule":
                return self._handle_mesh_job_schedule(data)
            if path.startswith("/mesh/jobs/") and path.endswith("/resume-from-checkpoint"):
                return self._handle_mesh_job_resume_from_checkpoint(path, data)
            if path.startswith("/mesh/jobs/") and path.endswith("/resume"):
                return self._handle_mesh_job_resume(path, data)
            if path.startswith("/mesh/jobs/") and path.endswith("/restart"):
                return self._handle_mesh_job_restart(path, data)
            if path.startswith("/mesh/jobs/") and path.endswith("/cancel"):
                return self._handle_mesh_job_cancel(path, data)
            if path == "/mesh/workers/register":
                return self._handle_mesh_worker_register(data)
            if path == "/mesh/notifications/publish":
                return self._handle_mesh_notification_publish(data)
            if path.startswith("/mesh/notifications/") and path.endswith("/ack"):
                return self._handle_mesh_notification_ack(path, data)
            if path == "/mesh/approvals/request":
                return self._handle_mesh_approval_request(data)
            if path.startswith("/mesh/approvals/") and path.endswith("/resolve"):
                return self._handle_mesh_approval_resolve(path, data)
            if path == "/mesh/secrets/put":
                return self._handle_mesh_secret_put(data)
            if path.startswith("/mesh/workers/") and path.endswith("/heartbeat"):
                return self._handle_mesh_worker_heartbeat(path, data)
            if path.startswith("/mesh/workers/") and path.endswith("/poll"):
                return self._handle_mesh_worker_poll(path, data)
            if path.startswith("/mesh/workers/") and path.endswith("/claim"):
                return self._handle_mesh_worker_claim(path, data)
            if path == "/mesh/queue/replay":
                return self._handle_mesh_queue_replay(data)
            if path == "/mesh/queue/ack-deadline":
                return self._handle_mesh_queue_ack_deadline(data)
            if path.startswith("/mesh/jobs/attempts/") and path.endswith("/heartbeat"):
                return self._handle_mesh_attempt_heartbeat(path, data)
            if path.startswith("/mesh/jobs/attempts/") and path.endswith("/complete"):
                return self._handle_mesh_attempt_complete(path, data)
            if path.startswith("/mesh/jobs/attempts/") and path.endswith("/fail"):
                return self._handle_mesh_attempt_fail(path, data)
            if path == "/mesh/artifacts/publish":
                return self._handle_mesh_artifact_publish(data)
            if path == "/mesh/artifacts/replicate":
                return self._handle_mesh_artifact_replicate(data)
            if path == "/mesh/artifacts/replicate-graph":
                return self._handle_mesh_artifact_replicate_graph(data)
            if path == "/mesh/artifacts/pin":
                return self._handle_mesh_artifact_pin(data)
            if path == "/mesh/artifacts/verify-mirror":
                return self._handle_mesh_artifact_verify_mirror(data)
            if path == "/mesh/artifacts/purge":
                return self._handle_mesh_artifact_purge(data)
            if path == "/mesh/agents/handoff":
                return self._handle_mesh_handoff(data)
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
        base_url=args.base_url.rstrip("/") if args.base_url else f"http://{args.host}:{args.port}",
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
