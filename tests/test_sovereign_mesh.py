import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

import server
from mesh import (
    MeshArtifactAccessError,
    MeshPeerClient,
    MeshPolicyError,
    MeshReplayError,
    MeshSignatureError,
    SovereignMesh,
)
from runtime import OCPRegistry, OCPStore


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

    def _send_json(self, data, code=200):
        self.payload = data
        self.code = code


ProbeHandler._mesh = server.OCPHandler._mesh
ProbeHandler._handle_mesh_manifest = server.OCPHandler._handle_mesh_manifest
ProbeHandler._handle_mesh_device_profile = server.OCPHandler._handle_mesh_device_profile
ProbeHandler._handle_mesh_device_profile_update = server.OCPHandler._handle_mesh_device_profile_update
ProbeHandler._handle_mesh_peers_sync = server.OCPHandler._handle_mesh_peers_sync
ProbeHandler._handle_mesh_workers = server.OCPHandler._handle_mesh_workers
ProbeHandler._handle_mesh_notifications = server.OCPHandler._handle_mesh_notifications
ProbeHandler._handle_mesh_notification_publish = server.OCPHandler._handle_mesh_notification_publish
ProbeHandler._handle_mesh_notification_ack = server.OCPHandler._handle_mesh_notification_ack
ProbeHandler._handle_mesh_approvals = server.OCPHandler._handle_mesh_approvals
ProbeHandler._handle_mesh_approval_request = server.OCPHandler._handle_mesh_approval_request
ProbeHandler._handle_mesh_approval_resolve = server.OCPHandler._handle_mesh_approval_resolve
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

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            params = parse_qs(parsed.query)
            try:
                if path == "/mesh/manifest":
                    self._send_json(mesh.get_manifest())
                    return
                if path == "/mesh/device-profile":
                    self._send_json({"status": "ok", "device_profile": dict(mesh.device_profile)})
                    return
                if path == "/mesh/stream":
                    since = int(params.get("since", ["0"])[0])
                    limit = int(params.get("limit", ["50"])[0])
                    self._send_json(mesh.stream_snapshot(since_seq=since, limit=limit))
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
                    since_seq = int(params.get("since", ["0"])[0])
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
                            ttl_seconds=int(payload.get("ttl_seconds") or 0),
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
                        "env_policy": {"inherit_host_env": True, "allow_env_override": False},
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
        alpha_peers = alpha.mesh.list_peers(limit=10)["peers"]
        beta_peers = beta.mesh.list_peers(limit=10)["peers"]
        self.assertEqual(alpha_peers[0]["peer_id"], "beta-node")
        self.assertEqual(beta_peers[0]["peer_id"], "alpha-node")
        event_types = [event["event_type"] for event in alpha.mesh.stream_snapshot(limit=20)["events"]]
        self.assertIn("mesh.handshake.sent", event_types)

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

        remote_events = alpha.mesh.list_remote_events("beta-node", limit=20)
        self.assertIn("mesh.synthetic.remote", {event["event_type"] for event in remote_events})
        peer = alpha.mesh.list_peers(limit=10)["peers"][0]
        self.assertEqual(peer["sync_state"]["remote_cursor"], synced["next_cursor"])
        self.assertEqual(peer["heartbeat"]["status"], "active")

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
        server.server_context["mesh"] = alpha.mesh
        probe = ProbeHandler()

        probe._handle_mesh_manifest()

        self.assertEqual(probe.code, 200)
        self.assertEqual(probe.payload["protocol"], "Open Compute Protocol")
        self.assertEqual(probe.payload["protocol_short_name"], "OCP")
        self.assertEqual(probe.payload["protocol_release"], "0.1")
        self.assertEqual(probe.payload["implementation"]["name"], "Sovereign Mesh")
        self.assertEqual(probe.payload["organism_card"]["organism_id"], "alpha-node")

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


if __name__ == "__main__":
    unittest.main()
