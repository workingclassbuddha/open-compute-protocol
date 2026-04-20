from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Optional

from mesh_protocol import MeshPolicyError


class MeshExecutionService:
    """Execution adapters for shell, python, docker, and wasm jobs."""

    def __init__(
        self,
        mesh,
        *,
        json_dump,
        normalize_env_var_name,
        normalize_secret_source,
        oci_digest,
        ocp_result_artifact_type,
        ocp_result_config_media_type,
        oci_manifest_media_type,
        secret_value_digest,
        sha256_bytes,
        sign_message,
        signature_scheme,
        subprocess_module,
        utcnow,
    ):
        self.mesh = mesh
        self._json_dump = json_dump
        self._normalize_env_var_name = normalize_env_var_name
        self._normalize_secret_source = normalize_secret_source
        self._oci_digest = oci_digest
        self._ocp_result_artifact_type = ocp_result_artifact_type
        self._ocp_result_config_media_type = ocp_result_config_media_type
        self._oci_manifest_media_type = oci_manifest_media_type
        self._secret_value_digest = secret_value_digest
        self._sha256_bytes = sha256_bytes
        self._sign_message = sign_message
        self._signature_scheme = signature_scheme
        self._subprocess = subprocess_module
        self._utcnow = utcnow

    def resolve_runtime_cwd(self, runtime_environment: dict, execution: dict) -> Path:
        requested_cwd = str(runtime_environment.get("cwd") or execution.get("cwd") or execution.get("working_dir") or "").strip()
        cwd_path = self.mesh.workspace_root
        if not requested_cwd:
            return cwd_path
        candidate = Path(requested_cwd)
        if not candidate.is_absolute():
            candidate = (self.mesh.workspace_root / candidate).resolve()
        else:
            candidate = candidate.resolve()
        if self.mesh.workspace_root != candidate and self.mesh.workspace_root not in candidate.parents:
            raise MeshPolicyError("runtime cwd must stay inside workspace_root")
        return candidate

    def resolve_secret_binding_value(self, binding: dict, raw_secret: Any) -> tuple[Optional[str], dict]:
        env_name = self._normalize_env_var_name(binding.get("env_var"))
        scope = str(binding.get("scope") or "").strip()
        required = bool(binding.get("required", True))
        source = self._normalize_secret_source(binding.get("source") or "inline")
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
            provider_name = self._normalize_env_var_name(binding.get("name") or env_name)
            delivery_record["name"] = provider_name
            resolved_value = os.environ.get(provider_name)
        elif source == "store":
            provider_name = str(binding.get("name") or "").strip()
            delivery_record["name"] = provider_name
            with self.mesh._conn() as conn:
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
            file_path = self.mesh._resolve_secret_file_path(requested_path)
            resolved_value = file_path.read_text(encoding="utf-8").rstrip("\r\n")
        if resolved_value is not None:
            delivery_record["resolved"] = True
            delivery_record["value_digest"] = self._secret_value_digest(resolved_value)
        return resolved_value, delivery_record

    def build_runtime_env(self, *, job: dict, payload: dict, spec: dict) -> tuple[dict[str, str], list[dict]]:
        execution = dict(spec.get("execution") or {})
        runtime_environment = dict(spec.get("runtime_environment") or {})
        env_policy = dict(runtime_environment.get("env_policy") or {})
        inherit_host_env = bool(env_policy.get("inherit_host_env", True))
        allow_env_override = bool(env_policy.get("allow_env_override", True))
        env: dict[str, str] = dict(os.environ) if inherit_host_env else {}
        delivery_records: list[dict] = []
        for key, value in dict(execution.get("env") or {}).items():
            env_name = self._normalize_env_var_name(key)
            if not env_name:
                continue
            if allow_env_override or env_name not in env:
                env[env_name] = str(value)
        payload_secrets = dict(payload.get("secrets") or {})
        for binding in list((runtime_environment.get("secrets") or {}).get("bindings") or []):
            normalized_name = self._normalize_env_var_name(binding.get("env_var"))
            if not normalized_name:
                continue
            if normalized_name.startswith("OCP_RESUME_"):
                raise MeshPolicyError(f"secret binding cannot override reserved runtime env: {normalized_name}")
            resolved_value, delivery_record = self.resolve_secret_binding_value(
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

    def container_runtime_paths(self, runtime_environment: dict, execution: dict) -> dict[str, Any]:
        filesystem = dict(runtime_environment.get("filesystem") or {})
        profile = str(filesystem.get("profile") or "workspace").strip().lower() or "workspace"
        if profile == "isolated":
            return {
                "mount_workspace": False,
                "host_workdir": None,
                "container_root": "",
                "container_workdir": "",
            }
        host_workdir = self.resolve_runtime_cwd(runtime_environment, execution)
        container_root = "/workspace"
        try:
            rel_path = host_workdir.relative_to(self.mesh.workspace_root)
            container_workdir = str((Path(container_root) / rel_path).as_posix())
        except Exception:
            container_workdir = container_root
        return {
            "mount_workspace": True,
            "host_workdir": host_workdir,
            "container_root": container_root,
            "container_workdir": container_workdir,
        }

    def cleanup_docker_container(self, container_name: str) -> None:
        sample = str(container_name or "").strip()
        if not sample:
            return
        try:
            self._subprocess.run(
                ["docker", "rm", "-f", sample],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        except Exception:
            self.mesh.logger.debug("docker cleanup failed for %s", sample, exc_info=True)

    def artifact_path_for_digest(self, digest: str) -> Optional[Path]:
        token = str(digest or "").strip()
        if not token:
            return None
        if token.startswith("sha256:"):
            token = token.split(":", 1)[1]
        with self.mesh._conn() as conn:
            row = conn.execute(
                "SELECT path FROM mesh_artifacts WHERE digest=? ORDER BY created_at DESC LIMIT 1",
                (token,),
            ).fetchone()
        if row is None:
            return None
        return Path(row["path"]).resolve()

    def resolve_wasm_component_path(self, execution: dict, payload: dict) -> tuple[Path, dict]:
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
                candidate = (self.mesh.workspace_root / candidate).resolve()
            else:
                candidate = candidate.resolve()
            if self.mesh.workspace_root != candidate and self.mesh.workspace_root not in candidate.parents:
                raise MeshPolicyError("wasm component path must stay inside workspace_root")
            if not candidate.exists():
                raise MeshPolicyError("wasm component path does not exist")
            return candidate, component_ref
        component_id = str(component_ref.get("id") or "").strip()
        if component_id:
            artifact = self.mesh.get_artifact(component_id, requester_peer_id="", include_content=False)
            path = Path(artifact["path"]).resolve()
            if not path.exists():
                raise MeshPolicyError("wasm component artifact is missing")
            return path, {
                **component_ref,
                "id": artifact.get("id") or component_id,
                "digest": artifact.get("digest") or component_ref.get("digest") or "",
            }
        digest_path = self.artifact_path_for_digest(component_ref.get("digest") or "")
        if digest_path is not None and digest_path.exists():
            return digest_path, component_ref
        raise MeshPolicyError("wasm component source could not be resolved")

    def ingest_job_submission(
        self,
        *,
        peer_id: str,
        request_id: str,
        job_body: dict,
        peer: Optional[dict],
    ) -> dict:
        existing = self.mesh._existing_job_by_request(request_id)
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
        policy = self.mesh._normalize_policy(job_body.get("policy") or {})
        if not self.mesh._policy_allows_peer(policy, peer):
            job = self.mesh._store_job_row(
                job_id=str(uuid.uuid4()),
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.mesh.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="rejected",
                metadata={"reason": "policy_denied"},
            )
            self.mesh._record_event(
                "mesh.job.rejected",
                peer_id=peer_id,
                request_id=request_id,
                payload={"job_id": job["id"], "reason": "policy_denied"},
            )
            return {"status": "rejected", "job": job}

        if dict(job_body.get("payload") or {}).get("secrets") and not policy.get("secret_scopes"):
            raise MeshPolicyError("payload.secrets requires explicit policy.secret_scopes")
        metadata = self.mesh._normalize_job_metadata(job_body.get("metadata") or {})
        spec = self.mesh._normalize_job_spec(
            job_body,
            requirements=requirements,
            policy=policy,
            metadata=metadata,
        )
        self.mesh._validate_normalized_job_spec(spec)
        requirements = dict(spec.get("requirements") or {})
        policy = self.mesh._normalize_policy(spec.get("policy") or policy)
        metadata["job_spec"] = spec
        if not self.mesh._requirements_satisfied(requirements):
            job = self.mesh._store_job_row(
                job_id=str(uuid.uuid4()),
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.mesh.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="rejected",
                metadata={"reason": "requirements_unmet", "job_spec": spec},
            )
            return {"status": "rejected", "job": job}

        dispatch_mode = self.mesh._job_dispatch_mode(kind, job_body)
        if dispatch_mode == "queued":
            metadata["dispatch_mode"] = "queued"
            queue_name = self.mesh._queue_name_for_job(job_body, metadata)
            queue_policy = self.mesh._queue_policy_for_job(job_body, metadata, queue_name)
            metadata["queue_name"] = queue_name
            metadata["queue_policy"] = dict(queue_policy)
            dedupe_key = self.mesh._dedupe_key_for_job(job_body, metadata)
            if dedupe_key:
                metadata["dedupe_key"] = dedupe_key
                existing_queued = self.mesh._find_queued_job_by_dedupe_key(dedupe_key, queue_name=queue_name)
                if existing_queued is not None:
                    existing_queue = dict(existing_queued.get("queue") or {})
                    self.mesh._record_event(
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
            queued_job = self.mesh._store_job_row(
                job_id=str(uuid.uuid4()),
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.mesh.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="queued",
                metadata=metadata,
            )
            queue_message = self.mesh._create_queue_message(
                job_id=queued_job["id"],
                queue_name=queue_name,
                dedupe_key=dedupe_key,
                queue_policy=queue_policy,
                metadata={"request_id": request_id, "kind": kind, "origin_peer_id": peer_id},
            )
            self.mesh._record_event(
                "mesh.job.queued",
                peer_id=peer_id,
                request_id=request_id,
                payload={"job_id": queued_job["id"], "kind": kind, "dispatch_mode": "queued"},
            )
            self.mesh._record_event(
                "mesh.queue.enqueued",
                peer_id=peer_id,
                request_id=request_id,
                payload={"job_id": queued_job["id"], "queue_message_id": queue_message["id"], "queue_name": queue_name},
            )
            return {"status": "queued", "job": self.mesh.get_job(queued_job["id"]), "queue_message": queue_message}

        job_id = str(uuid.uuid4())
        lease = self.mesh.acquire_lease(
            peer_id=peer_id,
            resource=(job_body.get("resource") or f"job:{job_id}"),
            agent_id=(job_body.get("agent_id") or "").strip(),
            job_id=job_id,
            ttl_seconds=int(job_body.get("ttl_seconds") or 300),
            metadata={"request_id": request_id, "job_kind": kind},
        )
        job = self.mesh._store_job_row(
            job_id=job_id,
            request_id=request_id,
            kind=kind,
            origin_peer_id=peer_id,
            target_peer_id=self.mesh.node_id,
            requirements=requirements,
            policy=policy,
            payload_ref=dict(job_body.get("payload_ref") or {}),
            payload_inline=dict(job_body.get("payload") or {}),
            artifact_inputs=list(job_body.get("artifact_inputs") or []),
            status="running",
            lease_id=lease["id"],
            metadata={"submitted_by": peer_id, **metadata},
        )
        self.mesh._record_event(
            "mesh.job.accepted",
            peer_id=peer_id,
            request_id=request_id,
            payload={"job_id": job_id, "kind": kind},
        )

        try:
            payload = self.mesh._resolve_job_payload(job_body)
            executor, result, completion_metadata = self.execute_job(job, payload=payload)
            result_package = self.publish_job_result_package(
                job,
                result=result,
                media_type="application/json",
                executor=executor,
                metadata={"job_id": job_id, "executor": executor, **dict(completion_metadata or {})},
            )
            result_artifact = result_package["result_ref"]
            job = self.mesh._store_job_row(
                job_id=job_id,
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.mesh.node_id,
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
            self.mesh.release_lease(lease["id"], status="completed")
            job = self.mesh.get_job(job_id)
            self.mesh._record_event(
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
            self.mesh.release_lease(lease["id"], status="failed")
            job = self.mesh._store_job_row(
                job_id=job_id,
                request_id=request_id,
                kind=kind,
                origin_peer_id=peer_id,
                target_peer_id=self.mesh.node_id,
                requirements=requirements,
                policy=policy,
                payload_ref=dict(job_body.get("payload_ref") or {}),
                payload_inline=dict(job_body.get("payload") or {}),
                artifact_inputs=list(job_body.get("artifact_inputs") or []),
                status="failed",
                lease_id=lease["id"],
                metadata={"submitted_by": peer_id, **metadata, "error": str(exc)},
            )
            self.mesh._record_event(
                "mesh.job.failed",
                peer_id=peer_id,
                request_id=request_id,
                payload={"job_id": job_id, "error": str(exc)},
            )
            raise

    def submit_local_job(self, job: dict, *, request_id: Optional[str] = None) -> dict:
        local_request_id = (request_id or uuid.uuid4().hex).strip()
        local_peer = {
            "peer_id": self.mesh.node_id,
            "organism_id": self.mesh.node_id,
            "display_name": self.mesh.display_name,
            "trust_tier": "self",
            "capability_cards": self.mesh.capability_cards(),
            "metadata": {},
        }
        return self.ingest_job_submission(
            peer_id=self.mesh.node_id,
            request_id=local_request_id,
            job_body=dict(job or {}),
            peer=local_peer,
        )

    def accept_job_submission(self, envelope: dict) -> dict:
        peer_id, request_meta, body, peer = self.mesh._verify_envelope(envelope, route="/mesh/jobs/submit")
        request_id = (request_meta.get("request_id") or "").strip()
        return self.ingest_job_submission(
            peer_id=peer_id,
            request_id=request_id,
            job_body=dict(body.get("job") or {}),
            peer=peer,
        )

    def execute_job(self, job: dict, *, payload: dict) -> tuple[str, dict, dict]:
        kind = (job.get("kind") or "").strip().lower()
        policy = dict(job.get("policy") or {})
        spec = dict(
            job.get("spec")
            or self.mesh._normalize_job_spec(
                {**job, "payload": payload},
                requirements=job.get("requirements"),
                policy=policy,
                metadata=job.get("metadata"),
            )
        )
        execution = dict(spec.get("execution") or {})
        runtime_environment = dict(spec.get("runtime_environment") or {})
        if kind == "shell.command":
            argv = [str(part) for part in (execution.get("command") or [])]
            if not argv:
                raise MeshPolicyError("shell.command requires payload.command")
            cwd_path = self.resolve_runtime_cwd(runtime_environment, execution)
            timeout_seconds = int(execution.get("timeout_seconds") or 300)
            env, secret_delivery = self.build_runtime_env(job=job, payload=payload, spec=spec)
            resume_checkpoint_ref = self.mesh._job_resume_checkpoint_ref(job)
            if resume_checkpoint_ref:
                env["OCP_RESUME_ARTIFACT_ID"] = str(resume_checkpoint_ref.get("id") or "")
                env["OCP_RESUME_ARTIFACT_DIGEST"] = str(resume_checkpoint_ref.get("digest") or "")
                env["OCP_RESUME_ARTIFACT_MEDIA_TYPE"] = str(resume_checkpoint_ref.get("media_type") or "")
            completed = self._subprocess.run(
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
            return self.execute_job(shell_job, payload=shell_payload)
        if kind == "docker.container":
            if not self.mesh.docker_enabled:
                raise MeshPolicyError("docker runtime unavailable")
            image = str(execution.get("image") or "").strip()
            if not image:
                raise MeshPolicyError("docker.container requires payload.image")
            timeout_seconds = int(execution.get("timeout_seconds") or 300)
            env, secret_delivery = self.build_runtime_env(job=job, payload=payload, spec=spec)
            resume_checkpoint_ref = self.mesh._job_resume_checkpoint_ref(job)
            if resume_checkpoint_ref:
                env["OCP_RESUME_ARTIFACT_ID"] = str(resume_checkpoint_ref.get("id") or "")
                env["OCP_RESUME_ARTIFACT_DIGEST"] = str(resume_checkpoint_ref.get("digest") or "")
                env["OCP_RESUME_ARTIFACT_MEDIA_TYPE"] = str(resume_checkpoint_ref.get("media_type") or "")
            network_mode = str((runtime_environment.get("network") or {}).get("mode") or "default").strip().lower() or "default"
            if network_mode not in {"default", "bridge", "host", "none"}:
                raise MeshPolicyError(f"unsupported container network mode: {network_mode}")
            path_info = self.container_runtime_paths(runtime_environment, execution)
            container_name = f"ocp-{self.mesh.node_id[:16]}-{str(job.get('id') or uuid.uuid4().hex)[:12]}"
            docker_argv = ["docker", "run", "--rm", "--name", container_name]
            if network_mode != "default":
                docker_argv.extend(["--network", network_mode])
            if path_info["mount_workspace"]:
                docker_argv.extend(
                    [
                        "-v",
                        f"{self.mesh.workspace_root}:{path_info['container_root']}:rw",
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
                completed = self._subprocess.run(
                    docker_argv,
                    capture_output=True,
                    text=True,
                    timeout=max(1, timeout_seconds),
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                self.cleanup_docker_container(container_name)
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
                    "cwd": str(path_info["host_workdir"] or self.mesh.workspace_root),
                    "runtime_environment": runtime_environment,
                    "exit_code": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                },
                {"secret_delivery": secret_delivery},
            )
        if kind == "wasm.component":
            if not self.mesh.wasm_enabled:
                raise MeshPolicyError("wasm runtime unavailable")
            component_path, resolved_component_ref = self.resolve_wasm_component_path(execution, payload)
            timeout_seconds = int(execution.get("timeout_seconds") or 300)
            env, secret_delivery = self.build_runtime_env(job=job, payload=payload, spec=spec)
            resume_checkpoint_ref = self.mesh._job_resume_checkpoint_ref(job)
            if resume_checkpoint_ref:
                env["OCP_RESUME_ARTIFACT_ID"] = str(resume_checkpoint_ref.get("id") or "")
                env["OCP_RESUME_ARTIFACT_DIGEST"] = str(resume_checkpoint_ref.get("digest") or "")
                env["OCP_RESUME_ARTIFACT_MEDIA_TYPE"] = str(resume_checkpoint_ref.get("media_type") or "")
            env["OCP_COMPONENT_ID"] = str(resolved_component_ref.get("id") or "")
            env["OCP_COMPONENT_DIGEST"] = str(resolved_component_ref.get("digest") or "")
            cwd_path = self.resolve_runtime_cwd(runtime_environment, execution)
            filesystem = dict(runtime_environment.get("filesystem") or {})
            network_mode = str((runtime_environment.get("network") or {}).get("mode") or "default").strip().lower() or "default"
            if network_mode not in {"default", "none"}:
                raise MeshPolicyError(f"unsupported wasm network mode: {network_mode}")
            wasm_argv = [self.mesh.wasm_runtime, "run"]
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
                completed = self._subprocess.run(
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
            if self.mesh.metabolism is None:
                raise MeshPolicyError("metabolism executor unavailable")
            local_job = self.mesh.metabolism.trigger(
                kind=(payload.get("kind") or "wake_maintenance"),
                topic=payload.get("topic"),
                payload=dict(payload.get("payload") or {}),
            )
            return "personal-mirror", {"status": "queued", "local_job": local_job}, {}
        if kind == "swarm.submit":
            if self.mesh.swarm is None:
                raise MeshPolicyError("swarm gateway unavailable")
            result = self.mesh.swarm.submit(payload)
            return "personal-mirror", result, {}
        if kind.startswith("golem.") or "golem-provider" in set(job.get("requirements", {}).get("capabilities") or []):
            result = self.mesh.golem_adapter.execute_job(kind, payload, policy)
            return "golem-mesh", result, {}
        raise MeshPolicyError(f"unsupported mesh job kind: {job.get('kind')}")

    def artifact_descriptor(self, ref: dict, *, role: str = "", annotations: Optional[dict] = None) -> dict:
        merged_annotations = dict(annotations or {})
        if role:
            merged_annotations.setdefault("org.opencompute.role", role)
        descriptor = {
            "id": ref.get("id") or "",
            "digest": ref.get("digest") or "",
            "oci_digest": self._oci_digest(ref.get("digest") or ""),
            "media_type": ref.get("media_type") or "application/octet-stream",
            "size_bytes": int(ref.get("size_bytes") or 0),
            "role": str(role or "").strip(),
            "annotations": merged_annotations,
        }
        descriptor["oci_descriptor"] = self.mesh._oci_descriptor(ref, annotations=merged_annotations)
        return descriptor

    def publish_job_result_package(
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
        package_metadata = dict(metadata or {})
        secret_delivery = [dict(item) for item in list(package_metadata.get("secret_delivery") or [])]
        result_ref = result_artifact or self.mesh.publish_local_artifact(
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
        descriptors = [self.artifact_descriptor(result_ref, role="result")]
        related_artifacts: dict[str, dict] = {}
        if isinstance(result, dict):
            for stream_name in ("stdout", "stderr"):
                content = result.get(stream_name)
                if content:
                    stream_ref = self.mesh.publish_local_artifact(
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
                    descriptors.append(self.artifact_descriptor(stream_ref, role=stream_name))
            checkpoint_payload = result.get("checkpoint")
            if checkpoint_payload is not None:
                checkpoint_ref = self.mesh.publish_local_artifact(
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
                descriptors.append(self.artifact_descriptor(checkpoint_ref, role="checkpoint"))
        material_descriptors = [
            self.mesh._artifact_descriptor_from_input(item)
            for item in list(job.get("artifact_inputs") or [])
        ]
        config_payload = {
            "kind": "ocp.artifact.config",
            "schema_version": 1,
            "artifact_type": self._ocp_result_artifact_type,
            "created_at": self._utcnow(),
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
        config_ref = self.mesh.publish_local_artifact(
            config_payload,
            media_type=self._ocp_result_config_media_type,
            policy=job["policy"],
            metadata={
                "artifact_kind": "config",
                "artifact_type": self._ocp_result_artifact_type,
                "job_id": job["id"],
                "attempt_id": attempt_id,
                "result_artifact_id": result_ref["id"],
            },
        )
        attestation_payload = {
            "kind": "ocp.execution.attestation",
            "schema_version": 2,
            "issued_at": self._utcnow(),
            "issuer": {
                "node_id": self.mesh.node_id,
                "display_name": self.mesh.display_name,
                "public_key": self.mesh.public_key,
                "signature_scheme": self._signature_scheme,
            },
            "subject": {
                "artifact_id": result_ref["id"],
                "digest": result_ref["digest"],
                "media_type": result_ref["media_type"],
            },
            "subject_descriptor": self.mesh._oci_descriptor(
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
                "result_descriptor": self.mesh._oci_descriptor(
                    result_ref,
                    annotations={"org.opencompute.role": "result"},
                ),
                "bundle_members": [descriptor["oci_descriptor"] for descriptor in descriptors],
                "secret_delivery": secret_delivery,
                "job_spec_digest": self._sha256_bytes(self._json_dump(job.get("spec") or {}).encode("utf-8")),
                "output_roles": [descriptor["role"] for descriptor in descriptors if descriptor.get("role")],
            },
            "verification": {
                "signature_scheme": self._signature_scheme,
                "canonical_form": "json-c14n-sort-keys",
            },
        }
        attestation_signature = self._sign_message(
            self.mesh.private_key,
            self._json_dump(attestation_payload).encode("utf-8"),
        )
        attestation_payload["signature"] = attestation_signature
        attestation_payload["verification"]["signed_payload_digest"] = self._sha256_bytes(
            self._json_dump({k: v for k, v in attestation_payload.items() if k != "signature"}).encode("utf-8")
        )
        attestation_ref = self.mesh.publish_local_artifact(
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
        descriptors.append(self.artifact_descriptor(attestation_ref, role="attestation"))
        bundle_manifest = {
            "schemaVersion": 2,
            "mediaType": self._oci_manifest_media_type,
            "artifactType": self._ocp_result_artifact_type,
            "config": self.mesh._oci_descriptor(
                config_ref,
                annotations={"org.opencompute.role": "config"},
            ),
            "layers": [descriptor["oci_descriptor"] for descriptor in descriptors],
            "subject": self.mesh._oci_descriptor(
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
            "created_at": self._utcnow(),
            "job_id": job["id"],
            "request_id": job.get("request_id") or "",
            "attempt_id": attempt_id,
            "executor": executor,
            "artifact_type": self._ocp_result_artifact_type,
            "primary": self.artifact_descriptor(result_ref, role="result"),
            "descriptors": descriptors,
        }
        bundle_ref = self.mesh.publish_local_artifact(
            bundle_manifest,
            media_type=self._oci_manifest_media_type,
            policy=job["policy"],
            metadata={
                "artifact_kind": "bundle",
                "bundle_type": "job-result",
                "artifact_type": self._ocp_result_artifact_type,
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
