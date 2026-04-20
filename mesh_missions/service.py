from __future__ import annotations

import json
import uuid
from typing import Any, Optional


class MeshMissionService:
    """Mission lifecycle and continuity orchestration for SovereignMesh."""

    def __init__(
        self,
        mesh,
        *,
        compact_text,
        normalize_mission_continuity,
        normalize_mission_policy,
        normalize_mission_priority,
        normalize_mission_status,
        normalize_target_strategy,
        normalize_trust_tier,
        normalize_workload_class,
        unique_tokens,
        utcnow,
    ):
        self.mesh = mesh
        self._compact_text = compact_text
        self._normalize_mission_continuity = normalize_mission_continuity
        self._normalize_mission_policy = normalize_mission_policy
        self._normalize_mission_priority = normalize_mission_priority
        self._normalize_mission_status = normalize_mission_status
        self._normalize_target_strategy = normalize_target_strategy
        self._normalize_trust_tier = normalize_trust_tier
        self._normalize_workload_class = normalize_workload_class
        self._unique_tokens = unique_tokens
        self._utcnow = utcnow

    def store_mission_row(
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
        now = self._utcnow()
        with self.mesh._conn() as conn:
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
                    self._normalize_mission_status(status),
                    self._normalize_mission_priority(priority),
                    self._normalize_workload_class(workload_class),
                    str(origin_peer_id or self.mesh.node_id).strip() or self.mesh.node_id,
                    self._normalize_target_strategy(target_strategy),
                    json.dumps(self._normalize_mission_policy(policy or {})),
                    json.dumps(self._normalize_mission_continuity(continuity or {})),
                    json.dumps(dict(metadata or {})),
                    json.dumps(self._unique_tokens(child_job_ids or [])),
                    json.dumps(self._unique_tokens(cooperative_task_ids or [])),
                    json.dumps(dict(latest_checkpoint_ref or {})),
                    json.dumps(dict(result_ref or {})),
                    json.dumps(dict(result_bundle_ref or {})),
                    created_at or now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_missions WHERE request_id=?", ((request_id or "").strip(),)).fetchone()
        return self.mesh._row_to_mission(row)

    def existing_mission_by_request(self, request_id: str) -> Optional[dict]:
        with self.mesh._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_missions WHERE request_id=?", ((request_id or "").strip(),)).fetchone()
        return self.mesh._row_to_mission(row) if row is not None else None

    def mission_status_from_children(self, child_jobs: list[dict], cooperative_tasks: list[dict], metadata: dict) -> str:
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

    def mission_runtime_summary(
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

    def refresh_mission_runtime(self, mission: dict) -> dict:
        mission_data = dict(mission or {})
        child_job_ids = self._unique_tokens(mission_data.get("child_job_ids") or [])
        cooperative_task_ids = self._unique_tokens(mission_data.get("cooperative_task_ids") or [])
        cooperative_tasks: list[dict] = []
        for task_id in cooperative_task_ids:
            try:
                cooperative_tasks.append(self.mesh.get_cooperative_task(task_id))
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
                child_jobs.append(self.mesh.get_job(job_id))
            except Exception as exc:
                child_jobs.append({"id": job_id, "status": "failed", "resolution_error": str(exc), "updated_at": self._utcnow()})
        continuity, summary, latest_checkpoint_ref, result_refs, lineage = self.mission_runtime_summary(
            mission=mission_data,
            child_jobs=child_jobs,
            cooperative_tasks=cooperative_tasks,
        )
        metadata = dict(mission_data.get("metadata") or {})
        status = self.mission_status_from_children(child_jobs, cooperative_tasks, metadata)
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
            self._normalize_mission_status(mission_data.get("status")) != status
            or dict(mission_data.get("continuity") or {}) != continuity
            or dict(mission_data.get("latest_checkpoint_ref") or {}) != latest_checkpoint_ref
            or dict(mission_data.get("result_ref") or {}) != refreshed["result_ref"]
            or dict(mission_data.get("result_bundle_ref") or {}) != refreshed["result_bundle_ref"]
            or self._unique_tokens(mission_data.get("child_job_ids") or []) != child_job_ids
        )
        if stored_changed:
            refreshed = self.store_mission_row(
                mission_id=mission_data["id"],
                request_id=mission_data["request_id"],
                title=mission_data.get("title") or "",
                intent=mission_data.get("intent") or "",
                status=status,
                priority=mission_data.get("priority") or "normal",
                workload_class=mission_data.get("workload_class") or "default",
                origin_peer_id=mission_data.get("origin_peer_id") or self.mesh.node_id,
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

    def continuity_artifact_state(self, artifact_ref: dict, *, label: str) -> dict:
        ref = dict(artifact_ref or {})
        if not ref.get("id"):
            return {"label": label, "available": False, "artifact": {}, "pinned": False}
        try:
            artifact = self.mesh.get_artifact(ref["id"], include_content=False)
        except Exception:
            artifact = dict(ref)
        return {
            "label": label,
            "available": True,
            "artifact": artifact,
            "pinned": bool(artifact.get("pinned")),
        }

    def continuity_device_entry(
        self,
        *,
        peer_id: str,
        display_name: str,
        trust_tier: str,
        profile: dict,
        connected: bool,
        current: bool = False,
    ) -> dict:
        normalized_profile = self.mesh._normalize_device_profile(profile)
        sync_policy = self.mesh._device_profile_sync_policy(normalized_profile)
        device_class = str(normalized_profile.get("device_class") or "full").strip().lower() or "full"
        stability = (
            "stable"
            if device_class in {"full", "relay"} and not sync_policy.get("intermittent")
            else ("portable" if device_class == "light" else "compact")
        )
        return {
            "peer_id": str(peer_id or "").strip(),
            "display_name": str(display_name or peer_id or "device").strip() or "device",
            "trust_tier": self._normalize_trust_tier(trust_tier or "trusted"),
            "device_class": device_class,
            "execution_tier": str(normalized_profile.get("execution_tier") or "").strip(),
            "form_factor": str(normalized_profile.get("form_factor") or "").strip(),
            "connected": bool(connected),
            "current": bool(current),
            "sleep_capable": bool(sync_policy.get("sleep_capable")),
            "relay_recommended": bool(sync_policy.get("relay_recommended")),
            "stability": stability,
            "summary": self._compact_text(
                f"{display_name or peer_id or 'device'} is a {device_class} device with {stability} continuity posture.",
                limit=120,
            ),
        }

    def mission_safe_devices(self, mission: dict, *, preferred_device_classes: Optional[list[str]] = None) -> list[dict]:
        preferred = self._unique_tokens(preferred_device_classes or [])
        current_targets = {
            str((child or {}).get("target") or self.mesh.node_id).strip() or self.mesh.node_id
            for child in list(mission.get("child_jobs") or [])
        }
        devices: list[dict] = []
        local_entry = self.continuity_device_entry(
            peer_id=self.mesh.node_id,
            display_name=self.mesh.display_name,
            trust_tier="self",
            profile=self.mesh.device_profile,
            connected=True,
            current=self.mesh.node_id in current_targets,
        )
        if not preferred or local_entry["device_class"] in set(preferred):
            devices.append(local_entry)
        for peer in list(self.mesh.list_peers(limit=500).get("peers") or []):
            trust_tier = self._normalize_trust_tier(peer.get("trust_tier") or "trusted")
            if trust_tier not in {"self", "trusted", "partner"}:
                continue
            profile = self.mesh._peer_device_profile(peer)
            entry = self.continuity_device_entry(
                peer_id=peer.get("peer_id") or "",
                display_name=peer.get("display_name") or peer.get("peer_id") or "peer",
                trust_tier=trust_tier,
                profile=profile,
                connected=str(peer.get("status") or "").strip().lower() == "connected",
                current=str(peer.get("peer_id") or "").strip() in current_targets,
            )
            if preferred and entry["device_class"] not in set(preferred):
                continue
            devices.append(entry)
        unique_devices: list[dict] = []
        seen_peer_ids: set[str] = set()
        for device in devices:
            peer_token = str(device.get("peer_id") or "").strip()
            if not peer_token or peer_token in seen_peer_ids:
                continue
            seen_peer_ids.add(peer_token)
            unique_devices.append(device)
        unique_devices.sort(
            key=lambda item: (
                not bool(item.get("current")),
                item.get("device_class") not in {"full", "relay"},
                not bool(item.get("connected")),
                item.get("trust_tier") not in {"self", "trusted"},
                item.get("display_name") or item.get("peer_id") or "",
            )
        )
        return unique_devices

    def mission_continuity_actions(self, mission: dict) -> list[dict]:
        status = str(mission.get("status") or "planned").strip().lower()
        continuity = dict(mission.get("continuity") or {})
        actions: list[dict] = []
        if continuity.get("resumable") and status in {"checkpointed", "failed", "waiting"}:
            actions.append(
                {
                    "action": "resume",
                    "label": "Continue Mission",
                    "description": "Resume from the latest safe checkpoint on a trusted device.",
                    "primary": True,
                }
            )
        if continuity.get("checkpoint_ready") and status in {"checkpointed", "failed", "waiting"}:
            actions.append(
                {
                    "action": "resume_from_checkpoint",
                    "label": "Recover From Checkpoint",
                    "description": "Resume from the most recent saved checkpoint.",
                    "primary": False,
                }
            )
        if status not in {"completed", "cancelled", "active"}:
            actions.append(
                {
                    "action": "restart",
                    "label": "Restart Cleanly",
                    "description": "Start the mission again from the beginning.",
                    "primary": False,
                }
            )
        if status not in {"completed", "cancelled"}:
            actions.append(
                {
                    "action": "cancel",
                    "label": "Stop Mission",
                    "description": "Cancel the mission and stop further work.",
                    "primary": False,
                }
            )
        return actions

    def get_mission_continuity(self, mission_id: str) -> dict:
        mission = self.get_mission(mission_id)
        continuity = dict(mission.get("continuity") or {})
        child_jobs = [dict(item or {}) for item in list(mission.get("child_jobs") or [])]
        status = str(mission.get("status") or "planned").strip().lower()
        checkpoint_ready = bool(continuity.get("checkpoint_ready"))
        resumable = bool(continuity.get("resumable"))
        preferred_device_classes: list[str] = []
        recommended_action = "wait"
        recommended_reason = ""
        for child in child_jobs:
            recovery = dict(child.get("recovery") or {})
            hint = dict(recovery.get("recovery_hint") or {})
            preferred_device_classes.extend(self._unique_tokens(hint.get("preferred_target_device_classes") or []))
            if not recommended_reason and hint:
                recommended_reason = self._compact_text(
                    hint.get("reason")
                    or hint.get("strategy")
                    or "trusted continuity move recommended",
                    limit=120,
                )
            if hint.get("recommended_action") in {"resume", "restart"}:
                recommended_action = str(hint.get("recommended_action") or "wait").strip().lower()
        preferred_device_classes = self._unique_tokens(preferred_device_classes)
        if checkpoint_ready and status in {"checkpointed", "failed", "waiting"}:
            continuity_state = "ready_to_continue"
            headline = "Ready to continue from the last safe checkpoint."
            recommended_action = "resume"
            if not preferred_device_classes:
                preferred_device_classes = ["full", "relay"]
        elif resumable and status in {"checkpointed", "failed", "waiting"}:
            continuity_state = "attention_needed"
            headline = "Mission can continue, but it needs a recovery decision."
            recommended_action = "resume"
            if not preferred_device_classes:
                preferred_device_classes = ["full", "relay"]
        elif status == "active":
            continuity_state = "in_progress"
            headline = "Mission is currently running across your trusted devices."
        elif status == "completed":
            continuity_state = "completed"
            headline = "Mission is complete and the results are ready."
            recommended_action = "review"
        elif status == "cancelled":
            continuity_state = "stopped"
            headline = "Mission has been stopped."
            recommended_action = "none"
        else:
            continuity_state = "preparing"
            headline = "Mission is preparing to run."
        safe_devices = self.mission_safe_devices(mission, preferred_device_classes=preferred_device_classes)
        current_devices = [item for item in safe_devices if item.get("current")]
        recommended_device = next(
            (
                item
                for item in safe_devices
                if item.get("peer_id") not in {device.get("peer_id") for device in current_devices}
            ),
            (safe_devices[0] if safe_devices else {}),
        )
        checkpoint_state = self.continuity_artifact_state(dict(mission.get("latest_checkpoint_ref") or {}), label="Latest Checkpoint")
        result_bundle_state = self.continuity_artifact_state(dict(mission.get("result_bundle_ref") or {}), label="Result Bundle")
        return {
            "mission_id": mission["id"],
            "title": mission.get("title") or "Mission",
            "status": mission.get("status") or "planned",
            "continuity_state": continuity_state,
            "headline": headline,
            "operator_summary": self._compact_text(
                recommended_reason or headline or "Mission continuity state is available.",
                limit=160,
            ),
            "intent": mission.get("intent") or "",
            "recommended_action": recommended_action,
            "recommended_action_label": {
                "resume": "Continue Mission",
                "restart": "Restart Cleanly",
                "review": "Review Results",
                "wait": "Keep Watching",
                "none": "No Action Needed",
            }.get(recommended_action, "Continue Mission"),
            "current_devices": current_devices,
            "recommended_device": recommended_device or {},
            "safe_devices": safe_devices,
            "preferred_target_device_classes": preferred_device_classes,
            "recovery": {
                "recoverable": bool(resumable and status in {"checkpointed", "failed", "waiting"}),
                "checkpoint_ready": checkpoint_ready,
                "recommended_action": recommended_action,
                "reason": recommended_reason,
            },
            "artifacts": {
                "checkpoint": checkpoint_state,
                "result_bundle": result_bundle_state,
            },
            "available_actions": self.mission_continuity_actions(mission),
            "mission": mission,
        }

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
            raise self.mesh.MeshPolicyError("mission launch requires exactly one of job or cooperative_task")
        mission_request_id = str(request_id or uuid.uuid4().hex).strip()
        existing = self.existing_mission_by_request(mission_request_id)
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
                inferred_workload = (dict(task_spec.get("base_job") or {}).get("metadata") or {}).get("workload_class") or ""
            else:
                inferred_workload = (job_spec.get("metadata") or {}).get("workload_class") or ""
        target_strategy_token = self._normalize_target_strategy(
            target_strategy or (f"cooperative_{str(task_spec.get('strategy') or 'spread')}" if task_spec else "local")
        )
        mission_metadata = dict(metadata or {})
        mission_metadata["launch"] = {"type": launch_type}
        created = self.store_mission_row(
            mission_id=str(uuid.uuid4()),
            request_id=mission_request_id,
            title=title_token,
            intent=intent_token,
            status="planned",
            priority=priority,
            workload_class=inferred_workload or "default",
            origin_peer_id=self.mesh.node_id,
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
                response = self.mesh.submit_local_job(
                    {**launch_job, "target": self.mesh.node_id},
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
                task = self.mesh.launch_cooperative_task(
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
            created = self.store_mission_row(
                mission_id=created["id"],
                request_id=mission_request_id,
                title=title_token,
                intent=intent_token,
                status="waiting",
                priority=priority,
                workload_class=inferred_workload or "default",
                origin_peer_id=self.mesh.node_id,
                target_strategy=target_strategy_token,
                policy=policy or {},
                continuity=continuity or {},
                metadata=updated_metadata,
                child_job_ids=child_job_ids,
                cooperative_task_ids=cooperative_task_ids,
                created_at=created.get("created_at") or None,
            )
            self.mesh._record_event(
                "mesh.mission.launched",
                peer_id=self.mesh.node_id,
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
            self.store_mission_row(
                mission_id=created["id"],
                request_id=mission_request_id,
                title=title_token,
                intent=intent_token,
                status="failed",
                priority=priority,
                workload_class=inferred_workload or "default",
                origin_peer_id=self.mesh.node_id,
                target_strategy=target_strategy_token,
                policy=policy or {},
                continuity=continuity or {},
                metadata=failed_metadata,
                created_at=created.get("created_at") or None,
            )
            raise

    def get_mission(self, mission_id: str) -> dict:
        with self.mesh._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_missions WHERE id=?", ((mission_id or "").strip(),)).fetchone()
        if row is None:
            raise self.mesh.MeshPolicyError("mission not found")
        return self.refresh_mission_runtime(self.mesh._row_to_mission(row))

    def list_missions(self, *, limit: int = 25, status: str = "") -> dict:
        with self.mesh._conn() as conn:
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
        status_token = self._normalize_mission_status(status) if str(status or "").strip() else ""
        if status_token:
            missions = [mission for mission in missions if mission.get("status") == status_token]
        return {"peer_id": self.mesh.node_id, "count": len(missions), "missions": missions}

    def record_mission_control_action(self, mission: dict, *, action: str, operator_id: str = "", reason: str = "") -> dict:
        mission_data = dict(mission or {})
        metadata = dict(mission_data.get("metadata") or {})
        metadata["last_control_action"] = str(action or "").strip()
        metadata["last_control_at"] = self._utcnow()
        metadata["last_control_by"] = str(operator_id or "")
        metadata["last_control_reason"] = str(reason or "")
        return self.store_mission_row(
            mission_id=mission_data["id"],
            request_id=mission_data["request_id"],
            title=mission_data.get("title") or "",
            intent=mission_data.get("intent") or "",
            status=mission_data.get("status") or "planned",
            priority=mission_data.get("priority") or "normal",
            workload_class=mission_data.get("workload_class") or "default",
            origin_peer_id=mission_data.get("origin_peer_id") or self.mesh.node_id,
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

    def recover_mission(
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
            raise self.mesh.MeshPolicyError("unsupported mission recovery mode")
        child_jobs = [dict(job or {}) for job in list(mission.get("child_jobs") or [])]
        if not child_jobs:
            raise self.mesh.MeshPolicyError("mission has no child jobs to recover")
        self.record_mission_control_action(mission, action=mode_token, operator_id=operator_id, reason=reason)
        updated_jobs = []
        queue_messages = []
        errors = []
        skipped = []
        explicit_checkpoint_id = str(checkpoint_artifact_id or "").strip()
        if explicit_checkpoint_id and len(child_jobs) != 1:
            raise self.mesh.MeshPolicyError("mission-level explicit checkpoint selection requires exactly one child job")
        for child_job in child_jobs:
            job_id = str(child_job.get("id") or "").strip()
            if not job_id:
                continue
            try:
                status = str(child_job.get("status") or "").strip().lower()
                if mode_token == "restart":
                    recovered = self.mesh.restart_job(job_id, operator_id=operator_id, reason=reason)
                elif mode_token == "resume_checkpoint":
                    checkpoint_id = explicit_checkpoint_id or str(dict(child_job.get("latest_checkpoint_ref") or {}).get("id") or "").strip()
                    if not checkpoint_id:
                        skipped.append({"job_id": job_id, "reason": "checkpoint_unavailable"})
                        continue
                    recovered = self.mesh.resume_job_from_checkpoint(
                        job_id,
                        checkpoint_artifact_id=checkpoint_id,
                        operator_id=operator_id,
                        reason=reason,
                    )
                else:
                    if status not in {"checkpointed", "retry_wait", "failed"}:
                        skipped.append({"job_id": job_id, "reason": f"status_{status or 'unknown'}"})
                        continue
                    recovered = self.mesh.resume_job(job_id, operator_id=operator_id, reason=reason)
                updated_jobs.append(recovered["job"])
                queue_messages.append(recovered["queue_message"])
            except Exception as exc:
                errors.append({"job_id": job_id, "error": str(exc)})
        if not updated_jobs and errors:
            raise self.mesh.MeshPolicyError(errors[0]["error"])
        if not updated_jobs and skipped:
            raise self.mesh.MeshPolicyError("mission has no recoverable child jobs for requested action")
        updated = self.get_mission(mission_id)
        event_type = {
            "resume_latest": "mesh.mission.resume_requested",
            "resume_checkpoint": "mesh.mission.resume_checkpoint_requested",
            "restart": "mesh.mission.restart_requested",
        }[mode_token]
        self.mesh._record_event(
            event_type,
            peer_id=self.mesh.node_id,
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
            "status": updated.get("status") or "waiting",
            "mission": updated,
            "jobs": updated_jobs,
            "queue_messages": queue_messages,
            "errors": errors,
            "skipped": skipped,
        }

    def resume_mission(self, mission_id: str, *, operator_id: str = "", reason: str = "mission_resume_latest") -> dict:
        return self.recover_mission(
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
        return self.recover_mission(
            mission_id,
            operator_id=operator_id,
            reason=reason,
            mode="resume_checkpoint",
            checkpoint_artifact_id=checkpoint_artifact_id,
        )

    def cancel_mission(self, mission_id: str, *, operator_id: str = "", reason: str = "mission_cancelled") -> dict:
        mission = self.get_mission(mission_id)
        self.record_mission_control_action(mission, action="cancel", operator_id=operator_id, reason=reason)
        results = []
        errors = []
        for job_id in self._unique_tokens(mission.get("child_job_ids") or []):
            try:
                results.append(self.mesh.cancel_job(job_id, reason=f"{reason}:{operator_id or 'operator'}"))
            except Exception as exc:
                errors.append({"job_id": job_id, "error": str(exc)})
        updated = self.get_mission(mission_id)
        return {"status": updated.get("status") or "cancelled", "mission": updated, "jobs": results, "errors": errors}

    def restart_mission(self, mission_id: str, *, operator_id: str = "", reason: str = "mission_restart") -> dict:
        return self.recover_mission(
            mission_id,
            operator_id=operator_id,
            reason=reason,
            mode="restart",
        )
