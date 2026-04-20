from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

from mesh_protocol import MeshPolicyError


class MeshHelperService:
    """Helper lifecycle and autonomous offload policy for SovereignMesh."""

    def __init__(
        self,
        mesh,
        *,
        loads_json,
        normalize_offload_policy,
        normalize_preference_token,
        normalize_trust_tier,
        normalize_workload_class,
        pressure_rank,
        unique_tokens,
        utcnow,
    ):
        self.mesh = mesh
        self._loads_json = loads_json
        self._normalize_offload_policy = normalize_offload_policy
        self._normalize_preference_token = normalize_preference_token
        self._normalize_trust_tier = normalize_trust_tier
        self._normalize_workload_class = normalize_workload_class
        self._pressure_rank = pressure_rank
        self._unique_tokens = unique_tokens
        self._utcnow = utcnow

    def mesh_pressure(self) -> dict:
        """Summarise local mesh compute pressure for helper-enlistment planning."""
        metrics = self.mesh.queue_metrics()
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
        compute_profile = dict(self.mesh.device_profile.get("compute_profile") or {})
        if self.mesh.device_profile.get("battery_powered") and pressure in {"saturated", "elevated"}:
            reasons.append("battery_under_load")
        return {
            "peer_id": self.mesh.node_id,
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
            "observed_at": self._utcnow(),
        }

    def peer_enlistment_state(self, peer: dict) -> dict:
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
        peers = self.mesh.list_peers(limit=limit).get("peers", [])
        helpers = [self.peer_enlistment_state(peer) for peer in peers]
        active = [item for item in helpers if item["state"] in {"enlisted", "draining"}]
        return {
            "peer_id": self.mesh.node_id,
            "count": len(helpers),
            "active_count": len(active),
            "pressure": self.mesh_pressure(),
            "helpers": helpers,
        }

    def record_enlistment_action(
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
        existing_row = self.mesh._get_peer_row(peer_id)
        if existing_row is None:
            raise MeshPolicyError("peer not found for enlistment")
        existing_metadata = dict(self._loads_json(existing_row["metadata"], {}))
        enlistment = dict(existing_metadata.get("enlistment") or {})
        now = self._utcnow()
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
        updated_peer = self.mesh._update_peer_record(peer_id, metadata=existing_metadata)
        self.mesh._record_event(
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
        return self.peer_enlistment_state(updated_peer)

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
        peer_row = self.mesh._get_peer_row(peer_token)
        if peer_row is None:
            raise MeshPolicyError("peer not found for enlistment")
        trust = self._normalize_trust_tier(peer_row["trust_tier"])
        if trust in {"blocked", "public"}:
            raise MeshPolicyError(f"cannot enlist peer with trust_tier={trust}")
        return self.record_enlistment_action(
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
        return self.record_enlistment_action(
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
        return self.record_enlistment_action(
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
            placement = self.mesh._normalized_placement(job_input)
        else:
            placement = {
                "workload_class": "default",
                "gpu_required": bool(mesh_pressure.get("pressure") == "saturated" and mesh_pressure.get("gpu_capable")),
                "queue_class": "default",
            }
        peers = self.mesh.list_peers(limit=200).get("peers", [])
        candidates = []
        synthetic_job = {"requirements": {"placement": placement}} if not job_input else job_input
        for peer in peers:
            enlistment = self.peer_enlistment_state(peer)
            trust = self._normalize_trust_tier(peer.get("trust_tier"))
            if trust in {"blocked", "public"}:
                continue
            score, reasons = self.mesh._peer_candidate_score(peer, synthetic_job)
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
            "peer_id": self.mesh.node_id,
            "pressure": mesh_pressure,
            "placement": placement,
            "candidate_count": len(candidates),
            "candidates": picks,
            "generated_at": self._utcnow(),
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
                discovery = self.mesh.seek_peers(
                    hosts=list(seek_hosts or []) or None,
                    trust_tier="trusted",
                    auto_connect=True,
                    refresh_known=True,
                )
            except Exception as exc:
                discovery = {"error": str(exc)}
        self.mesh._record_event(
            "mesh.helper.auto_seek",
            peer_id=self.mesh.node_id,
            payload={
                "pressure": pressure,
                "enlisted": [entry["peer_id"] for entry in enlisted],
                "skipped": [entry["peer_id"] for entry in skipped],
                "mode": mode,
                "reason": reason,
            },
        )
        return {
            "peer_id": self.mesh.node_id,
            "pressure": pressure,
            "plan": plan,
            "enlisted": enlisted,
            "skipped": skipped,
            "discovery": discovery,
            "generated_at": self._utcnow(),
        }

    def row_to_offload_preference(self, row) -> dict:
        if row is None:
            return {}
        return {
            "peer_id": str(row["peer_id"] or "").strip(),
            "workload_class": self._normalize_workload_class(row["workload_class"] or "default"),
            "preference": self._normalize_preference_token(row["preference"] or "allow"),
            "source": str(row["source"] or "").strip(),
            "metadata": self._loads_json(row["metadata"], {}),
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
        workload_token = self._normalize_workload_class(workload_class or "default")
        preference_token = self._normalize_preference_token(preference)
        if self.mesh._get_peer_row(peer_token) is None:
            raise MeshPolicyError("peer not found for offload preference")
        now = self._utcnow()
        with self.mesh._conn() as conn:
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
        result = self.row_to_offload_preference(row)
        self.mesh._record_event(
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
            args.append(self._normalize_workload_class(workload_class))
        query.append("ORDER BY updated_at DESC LIMIT ?")
        args.append(max(1, int(limit or 100)))
        with self.mesh._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(args)).fetchall()
        items = [self.row_to_offload_preference(row) for row in rows]
        return {"peer_id": self.mesh.node_id, "count": len(items), "preferences": items}

    def offload_preferences_map(self, workload_class: str) -> dict[str, dict]:
        workload_token = self._normalize_workload_class(workload_class or "default")
        with self.mesh._conn() as conn:
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
            item = self.row_to_offload_preference(row)
            peer_token = item.get("peer_id") or ""
            if peer_token and peer_token not in mapped:
                mapped[peer_token] = item
        return mapped

    def evaluate_autonomous_offload(self, *, job: Optional[dict] = None) -> dict:
        policy = self._normalize_offload_policy(
            (self.mesh.device_profile or {}).get("offload_policy") or {},
            self.mesh.device_profile,
        )
        pressure = self.mesh_pressure()
        job_input = dict(job or {})
        if job_input:
            placement = self.mesh._normalized_placement(job_input)
        else:
            placement = {
                "workload_class": "default",
                "gpu_required": False,
                "queue_class": "default",
            }
        threshold_ok = self._pressure_rank(pressure.get("pressure")) >= self._pressure_rank(policy.get("pressure_threshold"))
        result = {
            "peer_id": self.mesh.node_id,
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
            "generated_at": self._utcnow(),
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
        workload_class = self._normalize_workload_class(placement.get("workload_class") or "default")
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
        preference_map = self.offload_preferences_map(workload_class)
        eligible = []
        approval_reasons: list[str] = []
        for candidate in candidates:
            trust = self._normalize_trust_tier(candidate.get("trust_tier"))
            device_class = str(candidate.get("device_class") or "full").strip().lower()
            compute_profile = dict(candidate.get("compute_profile") or {})
            score = int(candidate.get("score") or 0)
            peer_pref = dict(preference_map.get(candidate.get("peer_id") or "") or {})
            pref_token = self._normalize_preference_token(peer_pref.get("preference") or "allow")
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
            result["reasons"].extend(self._unique_tokens(approval_reasons))
            return result
        result["decision"] = "auto_enlist"
        result["action"] = "auto"
        result["reasons"].append("policy_allows_auto_enlist")
        return result

    def autonomous_offload_request_id(self, evaluation: dict) -> str:
        eligible = list(evaluation.get("eligible_candidates") or [])
        peer_ids = sorted(str(item.get("peer_id") or "") for item in eligible if str(item.get("peer_id") or "").strip())
        placement = dict(evaluation.get("placement") or {})
        pressure = dict(evaluation.get("pressure") or {})
        basis = json.dumps(
            {
                "node_id": self.mesh.node_id,
                "peers": peer_ids,
                "pressure": pressure.get("pressure"),
                "workload_class": placement.get("workload_class"),
                "gpu_required": placement.get("gpu_required"),
            },
            sort_keys=True,
        )
        return "autonomy-offload-" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]

    def apply_autonomous_offload_approval(
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
        workload_class = self._normalize_workload_class(autonomy.get("workload_class") or "default")
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
            self.mesh.publish_notification(
                notification_type="helper.autonomy.applied",
                priority="high",
                title="Autonomous offload applied",
                body=f"Enlisted {len(enlisted)} helper peer(s) after approval.",
                target_peer_id=self.mesh.node_id,
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
            "peer_id": self.mesh.node_id,
            "evaluation": evaluation,
            "status": decision,
            "generated_at": self._utcnow(),
        }
        if decision in {"noop", "suggest"}:
            return result
        if decision == "request_approval":
            eligible = list(evaluation.get("eligible_candidates") or [])
            request = self.mesh.create_approval_request(
                title="Approve autonomous helper offload",
                summary=f"Pressure is {evaluation.get('pressure', {}).get('pressure') or 'unknown'} and OCP wants to enlist {len(eligible)} helper peer(s).",
                action_type="autonomous.offload",
                severity="high" if str(evaluation.get("pressure", {}).get("pressure") or "") == "saturated" else "normal",
                request_id=self.autonomous_offload_request_id(evaluation),
                requested_by_peer_id=self.mesh.node_id,
                requested_by_agent_id=actor_agent_id,
                target_peer_id=self.mesh.node_id,
                target_device_classes=policy.get("target_device_classes") or ["full", "light", "micro"],
                metadata={
                    "autonomous_offload": {
                        "peer_ids": [item.get("peer_id") for item in eligible],
                        "mode": "on_demand",
                        "role": "gpu_helper" if any(dict(item.get("compute_profile") or {}).get("gpu_capable") for item in eligible) else "helper",
                        "pressure": evaluation.get("pressure"),
                        "placement": evaluation.get("placement"),
                        "workload_class": self._normalize_workload_class(dict(evaluation.get("placement") or {}).get("workload_class") or "default"),
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
            self.mesh.publish_notification(
                notification_type="helper.autonomy.enlisted",
                priority="high",
                title="Autonomous helper offload active",
                body=f"Enlisted {len(auto_result.get('enlisted') or [])} helper peer(s) for local pressure relief.",
                target_peer_id=self.mesh.node_id,
                target_device_classes=policy.get("target_device_classes") or ["full", "light", "micro"],
                metadata={"peer_ids": [item.get("peer_id") for item in auto_result.get("enlisted") or []]},
            )
        return result
