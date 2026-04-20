from __future__ import annotations

import json
import uuid


class MeshSchedulerService:
    """Placement and decision persistence for SovereignMesh."""

    def __init__(self, mesh, *, utcnow):
        self.mesh = mesh
        self._utcnow = utcnow

    def record_scheduler_decision(
        self,
        *,
        request_id: str = "",
        job_id: str = "",
        job_kind: str = "",
        decision: dict | None = None,
    ) -> dict:
        decision = dict(decision or {})
        selected = dict(decision.get("selected") or {})
        decision_id = str(uuid.uuid4())
        with self.mesh._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_scheduler_decisions
                (id, request_id, job_id, job_kind, status, strategy, target_type, peer_id, score, placement, selected, candidates, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    (request_id or "").strip(),
                    (job_id or "").strip(),
                    (job_kind or "").strip(),
                    (decision.get("status") or "").strip() or "placed",
                    (decision.get("strategy") or "").strip(),
                    (selected.get("target_type") or "").strip(),
                    (selected.get("peer_id") or "").strip(),
                    int(selected.get("score") or 0),
                    json.dumps(dict(decision.get("placement") or {})),
                    json.dumps(selected),
                    json.dumps(list(decision.get("candidates") or [])),
                    self._utcnow(),
                ),
            )
            conn.execute(
                """
                DELETE FROM mesh_scheduler_decisions
                WHERE id NOT IN (
                    SELECT id
                    FROM mesh_scheduler_decisions
                    ORDER BY created_at DESC, id DESC
                    LIMIT 500
                )
                """
            )
            row = conn.execute("SELECT * FROM mesh_scheduler_decisions WHERE id=?", (decision_id,)).fetchone()
            conn.commit()
        return self.mesh._row_to_scheduler_decision(row)

    def attach_job_id(self, decision_id: str, job_id: str) -> dict | None:
        decision_id = (decision_id or "").strip()
        if not decision_id:
            return None
        with self.mesh._conn() as conn:
            conn.execute(
                "UPDATE mesh_scheduler_decisions SET job_id=? WHERE id=?",
                ((job_id or "").strip(), decision_id),
            )
            row = conn.execute("SELECT * FROM mesh_scheduler_decisions WHERE id=?", (decision_id,)).fetchone()
            conn.commit()
        return self.mesh._row_to_scheduler_decision(row) if row is not None else None

    def list_scheduler_decisions(self, *, limit: int = 25, status: str = "", target_type: str = "") -> dict:
        clauses = []
        params: list = []
        if status:
            clauses.append("status=?")
            params.append((status or "").strip())
        if target_type:
            clauses.append("target_type=?")
            params.append((target_type or "").strip())
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.mesh._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM mesh_scheduler_decisions
                {where}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                tuple(params + [max(1, int(limit or 25))]),
            ).fetchall()
        decisions = [self.mesh._row_to_scheduler_decision(row) for row in rows]
        return {"peer_id": self.mesh.node_id, "count": len(decisions), "decisions": decisions}

    def trust_score(self, trust_tier: str) -> int:
        return {
            "self": 1000,
            "trusted": 800,
            "partner": 600,
            "market": 250,
            "public": 100,
            "blocked": -10000,
        }.get(self.mesh._normalize_trust_tier(trust_tier), 0)

    def trust_rank(self, trust_tier: str) -> int:
        return {
            "blocked": 0,
            "public": 1,
            "market": 2,
            "partner": 3,
            "trusted": 4,
            "self": 5,
        }.get(self.mesh._normalize_trust_tier(trust_tier), 0)

    def trust_meets_floor(self, trust_tier: str, floor: str) -> bool:
        if not str(floor or "").strip():
            return True
        return self.trust_rank(trust_tier) >= self.trust_rank(floor)

    def local_load_summary(self) -> dict:
        queue_metrics = self.mesh.queue_metrics()
        workers = dict(queue_metrics.get("workers") or {})
        counts = dict(queue_metrics.get("counts") or {})
        queue_depth = int(counts.get("queued", 0) or 0) + int(counts.get("inflight", 0) or 0)
        total_slots = int(workers.get("total_slots", 0) or 0)
        active_attempts = int(workers.get("active_attempts", 0) or 0)
        available_slots = int(workers.get("available_slots", 0) or 0)
        utilization = round(active_attempts / max(1, total_slots), 2) if total_slots > 0 else None
        return {
            "queue_depth": queue_depth,
            "queued": int(counts.get("queued", 0) or 0),
            "inflight": int(counts.get("inflight", 0) or 0),
            "pressure": str(queue_metrics.get("pressure") or "idle"),
            "backlog_ratio": queue_metrics.get("backlog_ratio"),
            "scheduler_penalty": int(queue_metrics.get("scheduler_penalty") or 0),
            "total_slots": total_slots,
            "active_attempts": active_attempts,
            "available_slots": available_slots,
            "utilization": utilization,
        }

    def peer_load_summary(self, peer: dict | None) -> dict:
        metrics = self.mesh._peer_queue_metrics(peer)
        counts = dict(metrics.get("counts") or {})
        worker_metrics = dict(metrics.get("workers") or {})
        queue_depth = int(counts.get("queued", 0) or 0) + int(counts.get("inflight", 0) or 0)
        total_slots = int(worker_metrics.get("total_slots", 0) or 0)
        active_attempts = int(worker_metrics.get("active_attempts", 0) or 0)
        available_slots = int(worker_metrics.get("available_slots", 0) or 0)
        if total_slots <= 0:
            total_slots = self.mesh._peer_worker_count(peer)
        if available_slots <= 0:
            available_slots = self.mesh._peer_worker_slots(peer)
        utilization = round(active_attempts / max(1, total_slots), 2) if total_slots > 0 else None
        return {
            "queue_depth": queue_depth,
            "queued": int(counts.get("queued", 0) or 0),
            "inflight": int(counts.get("inflight", 0) or 0),
            "pressure": str(metrics.get("pressure") or "unknown"),
            "backlog_ratio": metrics.get("backlog_ratio"),
            "scheduler_penalty": int(metrics.get("scheduler_penalty") or 0),
            "total_slots": total_slots,
            "active_attempts": active_attempts,
            "available_slots": available_slots,
            "utilization": utilization,
        }

    def local_reliability_summary(self, *, limit: int = 40) -> dict:
        with self.mesh._conn() as conn:
            job_rows = conn.execute(
                """
                SELECT status
                FROM mesh_jobs
                WHERE target_peer_id=? AND status IN ('completed', 'failed')
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (self.mesh.node_id, max(1, int(limit))),
            ).fetchall()
            retry_row = conn.execute(
                """
                SELECT COUNT(*) AS retry_count
                FROM mesh_job_attempts
                WHERE status='failed' AND job_id IN (
                    SELECT id
                    FROM mesh_jobs
                    WHERE target_peer_id=?
                )
                """,
                (self.mesh.node_id,),
            ).fetchone()
        completed = sum(1 for row in job_rows if (row["status"] or "").strip() == "completed")
        failed = sum(1 for row in job_rows if (row["status"] or "").strip() == "failed")
        retries = int((retry_row["retry_count"] if retry_row is not None else 0) or 0)
        total = completed + failed
        if total <= 0:
            success_rate = None
            score = 0
        else:
            success_rate = round(completed / total, 4)
            score = int(round(((completed / total) - 0.5) * 160)) - min(retries, 5) * 4
        return {
            "source": "local_jobs",
            "completed": completed,
            "failed": failed,
            "retried": retries,
            "total": total,
            "success_rate": success_rate,
            "score": max(-120, min(120, score)),
        }

    def peer_reliability_summary(self, peer: dict | None, *, limit: int = 40) -> dict:
        peer_id = (peer or {}).get("peer_id") or self.mesh.node_id
        if peer_id == self.mesh.node_id:
            return self.local_reliability_summary(limit=limit)
        with self.mesh._conn() as conn:
            rows = conn.execute(
                """
                SELECT event_type
                FROM mesh_remote_events
                WHERE peer_id=? AND event_type IN ('mesh.job.completed', 'mesh.job.failed', 'mesh.job.retry_scheduled')
                ORDER BY remote_seq DESC
                LIMIT ?
                """,
                ((peer_id or "").strip(), max(1, int(limit))),
            ).fetchall()
        completed = sum(1 for row in rows if (row["event_type"] or "").strip() == "mesh.job.completed")
        failed = sum(1 for row in rows if (row["event_type"] or "").strip() == "mesh.job.failed")
        retries = sum(1 for row in rows if (row["event_type"] or "").strip() == "mesh.job.retry_scheduled")
        total = completed + failed
        if total <= 0:
            success_rate = None
            score = 0
        else:
            success_rate = round(completed / total, 4)
            score = int(round(((completed / total) - 0.5) * 160)) - min(retries, 5) * 4
        return {
            "source": "remote_events",
            "completed": completed,
            "failed": failed,
            "retried": retries,
            "total": total,
            "success_rate": success_rate,
            "score": max(-120, min(120, score)),
        }

    def local_candidate_score(self, job: dict) -> tuple[int, list[str]]:
        requirements = dict(job.get("requirements") or {})
        placement = self.mesh._normalized_placement(job)
        kind = (job.get("kind") or "").strip().lower()
        dispatch_mode = self.mesh._job_dispatch_mode(kind, job)
        requires_worker = self.mesh._dispatch_mode_requires_worker(kind, dispatch_mode)
        sync_resilience = self.mesh._job_sync_resilience(job)
        reasons = ["local-first", f"queue_class={placement['queue_class']}"]
        reliability = self.local_reliability_summary()
        load = self.local_load_summary()
        device_profile = dict(self.mesh.device_profile)
        reasons.extend(self.mesh._device_profile_schedule_reasons(device_profile))
        if placement["stay_local"]:
            reasons.append("stay_local")
        if placement["latency_sensitive"]:
            reasons.append("latency_sensitive")
        if placement["batch"]:
            reasons.append("batch")
        reasons.append(f"execution_class={placement['execution_class']}")
        reasons.append(f"backpressure={load['pressure']}")
        reasons.append(f"local_queue_depth={load['queue_depth']}")
        reasons.append(f"available_slots={load['available_slots']}")
        reasons.append(
            "reliability="
            + (
                f"{reliability['completed']}/{reliability['total']}"
                if reliability["total"] > 0
                else "unknown"
            )
        )
        reasons.append(f"resume_capable={str(sync_resilience['resume_capable']).lower()}")
        if not self.mesh._requirements_satisfied(requirements):
            return -10000, reasons + ["requirements_unmet"]
        device_ok, device_reason = self.mesh._device_profile_allows_job(device_profile, job, requires_worker=requires_worker)
        if not device_ok:
            return -10000, reasons + [device_reason]
        device_score, device_reasons = self.mesh._device_profile_schedule_score(
            device_profile,
            placement,
            requires_worker=requires_worker,
            remote=False,
            sync_resilience=sync_resilience,
        )
        if device_score <= -10000:
            return -10000, reasons + device_reasons
        reasons.extend(device_reasons)
        if placement["max_local_queue_depth"] is not None and load["queue_depth"] > placement["max_local_queue_depth"]:
            return -10000, reasons + ["local_backlog_limit_exceeded"]
        if requires_worker:
            workers = self.mesh.list_workers(limit=100)["workers"]
            matching_workers = [worker for worker in workers if self.mesh._requirements_satisfied_for_worker(requirements, worker)]
            if not matching_workers:
                return -10000, reasons + ["no_matching_local_worker"]
            available_slots = sum(
                max(0, int(worker.get("max_concurrent_jobs") or 1) - int(worker.get("active_attempts") or 0))
                for worker in matching_workers
                if worker.get("status") in {"active", "ready"}
            )
            reasons.append(f"matching_workers={len(matching_workers)}")
            reasons.append(f"available_slots={available_slots}")
            score = (
                self.trust_score("self")
                + 150
                + (available_slots * 20)
                - (self.mesh._local_queue_depth() * 10)
                + reliability["score"]
                + device_score
                - int(load.get("scheduler_penalty") or 0)
            )
            if placement["stay_local"]:
                score += 300
            if placement["latency_sensitive"]:
                score += 140
            if placement["batch"]:
                score -= 20
            if placement["prefer_low_backlog"]:
                score -= load["queue_depth"] * 18
                reasons.append("low_backlog_preferred")
            if placement["execution_class"] == "throughput":
                score += load["available_slots"] * 25
                score -= load["queue_depth"] * 8
                reasons.append("execution_class_throughput")
            elif placement["execution_class"] == "isolation":
                score -= 220
                reasons.append("execution_class_isolation_local_penalty")
            elif placement["execution_class"] == "latency":
                score += 90
                reasons.append("execution_class_latency")
            return score, reasons
        score = self.trust_score("self") + 120 + reliability["score"] + device_score - int(load.get("scheduler_penalty") or 0)
        if placement["stay_local"]:
            score += 300
        if placement["latency_sensitive"]:
            score += 140
        if placement["batch"]:
            score -= 20
        if placement["prefer_low_backlog"]:
            score -= load["queue_depth"] * 18
            reasons.append("low_backlog_preferred")
        if placement["execution_class"] == "throughput":
            score += load["available_slots"] * 25
            score -= load["queue_depth"] * 8
            reasons.append("execution_class_throughput")
        elif placement["execution_class"] == "isolation":
            score -= 220
            reasons.append("execution_class_isolation_local_penalty")
        elif placement["execution_class"] == "latency":
            score += 90
            reasons.append("execution_class_latency")
        return score, reasons + ["inline_capable"]

    def peer_candidate_score(self, peer: dict, job: dict) -> tuple[int, list[str]]:
        requirements = dict(job.get("requirements") or {})
        policy = self.mesh._normalize_policy(job.get("policy") or {})
        placement = self.mesh._normalized_placement(job)
        reliability = self.peer_reliability_summary(peer)
        load = self.peer_load_summary(peer)
        kind = (job.get("kind") or "").strip().lower()
        dispatch_mode = self.mesh._job_dispatch_mode(kind, job)
        requires_worker = self.mesh._dispatch_mode_requires_worker(kind, dispatch_mode)
        sync_resilience = self.mesh._job_sync_resilience(job)
        device_profile = self.mesh._peer_device_profile(peer)
        reasons = [f"trust_tier={peer.get('trust_tier') or 'trusted'}", f"queue_class={placement['queue_class']}"]
        reasons.append(f"execution_class={placement['execution_class']}")
        reasons.append(f"remote_queue_depth={load['queue_depth']}")
        reasons.append(f"remote_pressure={load['pressure']}")
        reasons.extend(self.mesh._device_profile_schedule_reasons(device_profile))
        reasons.append(
            "reliability="
            + (
                f"{reliability['completed']}/{reliability['total']}"
                if reliability["total"] > 0
                else "unknown"
            )
        )
        reasons.append(f"resume_capable={str(sync_resilience['resume_capable']).lower()}")
        if placement["stay_local"]:
            return -10000, reasons + ["stay_local"]
        if placement["trust_floor"] and not self.trust_meets_floor(peer.get("trust_tier") or "trusted", placement["trust_floor"]):
            return -10000, reasons + ["trust_floor_denied"]
        if placement["required_peer_ids"] and peer.get("peer_id") not in set(placement["required_peer_ids"]):
            return -10000, reasons + ["peer_not_required"]
        if placement["avoid_public"] and self.mesh._peer_is_public_lane(peer):
            return -10000, reasons + ["avoid_public"]
        if placement["max_peer_queue_depth"] is not None and load["queue_depth"] > placement["max_peer_queue_depth"]:
            return -10000, reasons + ["peer_backlog_limit_exceeded"]
        if not self.mesh._policy_allows_peer(policy, peer):
            return -10000, reasons + ["policy_denied"]
        needed = {str(item).strip() for item in (requirements.get("capabilities") or []) if str(item).strip()}
        if not needed.issubset(self.mesh._peer_capabilities(peer)):
            return -10000, reasons + ["requirements_unmet"]
        device_ok, device_reason = self.mesh._device_profile_allows_job(device_profile, job, requires_worker=requires_worker)
        if not device_ok:
            return -10000, reasons + [device_reason]
        device_score, device_reasons = self.mesh._device_profile_schedule_score(
            device_profile,
            placement,
            requires_worker=requires_worker,
            remote=True,
            sync_resilience=sync_resilience,
        )
        if device_score <= -10000:
            return -10000, reasons + device_reasons
        reasons.extend(device_reasons)
        if requires_worker:
            worker_count = self.mesh._peer_worker_count(peer)
            available_slots = self.mesh._peer_worker_slots(peer)
            reasons.append(f"remote_workers={worker_count}")
            reasons.append(f"available_slots={available_slots}")
            if worker_count <= 0:
                return -10000, reasons + ["no_remote_workers_advertised"]
        score = self.trust_score(peer.get("trust_tier") or "trusted") + reliability["score"] + device_score
        if peer.get("status") == "connected":
            score += 40
            reasons.append("connected")
        if peer.get("heartbeat", {}).get("status") == "active":
            score += 30
            reasons.append("active_heartbeat")
        if (peer.get("sync_state") or {}).get("last_sync_error"):
            score -= 80
            reasons.append("sync_error_penalty")
        if reliability["failed"] > reliability["completed"] and reliability["total"] > 0:
            reasons.append("reliability_penalty")
        elif reliability["completed"] > 0:
            reasons.append("reliability_bonus")
        if placement["latency_sensitive"]:
            score -= 80
            reasons.append("latency_sensitive_penalty")
        if placement["batch"]:
            score += 40
            reasons.append("batch_friendly")
        if placement["preferred_trust_tiers"] and self.mesh._normalize_trust_tier(peer.get("trust_tier") or "trusted") in set(placement["preferred_trust_tiers"]):
            score += 140
            reasons.append("preferred_trust_tier")
        score -= load["queue_depth"] * 12
        if load["pressure"] == "saturated":
            score -= 120
            reasons.append("remote_saturated")
        elif load["pressure"] == "elevated":
            score -= 60
            reasons.append("remote_elevated")
        if placement["prefer_low_backlog"]:
            score += max(0, 120 - (load["queue_depth"] * 20))
            reasons.append("low_backlog_preferred")
        if placement["execution_class"] == "throughput":
            score += load["available_slots"] * 20
            score += 60 if load["queue_depth"] <= 1 else 0
            reasons.append("execution_class_throughput")
        elif placement["execution_class"] == "isolation":
            if self.mesh._normalize_trust_tier(peer.get("trust_tier") or "trusted") in {"trusted", "partner"} and not self.mesh._peer_is_public_lane(peer):
                score += 220
                reasons.append("execution_class_isolation_preferred")
            elif self.mesh._peer_is_public_lane(peer):
                score -= 180
                reasons.append("execution_class_isolation_public_penalty")
        elif placement["execution_class"] == "latency":
            score -= 40
            reasons.append("execution_class_latency_penalty")
        if self.mesh._peer_is_public_lane(peer):
            reasons.append("public_lane")
            if placement["latency_sensitive"]:
                score -= 60
            if placement["batch"]:
                score += 20
        if placement["preferred_peer_ids"] and peer.get("peer_id") in set(placement["preferred_peer_ids"]):
            score += 250
            reasons.append("placement_preferred_peer")
        score += self.mesh._peer_worker_slots(peer) * 15
        enlistment_meta = dict((peer.get("metadata") or {}).get("enlistment") or {})
        enlistment_state = str(enlistment_meta.get("state") or "").strip().lower()
        enlistment_role = str(enlistment_meta.get("role") or "").strip().lower()
        if enlistment_state == "enlisted":
            score += 180
            reasons.append("helper_enlisted_bonus")
            if enlistment_role == "gpu_helper" and placement.get("workload_class") in {"gpu_inference", "gpu_training", "mixed"}:
                score += 120
                reasons.append("gpu_helper_preferred")
        elif enlistment_state == "draining":
            score -= 140
            reasons.append("helper_draining_penalty")
        return score, reasons

    def select_execution_target(
        self,
        job: dict,
        *,
        request_id: str = "",
        preferred_peer_id: str = "",
        allow_local: bool = True,
        allow_remote: bool = True,
    ) -> dict:
        normalized_job = dict(job or {})
        placement = self.mesh._normalized_placement(normalized_job)
        candidates = []
        if allow_local:
            score, reasons = self.local_candidate_score(normalized_job)
            candidates.append(
                {
                    "target_type": "local",
                    "peer_id": self.mesh.node_id,
                    "score": score,
                    "reasons": reasons,
                    "selected": False,
                }
            )
        if allow_remote:
            for peer in self.mesh.list_peers(limit=500).get("peers", []):
                score, reasons = self.peer_candidate_score(peer, normalized_job)
                if preferred_peer_id and peer["peer_id"] == preferred_peer_id:
                    score += 500
                    reasons = list(reasons) + ["preferred_peer"]
                candidates.append(
                    {
                        "target_type": "peer",
                        "peer_id": peer["peer_id"],
                        "score": score,
                        "reasons": reasons,
                        "selected": False,
                    }
                )
        candidates.sort(key=lambda item: (item["score"], item["target_type"] == "local"), reverse=True)
        selected = next((candidate for candidate in candidates if candidate["score"] > -10000), None)
        if selected is None:
            decision = {
                "status": "unplaced",
                "strategy": "trust-capability-load-v1.2",
                "placement": placement,
                "selected": {},
                "candidates": candidates,
            }
            persisted = self.record_scheduler_decision(
                request_id=request_id,
                job_kind=(normalized_job.get("kind") or "").strip(),
                decision=decision,
            )
            decision["decision_id"] = persisted["id"]
            self.mesh._record_event(
                "mesh.scheduler.unplaced",
                peer_id=self.mesh.node_id,
                payload={
                    "decision_id": decision["decision_id"],
                    "job_kind": normalized_job.get("kind") or "",
                    "placement": placement,
                    "candidates": candidates,
                },
            )
            return decision
        selected["selected"] = True
        decision = {
            "status": "placed",
            "strategy": "trust-capability-load-v1.2",
            "placement": placement,
            "selected": selected,
            "candidates": candidates,
        }
        persisted = self.record_scheduler_decision(
            request_id=request_id,
            job_kind=(normalized_job.get("kind") or "").strip(),
            decision=decision,
        )
        decision["decision_id"] = persisted["id"]
        self.mesh._record_event(
            "mesh.scheduler.decision",
            peer_id=selected["peer_id"],
            payload={
                "decision_id": decision["decision_id"],
                "job_kind": normalized_job.get("kind") or "",
                "target_type": selected["target_type"],
                "score": selected["score"],
                "reasons": selected["reasons"],
                "placement": placement,
            },
        )
        return decision
