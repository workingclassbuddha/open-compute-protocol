from __future__ import annotations

import json
import time
import uuid
import datetime as dt
from typing import Any, Callable, Optional

ROUTE_FRESH_SECONDS = 300
ROUTE_STALE_SECONDS = 1800


class MeshAutonomyService:
    """Mesh-level autonomy coordinator.

    This service owns route health and the one-button activation flow. It
    deliberately delegates scheduling, helper enlistment, approvals, missions,
    and notifications back to the existing mesh services so the alpha remains
    additive rather than becoming a parallel runtime.
    """

    def __init__(
        self,
        mesh,
        *,
        peer_client_type,
        loads_json: Callable[[Any, Any], Any],
        normalize_base_url: Callable[..., str],
        normalize_trust_tier: Callable[[Optional[str]], str],
        utcnow: Callable[[], str],
    ):
        self.mesh = mesh
        self._peer_client_type = peer_client_type
        self._loads_json = loads_json
        self._normalize_base_url = normalize_base_url
        self._normalize_trust_tier = normalize_trust_tier
        self._utcnow = utcnow

    def route_candidates_for_peer(self, peer: dict, *, base_url: str = "") -> list[dict[str, Any]]:
        peer = dict(peer or {})
        peer_id = str(peer.get("peer_id") or "").strip()
        metadata = dict(peer.get("metadata") or {})
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()

        def append(raw_url: str, *, source: str, extra: Optional[dict] = None) -> None:
            normalized = self._normalize_base_url(str(raw_url or "").strip())
            if not normalized or normalized in seen:
                return
            seen.add(normalized)
            payload = {
                "base_url": normalized,
                "source": source,
                "status": "unknown",
                "latency_ms": None,
                "checked_at": "",
                "last_success_at": "",
                "last_error": "",
            }
            payload.update(dict(extra or {}))
            payload["base_url"] = normalized
            payload["source"] = str(payload.get("source") or source).strip() or source
            candidates.append(payload)

        append(base_url, source="explicit")
        append(metadata.get("last_reachable_base_url") or "", source="last_reachable")
        for item in list(metadata.get("route_candidates") or []):
            if not isinstance(item, dict):
                continue
            append(item.get("base_url") or "", source=str(item.get("source") or "history").strip(), extra=item)
        if peer_id:
            discovery = self.mesh._discovery_candidate_by_peer_id(peer_id)
            if discovery:
                append(discovery.get("base_url") or discovery.get("endpoint_url") or "", source="discovery")
        append(peer.get("endpoint_url") or "", source="advertised")
        return candidates

    def _peer_from_manifest(self, manifest: dict) -> tuple[str, dict]:
        card = dict((manifest or {}).get("organism_card") or {})
        peer_id = str(card.get("organism_id") or card.get("node_id") or "").strip()
        return peer_id, card

    def _event(self, event_type: str, *, peer_id: str = "", request_id: str = "", payload: Optional[dict] = None) -> None:
        try:
            self.mesh._record_event(event_type, peer_id=peer_id, request_id=request_id, payload=dict(payload or {}))
        except Exception:
            self.mesh.logger.debug("mesh autonomy event recording failed", exc_info=True)

    def _action(
        self,
        actions: list[dict[str, Any]],
        kind: str,
        status: str,
        summary: str,
        *,
        peer_id: str = "",
        details: Optional[dict] = None,
        request_id: str = "",
    ) -> dict[str, Any]:
        action = {
            "id": str(uuid.uuid4()),
            "kind": str(kind or "autonomy.action").strip(),
            "status": str(status or "ok").strip(),
            "summary": str(summary or "").strip(),
            "peer_id": str(peer_id or "").strip(),
            "details": dict(details or {}),
            "created_at": self._utcnow(),
        }
        actions.append(action)
        self._event("mesh.autonomy.action", peer_id=peer_id or self.mesh.node_id, request_id=request_id, payload=action)
        return action

    def _route_summary(self, route: dict) -> str:
        peer_label = route.get("display_name") or route.get("peer_id") or "Peer"
        status = str(route.get("status") or "unknown").strip().lower()
        best = route.get("best_route") or route.get("last_reachable_base_url") or ""
        freshness = str(route.get("freshness") or "").strip().lower()
        if status == "reachable":
            if freshness == "stale":
                return f"{peer_label} was reachable at {best}, but the proof is stale. Probe it before dispatch."
            if freshness == "aging":
                return f"{peer_label} is reachable at {best}; route proof is aging."
            return f"{peer_label} is reachable at {best}."
        if status == "unreachable":
            hint = route.get("operator_hint") or route.get("last_error") or "last route probe failed"
            return f"{peer_label} needs attention: {hint}"
        return f"{peer_label} has no proven route yet."

    def _parse_time(self, value: Any) -> Optional[dt.datetime]:
        token = str(value or "").strip()
        if not token:
            return None
        try:
            parsed = dt.datetime.fromisoformat(token.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)

    def _now_dt(self) -> dt.datetime:
        parsed = self._parse_time(self._utcnow())
        return parsed or dt.datetime.now(dt.timezone.utc).replace(microsecond=0)

    def _format_time(self, value: dt.datetime) -> str:
        return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _route_age_seconds(self, *timestamps: Any) -> Optional[int]:
        parsed = [value for value in (self._parse_time(item) for item in timestamps) if value is not None]
        if not parsed:
            return None
        latest = max(parsed)
        return max(0, int((self._now_dt() - latest).total_seconds()))

    def _route_freshness(self, *, status: str, checked_at: Any = "", last_success_at: Any = "") -> str:
        if str(status or "").strip().lower() != "reachable":
            return "failed" if str(status or "").strip().lower() == "unreachable" else "unknown"
        age = self._route_age_seconds(last_success_at, checked_at)
        if age is None:
            return "unknown"
        if age <= ROUTE_FRESH_SECONDS:
            return "fresh"
        if age <= ROUTE_STALE_SECONDS:
            return "aging"
        return "stale"

    def _route_repair_hint(self, error: str, base_url: str) -> str:
        text = str(error or "").strip()
        lowered = text.lower()
        target = str(base_url or "").strip() or "the peer URL"
        if not text:
            return "Probe the route again or reconnect the peer."
        if "timed out" in lowered or "timeout" in lowered:
            return f"{target} did not answer in time. Make sure OCP is running there and allow Python/OCP through the firewall on that device."
        if "connection refused" in lowered or "actively refused" in lowered:
            return f"{target} is reachable but OCP is not listening on that port. Start OCP on the peer or check the port."
        if "no route to host" in lowered or "network is unreachable" in lowered or "name or service not known" in lowered:
            return f"{target} is not reachable from this device. Put both devices on the same Wi-Fi or use the peer's current LAN address."
        if "route reached" in lowered and "expected" in lowered:
            return "This URL answered as a different OCP node. Re-scan nearby devices and use the route tied to the expected peer."
        if "connection reset" in lowered:
            return f"{target} accepted the connection then closed it. Restart OCP on the peer and try again."
        return f"Route probe failed for {target}: {text}"

    def _next_probe_after(self, *, failure_count: int, checked_at: Any) -> str:
        base = self._parse_time(checked_at) or self._now_dt()
        delay_seconds = min(300, 15 * (2 ** max(0, min(5, int(failure_count or 1) - 1))))
        return self._format_time(base + dt.timedelta(seconds=delay_seconds))

    def _route_is_usable(self, peer_or_route: dict) -> bool:
        metadata = dict(peer_or_route.get("metadata") or {})
        route_health = dict(metadata.get("route_health") or {})
        status = str(route_health.get("status") or peer_or_route.get("status") or "").strip().lower()
        freshness = str(peer_or_route.get("freshness") or route_health.get("freshness") or "").strip().lower()
        has_route = bool(
            peer_or_route.get("best_route")
            or peer_or_route.get("last_reachable_base_url")
            or metadata.get("last_reachable_base_url")
            or route_health.get("best_route")
        )
        if status in {"unreachable", "failed"} or freshness in {"stale", "failed"}:
            return False
        return status == "reachable" or (has_route and freshness != "stale")

    def _merge_route_candidate(self, peer_id: str, candidate: dict) -> dict:
        peer_token = str(peer_id or "").strip()
        if not peer_token:
            return {}
        peer = self.mesh._row_to_peer(self.mesh._get_peer_row(peer_token))
        if not peer:
            return {}
        metadata = dict(peer.get("metadata") or {})
        candidate = dict(candidate or {})
        base_url = self._normalize_base_url(candidate.get("base_url") or "")
        if not base_url:
            return peer
        candidate["base_url"] = base_url
        previous = next(
            (
                dict(item)
                for item in list(metadata.get("route_candidates") or [])
                if isinstance(item, dict) and self._normalize_base_url(item.get("base_url") or "") == base_url
            ),
            {},
        )
        is_reachable = candidate.get("status") == "reachable"
        failure_count = 0 if is_reachable else int(previous.get("failure_count") or 0) + 1
        candidate["failure_count"] = failure_count
        candidate["operator_hint"] = "" if is_reachable else self._route_repair_hint(candidate.get("last_error") or "", base_url)
        candidate["next_probe_after"] = "" if is_reachable else self._next_probe_after(
            failure_count=failure_count,
            checked_at=candidate.get("checked_at") or self._utcnow(),
        )
        candidate["freshness"] = self._route_freshness(
            status=str(candidate.get("status") or ""),
            checked_at=candidate.get("checked_at") or "",
            last_success_at=candidate.get("last_success_at") or "",
        )
        existing = [
            dict(item)
            for item in list(metadata.get("route_candidates") or [])
            if isinstance(item, dict) and self._normalize_base_url(item.get("base_url") or "") != base_url
        ]
        metadata["route_candidates"] = [candidate, *existing][:8]
        metadata["last_route_probe_at"] = candidate.get("checked_at") or self._utcnow()
        route_health = dict(metadata.get("route_health") or {})
        route_health.update(
            {
                "status": candidate.get("status") or "unknown",
                "best_route": base_url if candidate.get("status") == "reachable" else route_health.get("best_route", ""),
                "checked_at": candidate.get("checked_at") or self._utcnow(),
                "last_error": candidate.get("last_error") or "",
                "latency_ms": candidate.get("latency_ms"),
                "source": candidate.get("source") or "",
                "freshness": candidate.get("freshness") or "unknown",
                "failure_count": failure_count,
                "next_probe_after": candidate.get("next_probe_after") or "",
                "operator_hint": candidate.get("operator_hint") or "",
            }
        )
        if candidate.get("status") == "reachable":
            route_health["last_success_at"] = candidate.get("last_success_at") or candidate.get("checked_at") or self._utcnow()
            metadata["last_reachable_base_url"] = base_url
        metadata["route_health"] = route_health
        status = "connected" if candidate.get("status") == "reachable" else None
        return self.mesh._update_peer_record(peer_token, metadata=metadata, status=status)

    def probe_routes(
        self,
        *,
        peer_id: str = "",
        base_url: str = "",
        timeout: float = 2.0,
        limit: int = 8,
    ) -> dict[str, Any]:
        peer_token = str(peer_id or "").strip()
        explicit_url = self._normalize_base_url(base_url or "")
        if not peer_token and not explicit_url:
            results = [
                self.probe_routes(peer_id=str(peer.get("peer_id") or ""), timeout=timeout, limit=4)
                for peer in list(self.mesh.list_peers(limit=max(1, int(limit or 8))).get("peers") or [])
                if str(peer.get("peer_id") or "").strip()
            ]
            return {
                "status": "ok",
                "peer_id": "",
                "count": len(results),
                "reachable": sum(1 for item in results if int(item.get("reachable") or 0) > 0),
                "results": results,
                "generated_at": self._utcnow(),
            }

        peer = self.mesh._row_to_peer(self.mesh._get_peer_row(peer_token)) if peer_token else {}
        if not peer and peer_token:
            return {
                "status": "not_found",
                "peer_id": peer_token,
                "checked": 0,
                "reachable": 0,
                "best_route": "",
                "candidates": [],
                "operator_summary": f"{peer_token} is not known locally yet.",
                "generated_at": self._utcnow(),
            }

        candidates = self.route_candidates_for_peer(peer or {}, base_url=explicit_url)
        if not candidates and explicit_url:
            candidates = [{"base_url": explicit_url, "source": "explicit", "status": "unknown"}]

        checked: list[dict[str, Any]] = []
        best_route = ""
        observed_peer_id = peer_token
        for candidate in candidates[: max(1, int(limit or 8))]:
            base = self._normalize_base_url(candidate.get("base_url") or "")
            if not base:
                continue
            checked_at = self._utcnow()
            started = time.monotonic()
            try:
                manifest = self._peer_client_type(base, timeout=float(timeout or 2.0)).manifest()
                latency_ms = int((time.monotonic() - started) * 1000)
                found_peer_id, card = self._peer_from_manifest(manifest)
                if peer_token and found_peer_id and found_peer_id != peer_token:
                    raise ValueError(f"route reached {found_peer_id}, expected {peer_token}")
                observed_peer_id = observed_peer_id or found_peer_id
                record = {
                    **candidate,
                    "base_url": base,
                    "status": "reachable",
                    "latency_ms": latency_ms,
                    "checked_at": checked_at,
                    "last_success_at": checked_at,
                    "last_error": "",
                    "observed_peer_id": found_peer_id,
                    "failure_count": 0,
                    "next_probe_after": "",
                    "operator_hint": "",
                    "freshness": "fresh",
                }
                checked.append(record)
                if observed_peer_id:
                    self._merge_route_candidate(observed_peer_id, record)
                if found_peer_id and not self.mesh._get_peer_row(found_peer_id):
                    self.mesh._remember_discovery_candidate(
                        base_url=base,
                        peer_id=found_peer_id,
                        display_name=card.get("display_name") or found_peer_id,
                        endpoint_url=card.get("endpoint_url") or base,
                        status="discovered",
                        trust_tier=card.get("trust_tier") or "trusted",
                        device_profile=dict(card.get("device_profile") or manifest.get("device_profile") or {}),
                        manifest=manifest,
                        metadata={"route_probe": "reachable"},
                    )
                best_route = best_route or base
            except Exception as exc:
                record = {
                    **candidate,
                    "base_url": base,
                    "status": "unreachable",
                    "latency_ms": None,
                    "checked_at": checked_at,
                    "last_success_at": candidate.get("last_success_at") or "",
                    "last_error": str(exc),
                }
                previous_failures = int(candidate.get("failure_count") or 0)
                record["failure_count"] = previous_failures + 1
                record["next_probe_after"] = self._next_probe_after(
                    failure_count=record["failure_count"],
                    checked_at=checked_at,
                )
                record["operator_hint"] = self._route_repair_hint(str(exc), base)
                record["freshness"] = "failed"
                checked.append(record)
                if peer_token:
                    self._merge_route_candidate(peer_token, record)

        reachable = [item for item in checked if item.get("status") == "reachable"]
        status = "ok" if reachable else "attention_needed"
        result = {
            "status": status,
            "peer_id": observed_peer_id or peer_token,
            "checked": len(checked),
            "reachable": len(reachable),
            "best_route": best_route,
            "candidates": checked,
            "operator_hint": "" if reachable else self._route_repair_hint(
                checked[-1].get("last_error") if checked else "",
                peer_token or explicit_url,
            ),
            "operator_summary": (
                f"{observed_peer_id or peer_token or 'Route'} is reachable at {best_route}."
                if reachable
                else f"No working route found for {peer_token or explicit_url}. "
                f"{self._route_repair_hint(checked[-1].get('last_error') if checked else '', peer_token or explicit_url)}"
            ),
            "generated_at": self._utcnow(),
        }
        self._event(
            "mesh.route.probed",
            peer_id=observed_peer_id or peer_token or self.mesh.node_id,
            payload={
                "peer_id": observed_peer_id or peer_token,
                "best_route": best_route,
                "checked": len(checked),
                "reachable": len(reachable),
                "candidates": checked,
            },
        )
        return result

    def routes_health(self, *, limit: int = 50) -> dict[str, Any]:
        routes = []
        for peer in list(self.mesh.list_peers(limit=max(1, int(limit or 50))).get("peers") or []):
            metadata = dict(peer.get("metadata") or {})
            route_health = dict(metadata.get("route_health") or {})
            candidates = [dict(item) for item in list(metadata.get("route_candidates") or []) if isinstance(item, dict)]
            last_reachable = self._normalize_base_url(metadata.get("last_reachable_base_url") or "")
            best_route = self._normalize_base_url(route_health.get("best_route") or last_reachable or "")
            status = str(route_health.get("status") or ("reachable" if best_route else "unknown")).strip().lower()
            checked_at = route_health.get("checked_at") or metadata.get("last_route_probe_at") or ""
            last_success_at = route_health.get("last_success_at") or ""
            age_seconds = self._route_age_seconds(last_success_at, checked_at)
            freshness = self._route_freshness(status=status, checked_at=checked_at, last_success_at=last_success_at)
            route = {
                "peer_id": peer.get("peer_id") or "",
                "display_name": peer.get("display_name") or peer.get("peer_id") or "",
                "status": status,
                "freshness": freshness,
                "age_seconds": age_seconds,
                "best_route": best_route,
                "last_reachable_base_url": last_reachable,
                "checked_at": checked_at,
                "last_success_at": last_success_at,
                "last_error": route_health.get("last_error") or "",
                "failure_count": int(route_health.get("failure_count") or 0),
                "next_probe_after": route_health.get("next_probe_after") or "",
                "operator_hint": route_health.get("operator_hint") or "",
                "candidates": candidates,
            }
            route["operator_summary"] = self._route_summary(route)
            routes.append(route)
        healthy = sum(1 for route in routes if self._route_is_usable(route))
        return {
            "status": "ok",
            "peer_id": self.mesh.node_id,
            "count": len(routes),
            "healthy": healthy,
            "routes": routes,
            "operator_summary": (
                "No remote routes are known yet."
                if not routes
                else f"{healthy} of {len(routes)} peer route(s) are proven reachable."
            ),
            "generated_at": self._utcnow(),
        }

    def _row_to_run(self, row) -> dict[str, Any]:
        if row is None:
            return {}
        return {
            "id": str(row["id"] or "").strip(),
            "request_id": str(row["request_id"] or "").strip(),
            "mode": str(row["mode"] or "assisted").strip(),
            "status": str(row["status"] or "planned").strip(),
            "summary": str(row["summary"] or "").strip(),
            "actions": self._loads_json(row["actions"], []),
            "result": self._loads_json(row["result"], {}),
            "metadata": self._loads_json(row["metadata"], {}),
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
        }

    def latest_run(self) -> dict[str, Any]:
        with self.mesh._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mesh_autonomy_runs ORDER BY created_at DESC, updated_at DESC LIMIT 1"
            ).fetchone()
        return self._row_to_run(row)

    def run_by_request_id(self, request_id: str) -> dict[str, Any]:
        request_token = str(request_id or "").strip()
        if not request_token:
            return {}
        with self.mesh._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mesh_autonomy_runs WHERE request_id=? LIMIT 1",
                (request_token,),
            ).fetchone()
        return self._row_to_run(row)

    def _run_response(self, run: dict) -> dict[str, Any]:
        stored_result = dict(run.get("result") or {})
        helpers = dict(stored_result.get("helpers") or {})
        return {
            "status": run.get("status") or "running",
            "request_id": run.get("request_id") or "",
            "mode": run.get("mode") or "assisted",
            "summary": run.get("summary") or "",
            "operator_summary": run.get("summary") or "",
            "actions": list(run.get("actions") or []),
            "routes": stored_result.get("routes") or self.routes_health(limit=24),
            "proof": stored_result.get("proof_retry") or stored_result.get("proof") or {},
            "helpers": helpers,
            "approvals": helpers.get("approvals") or [],
            "run": run,
            "result": stored_result,
            "generated_at": self._utcnow(),
            "deduped": True,
        }

    def _record_run(
        self,
        run_id: str,
        *,
        request_id: str,
        mode: str,
        status: str,
        summary: str,
        actions: list[dict[str, Any]],
        result: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ) -> dict[str, Any]:
        now = self._utcnow()
        with self.mesh._conn() as conn:
            existing = conn.execute(
                """
                SELECT id FROM mesh_autonomy_runs
                WHERE id=? OR request_id=?
                ORDER BY CASE WHEN id=? THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (run_id, request_id, run_id),
            ).fetchone()
            if existing is not None:
                run_id = existing["id"]
                conn.execute(
                    """
                    UPDATE mesh_autonomy_runs
                    SET mode=?,
                        status=?,
                        summary=?,
                        actions=?,
                        result=?,
                        metadata=?,
                        updated_at=?
                    WHERE id=?
                    """,
                    (
                        mode,
                        status,
                        summary,
                        json.dumps(actions),
                        json.dumps(dict(result or {})),
                        json.dumps(dict(metadata or {})),
                        now,
                        run_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO mesh_autonomy_runs
                    (id, request_id, mode, status, summary, actions, result, metadata, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        request_id,
                        mode,
                        status,
                        summary,
                        json.dumps(actions),
                        json.dumps(dict(result or {})),
                        json.dumps(dict(metadata or {})),
                        now,
                        now,
                    ),
                )
            conn.commit()
            row = conn.execute("SELECT * FROM mesh_autonomy_runs WHERE id=?", (run_id,)).fetchone()
        run = self._row_to_run(row)
        self._event("mesh.autonomy.run", peer_id=self.mesh.node_id, request_id=request_id, payload=run)
        return run

    def status(self) -> dict[str, Any]:
        routes = self.routes_health(limit=50)
        try:
            helper_autonomy = dict(self.mesh.evaluate_autonomous_offload() or {})
        except Exception:
            helper_autonomy = {"decision": "noop", "reasons": ["helper_autonomy_unavailable"]}
        try:
            pressure = dict(self.mesh.mesh_pressure() or {})
        except Exception:
            pressure = {"pressure": "unknown", "needs_help": False}
        try:
            connectivity = dict(self.mesh.connectivity_diagnostics(limit=24) or {})
        except Exception:
            connectivity = {"status": "error", "share_advice": "Connectivity diagnostics are unavailable."}
        last_run = self.latest_run()
        healthy = int(routes.get("healthy") or 0)
        route_count = int(routes.get("count") or 0)
        if route_count and healthy == route_count:
            summary = "Mesh is strong: every known peer has a proven route."
        elif healthy:
            summary = f"Mesh is usable: {healthy} of {route_count} peer route(s) are proven."
        else:
            summary = "Autonomic Mesh is ready. Press Activate to discover, repair, enlist, and prove nearby devices."
        return {
            "status": "ok",
            "mode": "assisted",
            "peer_id": self.mesh.node_id,
            "operator_summary": summary,
            "routes": routes,
            "pressure": pressure,
            "helper_autonomy": helper_autonomy,
            "connectivity": connectivity,
            "last_run": last_run,
            "recommended_actions": self._recommended_actions(routes, connectivity),
            "generated_at": self._utcnow(),
        }

    def _recommended_actions(self, routes: dict, connectivity: dict) -> list[str]:
        actions = []
        if not (routes.get("routes") or []):
            actions.append("Scan nearby devices and connect trusted peers.")
        if int(routes.get("count") or 0) and int(routes.get("healthy") or 0) < int(routes.get("count") or 0):
            actions.append("Probe routes and repair any stale device URL.")
        if connectivity.get("share_advice"):
            actions.append(str(connectivity.get("share_advice")))
        return actions[:4]

    def _proof_failed_due_transport(self, proof: dict) -> bool:
        mission = dict((proof or {}).get("mission") or {})
        metadata = dict(mission.get("metadata") or {})
        text = " ".join(
            str(value or "")
            for value in [
                proof.get("error"),
                metadata.get("launch_error"),
                metadata.get("error"),
                mission.get("status"),
            ]
        ).lower()
        return "timed out" in text or "timeout" in text or "urlopen" in text or "connection" in text

    def _run_whole_mesh_proof(self, *, include_local: bool, limit: int, request_id: str) -> dict[str, Any]:
        return self.mesh.launch_mesh_test_mission(include_local=include_local, limit=limit, request_id=request_id)

    def _repair_routes(self, peer_ids: list[str], *, timeout: float, request_id: str, actions: list[dict[str, Any]]) -> list[dict]:
        repairs = []
        for peer_id in peer_ids:
            probe = self.probe_routes(peer_id=peer_id, timeout=timeout, limit=4)
            repairs.append(probe)
            best_route = str(probe.get("best_route") or "").strip()
            if best_route:
                try:
                    sync = self.mesh.sync_peer(peer_id, base_url=best_route, limit=20, refresh_manifest=True)
                    self._action(
                        actions,
                        "route_synced",
                        "ok",
                        f"Synced {peer_id} through repaired route.",
                        peer_id=peer_id,
                        details={"base_url": best_route, "sync": sync},
                        request_id=request_id,
                    )
                except Exception as exc:
                    self._action(
                        actions,
                        "route_sync_failed",
                        "warning",
                        f"Route was reachable for {peer_id}, but sync failed: {exc}",
                        peer_id=peer_id,
                        details={"base_url": best_route, "error": str(exc)},
                        request_id=request_id,
                    )
        return repairs

    def activate(
        self,
        *,
        mode: str = "assisted",
        limit: int = 24,
        scan_timeout: float = 0.8,
        timeout: float = 3.0,
        run_proof: bool = True,
        repair: bool = True,
        max_enlist: int = 2,
        actor_agent_id: str = "ocp-autonomy",
        request_id: Optional[str] = None,
    ) -> dict[str, Any]:
        mode_token = str(mode or "assisted").strip().lower() or "assisted"
        request_token = str(request_id or f"autonomic-mesh-{uuid.uuid4().hex[:12]}").strip()
        if request_id:
            existing_run = self.run_by_request_id(request_token)
            if existing_run:
                return self._run_response(existing_run)
        run_id = str(uuid.uuid4())
        actions: list[dict[str, Any]] = []
        result: dict[str, Any] = {}
        self._record_run(
            run_id,
            request_id=request_token,
            mode=mode_token,
            status="running",
            summary="Autonomic Mesh activation is running.",
            actions=actions,
            result=result,
            metadata={"actor_agent_id": actor_agent_id},
        )

        try:
            diagnostics = self.mesh.connectivity_diagnostics(limit=limit)
            result["diagnostics"] = diagnostics
            self._action(
                actions,
                "diagnostics",
                "ok",
                diagnostics.get("share_advice") or "Checked local IPs and shareable URLs.",
                details={"sharing_mode": diagnostics.get("sharing_mode"), "lan_urls": diagnostics.get("lan_urls") or []},
                request_id=request_token,
            )
        except Exception as exc:
            self._action(actions, "diagnostics", "warning", f"Connectivity diagnostics failed: {exc}", details={"error": str(exc)}, request_id=request_token)

        try:
            scan = self.mesh.scan_local_peers(timeout=scan_timeout, limit=limit, trust_tier="trusted")
            result["scan"] = scan
            self._action(
                actions,
                "scan",
                "ok",
                f"Scanned nearby routes: {scan.get('reachable', scan.get('discovered', 0))} candidate(s) surfaced.",
                details={"discovered": scan.get("discovered"), "errors": scan.get("errors")},
                request_id=request_token,
            )
        except Exception as exc:
            self._action(actions, "scan", "warning", f"Nearby scan could not complete: {exc}", details={"error": str(exc)}, request_id=request_token)

        try:
            connected = self.mesh.connect_all_devices(timeout=timeout, scan_timeout=scan_timeout, limit=limit, trust_tier="trusted")
            result["connect"] = connected
            self._action(
                actions,
                "connect",
                "ok",
                connected.get("operator_summary") or f"Connected {connected.get('connected', 0)} peer(s).",
                details={"connected": connected.get("connected"), "already_connected": connected.get("already_connected"), "errors": connected.get("errors")},
                request_id=request_token,
            )
        except Exception as exc:
            self._action(actions, "connect", "warning", f"Connect pass had trouble: {exc}", details={"error": str(exc)}, request_id=request_token)

        peer_rows = list(self.mesh.list_peers(limit=max(24, int(limit or 24) * 2)).get("peers") or [])
        peer_ids = [str(peer.get("peer_id") or "").strip() for peer in peer_rows if str(peer.get("peer_id") or "").strip()]
        route_probes = []
        for peer_id in peer_ids:
            probe = self.probe_routes(peer_id=peer_id, timeout=timeout, limit=4)
            route_probes.append(probe)
            self._action(
                actions,
                "route_probe",
                "ok" if int(probe.get("reachable") or 0) else "warning",
                probe.get("operator_summary") or f"Probed routes for {peer_id}.",
                peer_id=peer_id,
                details={"best_route": probe.get("best_route"), "reachable": probe.get("reachable")},
                request_id=request_token,
            )
        result["routes"] = self.routes_health(limit=max(24, int(limit or 24)))
        result["route_probes"] = route_probes

        helper_result = self._evaluate_and_enlist_helpers(
            actions,
            request_id=request_token,
            max_enlist=max_enlist,
            actor_agent_id=actor_agent_id,
        )
        result["helpers"] = helper_result

        proof: dict[str, Any] = {}
        if run_proof:
            try:
                proof = self._run_whole_mesh_proof(include_local=True, limit=limit, request_id=f"{request_token}-proof")
                result["proof"] = proof
                mission = dict(proof.get("mission") or {})
                mission_status = str(mission.get("status") or proof.get("status") or "unknown")
                self._action(
                    actions,
                    "whole_mesh_proof",
                    "ok" if mission_status in {"completed", "planned", "accepted"} else "warning",
                    f"Whole-mesh proof launched with status {mission_status}.",
                    details={"mission_id": mission.get("id"), "mission_status": mission_status},
                    request_id=request_token,
                )
                if repair and self._proof_failed_due_transport(proof):
                    self._action(actions, "route_repair", "running", "Proof hit a transport timeout; probing routes once before retry.", request_id=request_token)
                    result["repairs"] = self._repair_routes(peer_ids, timeout=timeout, request_id=request_token, actions=actions)
                    proof = self._run_whole_mesh_proof(include_local=True, limit=limit, request_id=f"{request_token}-proof-retry")
                    result["proof_retry"] = proof
                    retry_mission = dict(proof.get("mission") or {})
                    self._action(
                        actions,
                        "whole_mesh_proof_retry",
                        "ok",
                        f"Retried whole-mesh proof with status {retry_mission.get('status') or proof.get('status') or 'unknown'}.",
                        details={"mission_id": retry_mission.get("id"), "mission_status": retry_mission.get("status")},
                        request_id=request_token,
                    )
            except Exception as exc:
                result["proof_error"] = str(exc)
                self._action(actions, "whole_mesh_proof", "warning", f"Whole-mesh proof needs attention: {exc}", details={"error": str(exc)}, request_id=request_token)

        status, summary = self._activation_outcome(result, actions)
        run = self._record_run(
            run_id,
            request_id=request_token,
            mode=mode_token,
            status=status,
            summary=summary,
            actions=actions,
            result=result,
            metadata={"actor_agent_id": actor_agent_id},
        )
        result["run"] = run
        try:
            self.mesh.publish_notification(
                notification_type="mesh.autonomy.summary",
                priority="high" if status in {"needs_attention", "failed"} else "normal",
                title="Autonomic Mesh activation complete",
                body=summary,
                compact_title="Autonomic Mesh",
                compact_body=summary,
                target_peer_id=self.mesh.node_id,
                target_agent_id=actor_agent_id,
                target_device_classes=["full", "light", "micro"],
                metadata={"request_id": request_token, "status": status},
            )
        except Exception:
            self.mesh.logger.debug("autonomy summary notification failed", exc_info=True)
        return {
            "status": status,
            "request_id": request_token,
            "mode": mode_token,
            "summary": summary,
            "operator_summary": summary,
            "actions": actions,
            "routes": result.get("routes") or self.routes_health(limit=limit),
            "proof": result.get("proof_retry") or result.get("proof") or {},
            "helpers": helper_result,
            "approvals": helper_result.get("approvals") or [],
            "run": run,
            "result": result,
            "generated_at": self._utcnow(),
        }

    def _evaluate_and_enlist_helpers(
        self,
        actions: list[dict[str, Any]],
        *,
        request_id: str,
        max_enlist: int,
        actor_agent_id: str,
    ) -> dict[str, Any]:
        try:
            plan = self.mesh.plan_helper_enlistment(
                job={
                    "kind": "python.inline",
                    "requirements": {"capabilities": ["python"], "placement": {"workload_class": "connectivity_test"}},
                    "policy": {"classification": "trusted", "mode": "batch"},
                },
                limit=max(1, int(max_enlist or 2)) * 3,
            )
        except Exception as exc:
            self._action(actions, "helper_plan", "warning", f"Helper planning failed: {exc}", details={"error": str(exc)}, request_id=request_id)
            return {"status": "error", "error": str(exc), "plan": {}, "enlisted": [], "approvals": [], "skipped": []}

        enlisted = []
        approvals = []
        skipped = []
        self._action(
            actions,
            "helper_plan",
            "ok",
            f"Evaluated {plan.get('candidate_count', 0)} helper candidate(s).",
            details={"candidate_count": plan.get("candidate_count")},
            request_id=request_id,
        )
        candidates = list(plan.get("candidates") or [])
        known_candidate_ids = {str(candidate.get("peer_id") or "").strip() for candidate in candidates}
        for peer in list(self.mesh.list_peers(limit=100).get("peers") or []):
            peer_id = str(peer.get("peer_id") or "").strip()
            trust = self._normalize_trust_tier(peer.get("trust_tier") or "trusted")
            if not peer_id or peer_id in known_candidate_ids or trust in {"blocked", "public"}:
                continue
            metadata = dict(peer.get("metadata") or {})
            route_health = dict(metadata.get("route_health") or {})
            route_status = str(route_health.get("status") or "").strip().lower()
            has_proven_route = bool(metadata.get("last_reachable_base_url")) or route_status == "reachable"
            if route_status == "unreachable" or not has_proven_route:
                continue
            device_profile = dict(peer.get("device_profile") or {})
            compute_profile = dict(device_profile.get("compute_profile") or {})
            try:
                enlistment = self.mesh.helpers.peer_enlistment_state(peer)
            except Exception:
                enlistment = dict((peer.get("metadata") or {}).get("enlistment") or {"state": "unenlisted"})
            candidates.append(
                {
                    "peer_id": peer_id,
                    "display_name": peer.get("display_name") or peer_id,
                    "trust_tier": trust,
                    "score": 0,
                    "enlistment": enlistment,
                    "device_class": device_profile.get("device_class") or "full",
                    "execution_tier": device_profile.get("execution_tier") or "standard",
                    "compute_profile": compute_profile,
                    "reasons": ["known_connected_peer"],
                    "recommended_action": "enlist",
                }
            )
            known_candidate_ids.add(peer_id)

        for candidate in candidates:
            peer_id = str(candidate.get("peer_id") or "").strip()
            if not peer_id:
                continue
            peer = self.mesh._row_to_peer(self.mesh._get_peer_row(peer_id)) or {}
            if not self._route_is_usable(peer):
                skipped.append({"peer_id": peer_id, "reason": "route_not_usable"})
                self._action(actions, "helper_skipped", "warning", f"Did not enlist {peer_id} because no fresh working route is proven.", peer_id=peer_id, request_id=request_id)
                continue
            trust = self._normalize_trust_tier(candidate.get("trust_tier") or "trusted")
            device_class = str(candidate.get("device_class") or "full").strip().lower()
            role = "gpu_helper" if dict(candidate.get("compute_profile") or {}).get("gpu_capable") else "helper"
            if str((candidate.get("enlistment") or {}).get("state") or "").strip().lower() == "enlisted":
                skipped.append({"peer_id": peer_id, "reason": "already_enlisted"})
                self._action(actions, "helper_reuse", "ok", f"{candidate.get('display_name') or peer_id} is already enlisted.", peer_id=peer_id, request_id=request_id)
                continue
            if len(enlisted) >= max(0, int(max_enlist or 0)):
                skipped.append({"peer_id": peer_id, "reason": "max_enlist_reached"})
                continue
            if trust == "trusted" and device_class == "full":
                try:
                    state = self.mesh.enlist_helper(peer_id, mode="on_demand", role=role, reason="autonomic_mesh_activation", source="autonomy")
                    enlisted.append({"peer_id": peer_id, "state": state})
                    self._action(actions, "helper_enlisted", "ok", f"Enlisted {candidate.get('display_name') or peer_id} as a safe helper.", peer_id=peer_id, details={"role": role}, request_id=request_id)
                except Exception as exc:
                    skipped.append({"peer_id": peer_id, "reason": str(exc)})
                    self._action(actions, "helper_enlist_failed", "warning", f"Could not enlist {peer_id}: {exc}", peer_id=peer_id, details={"error": str(exc)}, request_id=request_id)
            elif trust == "partner":
                approval = self.mesh.create_approval_request(
                    title=f"Allow {candidate.get('display_name') or peer_id} to help this mesh?",
                    summary="Autonomic Mesh found a partner peer that could help, but partner devices need approval before helper enlistment.",
                    action_type="autonomic.helper.enlist",
                    severity="normal",
                    request_id=f"{request_id}-helper-{peer_id}",
                    requested_by_peer_id=self.mesh.node_id,
                    requested_by_agent_id=actor_agent_id,
                    target_peer_id=self.mesh.node_id,
                    target_agent_id=actor_agent_id,
                    target_device_classes=["full", "light", "micro"],
                    metadata={
                        "autonomic_mesh": True,
                        "candidate_peer_id": peer_id,
                        "autonomous_offload": {
                            "peer_ids": [peer_id],
                            "mode": "on_demand",
                            "role": role,
                            "workload_class": "connectivity_test",
                            "max_auto_enlist": 1,
                        },
                    },
                )
                approvals.append(approval)
                self._action(actions, "helper_approval_requested", "approval_required", f"Asked before using partner peer {candidate.get('display_name') or peer_id}.", peer_id=peer_id, details={"approval": approval}, request_id=request_id)
            else:
                skipped.append({"peer_id": peer_id, "reason": f"trust_tier_{trust}_not_auto_enlisted"})
                self._action(actions, "helper_skipped", "blocked", f"Did not auto-enlist {peer_id} because trust tier is {trust}.", peer_id=peer_id, request_id=request_id)
        return {
            "status": "ok",
            "plan": plan,
            "enlisted": enlisted,
            "approvals": approvals,
            "skipped": skipped,
        }

    def _activation_outcome(self, result: dict, actions: list[dict[str, Any]]) -> tuple[str, str]:
        routes = dict(result.get("routes") or {})
        healthy = int(routes.get("healthy") or 0)
        route_count = int(routes.get("count") or 0)
        proof = dict(result.get("proof_retry") or result.get("proof") or {})
        mission = dict(proof.get("mission") or {})
        proof_status = str(mission.get("status") or proof.get("status") or "").strip().lower()
        warnings = [action for action in actions if action.get("status") in {"warning", "blocked"}]
        approvals = list(((result.get("helpers") or {}).get("approvals") or []))
        if result.get("proof_error") and healthy == 0:
            return "needs_attention", "Autonomic Mesh found peers, but proof execution still needs attention."
        if approvals:
            return "approval_requested", "Mesh routes are prepared; one or more partner helpers need approval before OCP can use them."
        if route_count and healthy == route_count and (not proof or proof_status in {"completed", "planned", "accepted", "ok"}):
            return "completed", f"Mesh is strong: {healthy} route(s) proven, helpers evaluated, and whole-mesh proof launched."
        if healthy:
            return "partial", f"Mesh is partly healthy: {healthy} route(s) work, but {len(warnings)} item(s) need attention."
        return "needs_attention", "Autonomic Mesh could not prove a working remote route yet. Check Wi-Fi, firewall, or the peer URL."
