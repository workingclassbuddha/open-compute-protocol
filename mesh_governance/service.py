from __future__ import annotations

import json
import uuid
from typing import Any, Optional

from mesh_protocol import MeshPolicyError


class MeshGovernanceService:
    """Notifications and approvals for SovereignMesh."""

    def __init__(
        self,
        mesh,
        *,
        compact_text,
        loads_json,
        normalize_approval_status,
        normalize_notification_status,
        unique_tokens,
        utcnow,
        apply_approval_automation,
    ):
        self.mesh = mesh
        self._compact_text = compact_text
        self._loads_json = loads_json
        self._normalize_approval_status = normalize_approval_status
        self._normalize_notification_status = normalize_notification_status
        self._unique_tokens = unique_tokens
        self._utcnow = utcnow
        self._apply_approval_automation = apply_approval_automation

    def publish_notification(
        self,
        *,
        notification_type: str = "info",
        priority: str = "normal",
        title: str,
        body: str = "",
        compact_title: str = "",
        compact_body: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
        target_device_classes: Optional[list[str]] = None,
        related_job_id: str = "",
        related_approval_id: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        title_token = str(title or "").strip()
        if not title_token:
            raise MeshPolicyError("notification title is required")
        now = self._utcnow()
        notification_id = str(uuid.uuid4())
        device_classes = self._unique_tokens(target_device_classes)
        priority_token = str(priority or "normal").strip().lower() or "normal"
        if priority_token not in {"low", "normal", "high", "critical"}:
            priority_token = "normal"
        with self.mesh._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_notifications
                (id, notification_type, priority, title, body, compact_title, compact_body, status,
                 target_peer_id, target_agent_id, target_device_classes, related_job_id, related_approval_id,
                 metadata, created_at, updated_at, acked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'unread', ?, ?, ?, ?, ?, ?, ?, ?, '')
                """,
                (
                    notification_id,
                    str(notification_type or "info").strip().lower() or "info",
                    priority_token,
                    title_token,
                    str(body or ""),
                    str(compact_title or "").strip() or self._compact_text(title_token, limit=48),
                    str(compact_body or "").strip() or self._compact_text(body, limit=96),
                    str(target_peer_id or "").strip(),
                    str(target_agent_id or "").strip(),
                    json.dumps(device_classes),
                    str(related_job_id or "").strip(),
                    str(related_approval_id or "").strip(),
                    json.dumps(dict(metadata or {})),
                    now,
                    now,
                ),
            )
            row = conn.execute("SELECT * FROM mesh_notifications WHERE id=?", (notification_id,)).fetchone()
            conn.commit()
        notification = self.row_to_notification(row)
        self.mesh._record_event(
            "mesh.notification.published",
            peer_id=notification.get("target_peer_id") or self.mesh.node_id,
            payload={
                "notification_id": notification["id"],
                "notification_type": notification["notification_type"],
                "priority": notification["priority"],
                "target_peer_id": notification.get("target_peer_id") or "",
            },
        )
        return notification

    def list_notifications(
        self,
        *,
        limit: int = 25,
        status: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
    ) -> dict:
        status_token = self._normalize_notification_status(status) if str(status or "").strip() else ""
        query = [
            "SELECT * FROM mesh_notifications WHERE 1=1",
        ]
        args: list[Any] = []
        if status_token:
            query.append("AND status=?")
            args.append(status_token)
        if str(target_peer_id or "").strip():
            query.append("AND target_peer_id=?")
            args.append(str(target_peer_id or "").strip())
        if str(target_agent_id or "").strip():
            query.append("AND target_agent_id=?")
            args.append(str(target_agent_id or "").strip())
        query.append("ORDER BY created_at DESC LIMIT ?")
        args.append(max(1, int(limit)))
        with self.mesh._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(args)).fetchall()
        notifications = [self.row_to_notification(row) for row in rows]
        return {"peer_id": self.mesh.node_id, "count": len(notifications), "notifications": notifications}

    def ack_notification(
        self,
        notification_id: str,
        *,
        status: str = "acked",
        actor_peer_id: str = "",
        actor_agent_id: str = "",
        reason: str = "",
    ) -> dict:
        row = None
        with self.mesh._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_notifications WHERE id=?", ((notification_id or "").strip(),)).fetchone()
        if row is None:
            raise MeshPolicyError("notification not found")
        status_token = self._normalize_notification_status(status)
        now = self._utcnow()
        metadata = self._loads_json(row["metadata"], {})
        metadata["last_actor_peer_id"] = str(actor_peer_id or "")
        metadata["last_actor_agent_id"] = str(actor_agent_id or "")
        metadata["last_ack_reason"] = str(reason or "")
        with self.mesh._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_notifications
                SET status=?, metadata=?, updated_at=?, acked_at=?
                WHERE id=?
                """,
                (status_token, json.dumps(metadata), now, now if status_token != "unread" else "", (notification_id or "").strip()),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_notifications WHERE id=?", ((notification_id or "").strip(),)).fetchone()
        notification = self.row_to_notification(fresh)
        self.mesh._record_event(
            "mesh.notification.acked",
            peer_id=notification.get("target_peer_id") or self.mesh.node_id,
            payload={"notification_id": notification["id"], "status": notification["status"]},
        )
        return notification

    def create_approval_request(
        self,
        *,
        title: str,
        summary: str = "",
        action_type: str = "operator_action",
        severity: str = "normal",
        request_id: str = "",
        requested_by_peer_id: str = "",
        requested_by_agent_id: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
        target_device_classes: Optional[list[str]] = None,
        related_job_id: str = "",
        expires_at: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        title_token = str(title or "").strip()
        if not title_token:
            raise MeshPolicyError("approval title is required")
        request_token = str(request_id or "").strip()
        if request_token:
            with self.mesh._conn() as conn:
                existing = conn.execute("SELECT * FROM mesh_approvals WHERE request_id=?", (request_token,)).fetchone()
            if existing is not None:
                approval = self.row_to_approval(existing)
                approval["deduped"] = True
                return {"status": approval["status"], "approval": approval}
        severity_token = str(severity or "normal").strip().lower() or "normal"
        if severity_token not in {"low", "normal", "high", "critical"}:
            severity_token = "normal"
        notification = self.publish_notification(
            notification_type="approval.request",
            priority="critical" if severity_token == "critical" else ("high" if severity_token in {"high", "critical"} else "normal"),
            title=title_token,
            body=summary,
            target_peer_id=target_peer_id,
            target_agent_id=target_agent_id,
            target_device_classes=target_device_classes,
            related_job_id=related_job_id,
            metadata={"action_type": action_type, "approval_request": True},
        )
        approval_id = str(uuid.uuid4())
        now = self._utcnow()
        device_classes = self._unique_tokens(target_device_classes)
        with self.mesh._conn() as conn:
            conn.execute(
                """
                INSERT INTO mesh_approvals
                (id, request_id, action_type, severity, title, summary, compact_summary, status,
                 requested_by_peer_id, requested_by_agent_id, target_peer_id, target_agent_id, target_device_classes,
                 related_job_id, notification_id, resolution, metadata, created_at, updated_at, expires_at, resolved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?, ?, '')
                """,
                (
                    approval_id,
                    request_token or None,
                    str(action_type or "operator_action").strip().lower() or "operator_action",
                    severity_token,
                    title_token,
                    str(summary or ""),
                    self._compact_text(summary or title_token, limit=96),
                    str(requested_by_peer_id or "").strip(),
                    str(requested_by_agent_id or "").strip(),
                    str(target_peer_id or "").strip(),
                    str(target_agent_id or "").strip(),
                    json.dumps(device_classes),
                    str(related_job_id or "").strip(),
                    notification["id"],
                    json.dumps(dict(metadata or {})),
                    now,
                    now,
                    str(expires_at or "").strip(),
                ),
            )
            row = conn.execute("SELECT * FROM mesh_approvals WHERE id=?", (approval_id,)).fetchone()
            conn.commit()
        approval = self.row_to_approval(row)
        self.mesh._record_event(
            "mesh.approval.requested",
            peer_id=approval.get("target_peer_id") or self.mesh.node_id,
            request_id=request_token,
            payload={
                "approval_id": approval["id"],
                "action_type": approval["action_type"],
                "target_peer_id": approval.get("target_peer_id") or "",
                "severity": approval["severity"],
            },
        )
        return {"status": "pending", "approval": approval, "notification": notification}

    def expire_pending_approvals(self) -> int:
        now = self._utcnow()
        expired = 0
        with self.mesh._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mesh_approvals
                WHERE status='pending' AND expires_at != '' AND expires_at < ?
                """,
                (now,),
            ).fetchall()
            for row in rows:
                resolution = {"decision": "expired", "resolved_at": now}
                conn.execute(
                    """
                    UPDATE mesh_approvals
                    SET status='expired', resolution=?, updated_at=?, resolved_at=?
                    WHERE id=?
                    """,
                    (json.dumps(resolution), now, now, row["id"]),
                )
                expired += 1
            conn.commit()
        return expired

    def list_approvals(
        self,
        *,
        limit: int = 25,
        status: str = "",
        target_peer_id: str = "",
        target_agent_id: str = "",
    ) -> dict:
        self.expire_pending_approvals()
        status_token = self._normalize_approval_status(status) if str(status or "").strip() else ""
        query = [
            "SELECT * FROM mesh_approvals WHERE 1=1",
        ]
        args: list[Any] = []
        if status_token:
            query.append("AND status=?")
            args.append(status_token)
        if str(target_peer_id or "").strip():
            query.append("AND target_peer_id=?")
            args.append(str(target_peer_id or "").strip())
        if str(target_agent_id or "").strip():
            query.append("AND target_agent_id=?")
            args.append(str(target_agent_id or "").strip())
        query.append("ORDER BY created_at DESC LIMIT ?")
        args.append(max(1, int(limit)))
        with self.mesh._conn() as conn:
            rows = conn.execute("\n".join(query), tuple(args)).fetchall()
        approvals = [self.row_to_approval(row) for row in rows]
        return {"peer_id": self.mesh.node_id, "count": len(approvals), "approvals": approvals}

    def resolve_approval(
        self,
        approval_id: str,
        *,
        decision: str,
        operator_peer_id: str = "",
        operator_agent_id: str = "",
        reason: str = "",
        metadata: Optional[dict] = None,
    ) -> dict:
        self.expire_pending_approvals()
        with self.mesh._conn() as conn:
            row = conn.execute("SELECT * FROM mesh_approvals WHERE id=?", ((approval_id or "").strip(),)).fetchone()
        if row is None:
            raise MeshPolicyError("approval not found")
        approval = self.row_to_approval(row)
        if approval["status"] != "pending":
            return {"status": approval["status"], "approval": approval}
        decision_token = str(decision or "").strip().lower()
        if decision_token not in {"approved", "rejected", "deferred"}:
            raise MeshPolicyError("unsupported approval decision")
        now = self._utcnow()
        resolution = {
            "decision": decision_token,
            "reason": str(reason or ""),
            "operator_peer_id": str(operator_peer_id or ""),
            "operator_agent_id": str(operator_agent_id or ""),
            "metadata": dict(metadata or {}),
            "resolved_at": now,
        }
        merged_metadata = dict(approval.get("metadata") or {})
        merged_metadata["last_resolution"] = resolution
        with self.mesh._conn() as conn:
            conn.execute(
                """
                UPDATE mesh_approvals
                SET status=?, resolution=?, metadata=?, updated_at=?, resolved_at=?
                WHERE id=?
                """,
                (decision_token, json.dumps(resolution), json.dumps(merged_metadata), now, now, (approval_id or "").strip()),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM mesh_approvals WHERE id=?", ((approval_id or "").strip(),)).fetchone()
        updated = self.row_to_approval(fresh)
        resolution_notification = self.publish_notification(
            notification_type="approval.resolved",
            priority="normal",
            title=f"Approval {decision_token}",
            body=updated["title"],
            target_peer_id=updated.get("requested_by_peer_id") or "",
            target_agent_id=updated.get("requested_by_agent_id") or "",
            target_device_classes=updated.get("target_device_classes") or [],
            related_job_id=updated.get("related_job_id") or "",
            related_approval_id=updated["id"],
            metadata={"decision": decision_token, "reason": str(reason or "")},
        )
        self.mesh._record_event(
            "mesh.approval.resolved",
            peer_id=updated.get("target_peer_id") or self.mesh.node_id,
            payload={"approval_id": updated["id"], "decision": decision_token},
        )
        response = {"status": decision_token, "approval": updated, "notification": resolution_notification}
        response["automation"] = self._apply_approval_automation(
            updated,
            decision=decision_token,
            operator_peer_id=operator_peer_id,
            operator_agent_id=operator_agent_id,
            reason=reason,
        )
        return response

    def row_to_notification(self, row) -> Optional[dict]:
        if row is None:
            return None
        target_device_classes = self._loads_json(row["target_device_classes"], [])
        compact_title = row["compact_title"] or self._compact_text(row["title"] or "", limit=48)
        compact_body = row["compact_body"] or self._compact_text(row["body"] or "", limit=96)
        return {
            "id": row["id"],
            "notification_type": row["notification_type"] or "info",
            "priority": row["priority"] or "normal",
            "title": row["title"] or "",
            "body": row["body"] or "",
            "compact_title": compact_title,
            "compact_body": compact_body,
            "status": self._normalize_notification_status(row["status"]),
            "target_peer_id": row["target_peer_id"] or "",
            "target_agent_id": row["target_agent_id"] or "",
            "target_device_classes": target_device_classes,
            "related_job_id": row["related_job_id"] or "",
            "related_approval_id": row["related_approval_id"] or "",
            "presentation": {
                "compact": bool(set(target_device_classes) & {"light", "micro"}),
                "compact_title": compact_title,
                "compact_body": compact_body,
            },
            "metadata": self._loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "acked_at": row["acked_at"] or "",
        }

    def row_to_approval(self, row) -> Optional[dict]:
        if row is None:
            return None
        return {
            "id": row["id"],
            "request_id": row["request_id"] or "",
            "action_type": row["action_type"] or "operator_action",
            "severity": row["severity"] or "normal",
            "title": row["title"] or "",
            "summary": row["summary"] or "",
            "compact_summary": row["compact_summary"] or self._compact_text(row["summary"] or row["title"] or "", limit=96),
            "status": self._normalize_approval_status(row["status"]),
            "requested_by_peer_id": row["requested_by_peer_id"] or "",
            "requested_by_agent_id": row["requested_by_agent_id"] or "",
            "target_peer_id": row["target_peer_id"] or "",
            "target_agent_id": row["target_agent_id"] or "",
            "target_device_classes": self._loads_json(row["target_device_classes"], []),
            "related_job_id": row["related_job_id"] or "",
            "notification_id": row["notification_id"] or "",
            "resolution": self._loads_json(row["resolution"], {}),
            "metadata": self._loads_json(row["metadata"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "expires_at": row["expires_at"] or "",
            "resolved_at": row["resolved_at"] or "",
        }
