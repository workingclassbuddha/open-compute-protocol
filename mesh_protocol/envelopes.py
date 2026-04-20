from __future__ import annotations

import datetime as dt
import json
import uuid
from typing import Optional

from mesh.crypto import SIGNATURE_SCHEME, sign_message, verify_message

from .constants import (
    IMPLEMENTATION_NAME,
    MAX_CLOCK_SKEW_SECONDS,
    PROTOCOL_RELEASE,
    PROTOCOL_SHORT_NAME,
    PROTOCOL_VERSION,
)
from .errors import MeshSignatureError


def _json_dump(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _utcnow_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def _utcnow() -> str:
    return _utcnow_dt().isoformat().replace("+00:00", "Z")


class MeshProtocolService:
    """Signed-envelope protocol primitives delegated by SovereignMesh."""

    def __init__(self, mesh):
        self.mesh = mesh

    def canonical_signing_bytes(self, route: str, body: dict, request_meta: dict) -> bytes:
        payload = {
            "route": route,
            "body": body,
            "node_id": request_meta["node_id"],
            "timestamp": request_meta["timestamp"],
            "nonce": request_meta["nonce"],
            "request_id": request_meta["request_id"],
            "protocol_version": request_meta["protocol_version"],
        }
        return _json_dump(payload).encode("utf-8")

    def build_signed_envelope(
        self,
        route: str,
        body: dict,
        *,
        request_id: Optional[str] = None,
        timestamp: Optional[str] = None,
        nonce: Optional[str] = None,
    ) -> dict:
        request_meta = {
            "node_id": self.mesh.node_id,
            "timestamp": timestamp or _utcnow(),
            "nonce": nonce or uuid.uuid4().hex,
            "request_id": request_id or uuid.uuid4().hex,
            "protocol_family": PROTOCOL_SHORT_NAME,
            "protocol_release": PROTOCOL_RELEASE,
            "implementation": IMPLEMENTATION_NAME,
            "protocol_version": PROTOCOL_VERSION,
        }
        signature = sign_message(
            self.mesh.private_key,
            self.canonical_signing_bytes(route, body, request_meta),
        )
        request_meta["signature_scheme"] = SIGNATURE_SCHEME
        request_meta["signature"] = signature
        return {"request": request_meta, "body": dict(body or {})}

    def parse_timestamp(self, value: str) -> dt.datetime:
        sample = (value or "").strip()
        if not sample:
            raise MeshSignatureError("timestamp is required")
        if sample.endswith("Z"):
            sample = sample[:-1] + "+00:00"
        return dt.datetime.fromisoformat(sample).astimezone(dt.timezone.utc)

    def verify_envelope(
        self,
        envelope: dict,
        *,
        route: str,
        peer_card: Optional[dict] = None,
    ) -> tuple[str, dict, dict, Optional[dict]]:
        request_meta = dict(envelope.get("request") or {})
        body = dict(envelope.get("body") or {})
        if not request_meta:
            raise MeshSignatureError("request metadata is required")

        peer_id = (request_meta.get("node_id") or "").strip()
        if not peer_id:
            raise MeshSignatureError("node_id is required")

        protocol_version = (request_meta.get("protocol_version") or "").strip()
        if protocol_version != PROTOCOL_VERSION:
            raise MeshSignatureError("unsupported mesh protocol version")

        signature_scheme = (request_meta.get("signature_scheme") or "").strip()
        if signature_scheme != SIGNATURE_SCHEME:
            raise MeshSignatureError("unsupported signature scheme")

        timestamp = self.parse_timestamp(request_meta.get("timestamp"))
        if abs((_utcnow_dt() - timestamp).total_seconds()) > MAX_CLOCK_SKEW_SECONDS:
            raise MeshSignatureError("stale or future timestamp")

        signature = (request_meta.get("signature") or "").strip()
        if not signature:
            raise MeshSignatureError("signature is required")

        public_key = ""
        peer_row = None
        if peer_card is not None:
            public_key = (peer_card.get("public_key") or "").strip()
        else:
            peer_row = self.mesh._get_peer_row(peer_id)
            if peer_row is None:
                raise MeshSignatureError(f"unknown peer {peer_id}")
            public_key = (peer_row["public_key"] or "").strip()

        if not public_key:
            raise MeshSignatureError("public key unavailable for verification")

        signing_bytes = self.canonical_signing_bytes(route, body, request_meta)
        if not verify_message(public_key, signing_bytes, signature):
            raise MeshSignatureError("signature verification failed")

        self.mesh._remember_nonce(
            peer_id,
            (request_meta.get("nonce") or "").strip(),
            route,
            (request_meta.get("request_id") or "").strip(),
        )
        peer = self.mesh._row_to_peer(peer_row) if peer_row is not None else None
        return peer_id, request_meta, body, peer
