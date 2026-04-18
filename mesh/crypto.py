"""
mesh.crypto — lightweight asymmetric signing helpers for Sovereign Mesh.

The Python standard library does not ship Ed25519 primitives, so this module
uses a small Schnorr-style signature over a safe-prime multiplicative group.
It is sufficient for local-first federation tests and protocol integrity
without adding external dependencies.
"""

from __future__ import annotations

import hashlib
import secrets


# 1024-bit Oakley group 2 safe prime from RFC 2409 / RFC 3526 family.
_P = int(
    (
        "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E08"
        "8A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD"
        "3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E"
        "7EC6F44C42E9A637ED6B0BFF5CB6F406B7EDEE386BFB5A899F"
        "A5AE9F24117C4B1FE649286651ECE65381FFFFFFFFFFFFFFFF"
    ),
    16,
)
_Q = (_P - 1) // 2
_G = 4

SIGNATURE_SCHEME = "schnorr-sha256-modp1024-v1"


def _hash_to_int(*parts: bytes) -> int:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(part)
    return int.from_bytes(digest.digest(), "big") % _Q


def _int_to_bytes(value: int) -> bytes:
    width = max(1, (_P.bit_length() + 7) // 8)
    return int(value).to_bytes(width, "big")


def generate_keypair() -> tuple[str, str]:
    private_int = secrets.randbelow(_Q - 2) + 1
    public_int = pow(_G, private_int, _P)
    return format(private_int, "x"), format(public_int, "x")


def public_key_from_private(private_key_hex: str) -> str:
    private_int = int((private_key_hex or "0").strip(), 16)
    if private_int <= 0:
        raise ValueError("private key is required")
    return format(pow(_G, private_int, _P), "x")


def sign_message(private_key_hex: str, payload: bytes) -> str:
    private_int = int((private_key_hex or "0").strip(), 16)
    if private_int <= 0:
        raise ValueError("private key is required")
    public_int = pow(_G, private_int, _P)
    nonce = secrets.randbelow(_Q - 2) + 1
    commitment = pow(_G, nonce, _P)
    challenge = _hash_to_int(
        _int_to_bytes(public_int),
        _int_to_bytes(commitment),
        payload,
    )
    response = (nonce + private_int * challenge) % _Q
    return f"{format(commitment, 'x')}.{format(response, 'x')}"


def verify_message(public_key_hex: str, payload: bytes, signature: str) -> bool:
    try:
        public_int = int((public_key_hex or "0").strip(), 16)
        commitment_hex, response_hex = (signature or "").split(".", 1)
        commitment = int(commitment_hex, 16)
        response = int(response_hex, 16)
    except Exception:
        return False

    if public_int <= 1 or public_int >= _P:
        return False
    if commitment <= 0 or commitment >= _P:
        return False
    if response <= 0 or response >= _Q:
        return False

    challenge = _hash_to_int(
        _int_to_bytes(public_int),
        _int_to_bytes(commitment),
        payload,
    )
    left = pow(_G, response, _P)
    right = (commitment * pow(public_int, challenge, _P)) % _P
    return left == right
