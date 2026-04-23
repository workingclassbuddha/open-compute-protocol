"""Ed25519 signing helpers for Sovereign Mesh envelopes."""

from __future__ import annotations

import hashlib
import secrets


SIGNATURE_SCHEME = "ed25519-sha512-v1"

_Q = 2**255 - 19
_L = 2**252 + 27742317777372353535851937790883648493
_D = (-121665 * pow(121666, _Q - 2, _Q)) % _Q
_I = pow(2, (_Q - 1) // 4, _Q)
_IDENTITY = (0, 1, 1, 0)


def _xrecover(y: int) -> int:
    xx = (y * y - 1) * pow(_D * y * y + 1, _Q - 2, _Q)
    x = pow(xx, (_Q + 3) // 8, _Q)
    if (x * x - xx) % _Q != 0:
        x = (x * _I) % _Q
    if x & 1:
        x = _Q - x
    return x


def _extended_from_affine(point: tuple[int, int]) -> tuple[int, int, int, int]:
    x, y = point
    return x % _Q, y % _Q, 1, (x * y) % _Q


_B_Y = 4 * pow(5, _Q - 2, _Q) % _Q
_B = _extended_from_affine((_xrecover(_B_Y), _B_Y))


def _edwards_add(
    point: tuple[int, int, int, int],
    other: tuple[int, int, int, int],
) -> tuple[int, int, int, int]:
    x1, y1, z1, t1 = point
    x2, y2, z2, t2 = other
    a = ((y1 - x1) * (y2 - x2)) % _Q
    b = ((y1 + x1) * (y2 + x2)) % _Q
    c = (2 * _D * t1 * t2) % _Q
    d = (2 * z1 * z2) % _Q
    e = (b - a) % _Q
    f = (d - c) % _Q
    g = (d + c) % _Q
    h = (b + a) % _Q
    return (e * f) % _Q, (g * h) % _Q, (f * g) % _Q, (e * h) % _Q


def _scalarmult(point: tuple[int, int, int, int], scalar: int) -> tuple[int, int, int, int]:
    result = _IDENTITY
    addend = point
    remaining = int(scalar)
    while remaining > 0:
        if remaining & 1:
            result = _edwards_add(result, addend)
        addend = _edwards_add(addend, addend)
        remaining >>= 1
    return result


def _to_affine(point: tuple[int, int, int, int]) -> tuple[int, int]:
    x, y, z, _ = point
    z_inv = pow(z, _Q - 2, _Q)
    return (x * z_inv) % _Q, (y * z_inv) % _Q


def _points_equal(point: tuple[int, int, int, int], other: tuple[int, int, int, int]) -> bool:
    x1, y1, z1, _ = point
    x2, y2, z2, _ = other
    return (x1 * z2 - x2 * z1) % _Q == 0 and (y1 * z2 - y2 * z1) % _Q == 0


def _encode_point(point: tuple[int, int, int, int]) -> bytes:
    x, y = _to_affine(point)
    bits = int(y).to_bytes(32, "little")
    return bits[:31] + bytes([bits[31] | ((x & 1) << 7)])


def _point_is_on_curve(point: tuple[int, int]) -> bool:
    x, y = point
    return (-x * x + y * y - 1 - _D * x * x * y * y) % _Q == 0


def _decode_point(encoded: bytes) -> tuple[int, int, int, int]:
    if len(encoded) != 32:
        raise ValueError("Ed25519 points must be 32 bytes")
    y = int.from_bytes(encoded, "little") & ((1 << 255) - 1)
    x_sign = encoded[31] >> 7
    x = _xrecover(y)
    if (x & 1) != x_sign:
        x = _Q - x
    affine = (x, y)
    if not _point_is_on_curve(affine):
        raise ValueError("Ed25519 point is not on curve")
    point = _extended_from_affine(affine)
    if _points_equal(_scalarmult(point, 8), _IDENTITY):
        raise ValueError("Ed25519 small-order point is not allowed")
    return point


def _private_seed(private_key_hex: str) -> bytes:
    try:
        seed = bytes.fromhex((private_key_hex or "").strip())
    except ValueError as exc:
        raise ValueError("private key must be hex") from exc
    if len(seed) != 32:
        raise ValueError("Ed25519 private key seed must be 32 bytes")
    return seed


def _public_key_bytes(public_key_hex: str) -> bytes:
    try:
        public_key = bytes.fromhex((public_key_hex or "").strip())
    except ValueError as exc:
        raise ValueError("public key must be hex") from exc
    if len(public_key) != 32:
        raise ValueError("Ed25519 public key must be 32 bytes")
    return public_key


def _clamped_scalar(seed: bytes) -> tuple[int, bytes]:
    digest = hashlib.sha512(seed).digest()
    scalar_bytes = bytearray(digest[:32])
    scalar_bytes[0] &= 248
    scalar_bytes[31] &= 63
    scalar_bytes[31] |= 64
    return int.from_bytes(scalar_bytes, "little"), digest[32:]


def generate_keypair() -> tuple[str, str]:
    private_key = secrets.token_bytes(32)
    return private_key.hex(), public_key_from_private(private_key.hex())


def public_key_from_private(private_key_hex: str) -> str:
    seed = _private_seed(private_key_hex)
    scalar, _ = _clamped_scalar(seed)
    return _encode_point(_scalarmult(_B, scalar)).hex()


def sign_message(private_key_hex: str, payload: bytes) -> str:
    seed = _private_seed(private_key_hex)
    scalar, prefix = _clamped_scalar(seed)
    public_key = _encode_point(_scalarmult(_B, scalar))
    message = bytes(payload or b"")
    r = int.from_bytes(hashlib.sha512(prefix + message).digest(), "little") % _L
    encoded_r = _encode_point(_scalarmult(_B, r))
    challenge = int.from_bytes(hashlib.sha512(encoded_r + public_key + message).digest(), "little") % _L
    s = (r + challenge * scalar) % _L
    return (encoded_r + s.to_bytes(32, "little")).hex()


def verify_message(public_key_hex: str, payload: bytes, signature: str) -> bool:
    try:
        public_key = _public_key_bytes(public_key_hex)
        signature_bytes = bytes.fromhex((signature or "").strip())
        if len(signature_bytes) != 64:
            return False
        encoded_r = signature_bytes[:32]
        s = int.from_bytes(signature_bytes[32:], "little")
        if s >= _L:
            return False
        public_point = _decode_point(public_key)
        r_point = _decode_point(encoded_r)
    except Exception:
        return False

    message = bytes(payload or b"")
    challenge = int.from_bytes(hashlib.sha512(encoded_r + public_key + message).digest(), "little") % _L
    left = _scalarmult(_B, s)
    right = _edwards_add(r_point, _scalarmult(public_point, challenge))
    return _points_equal(left, right)
