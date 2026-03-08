import base64
import hashlib
import hmac
import json
import time
from typing import Any


def _b64url_decode(data: str) -> bytes:
    pad = "=" * ((4 - (len(data) % 4)) % 4)
    return base64.urlsafe_b64decode((data + pad).encode("utf-8"))


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def get_token_algorithm(token: str) -> str:
    parts = str(token or "").split(".")
    if len(parts) != 3:
        raise ValueError("invalid token format")
    header_b64 = parts[0]
    try:
        header = json.loads(_b64url_decode(header_b64).decode("utf-8"))
    except Exception as e:  # pragma: no cover - parse errors are covered by ValueError path
        raise ValueError("invalid token payload") from e
    alg = str(header.get("alg") or "").strip()
    if not alg:
        raise ValueError("invalid token algorithm")
    return alg


def decode_and_verify_hs256(token: str, secret: str) -> dict[str, Any]:
    parts = str(token or "").split(".")
    if len(parts) != 3:
        raise ValueError("invalid token format")

    header_b64, payload_b64, signature_b64 = parts
    try:
        header = json.loads(_b64url_decode(header_b64).decode("utf-8"))
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception as e:  # pragma: no cover - parse errors are covered by ValueError path
        raise ValueError("invalid token payload") from e

    if header.get("alg") != "HS256":
        raise ValueError("unsupported token algorithm")

    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected_sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    expected_sig_b64 = _b64url_encode(expected_sig)
    if not hmac.compare_digest(expected_sig_b64, signature_b64):
        raise ValueError("invalid token signature")

    exp = payload.get("exp")
    if exp is not None:
        try:
            exp_ts = int(exp)
        except Exception as e:  # pragma: no cover
            raise ValueError("invalid token exp") from e
        if exp_ts <= int(time.time()):
            raise ValueError("token expired")

    return payload
