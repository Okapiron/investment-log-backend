import hashlib
import re
import secrets
import string


INVITE_CODE_PATTERN = re.compile(r"^[A-Z0-9]{8,12}$")


def normalize_invite_code(value: str) -> str:
    return str(value or "").strip().upper()


def is_valid_invite_code(value: str) -> bool:
    return bool(INVITE_CODE_PATTERN.fullmatch(normalize_invite_code(value)))


def hash_invite_code(value: str) -> str:
    normalized = normalize_invite_code(value)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def generate_invite_code(length: int = 10) -> str:
    if length < 8 or length > 12:
        raise ValueError("length must be between 8 and 12")
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))
