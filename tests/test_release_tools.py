import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _run_json_script(args: list[str]) -> tuple[int, dict]:
    completed = subprocess.run(
        [sys.executable, *args],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    output = str(completed.stdout or "").strip()
    payload = json.loads(output) if output else {}
    return int(completed.returncode), payload


def test_smoke_release_json_reports_failure_when_backend_unreachable():
    rc, payload = _run_json_script(
        [
            "tools/smoke_release.py",
            "--base",
            "http://127.0.0.1:9",
            "--json",
        ]
    )
    assert rc == 1
    assert payload.get("status") == "failed"
    checks = payload.get("checks") or []
    assert isinstance(checks, list) and len(checks) > 0
    health = next((c for c in checks if c.get("name") == "health"), None)
    assert health is not None
    assert health.get("ok") is False


def test_preflight_release_json_reports_failure_when_backend_unreachable():
    rc, payload = _run_json_script(
        [
            "tools/preflight_release.py",
            "--base",
            "http://127.0.0.1:9",
            "--json",
            "--no-expect-auth-required",
        ]
    )
    assert rc == 1
    assert payload.get("status") == "failed"
    checks = payload.get("checks") or {}
    assert isinstance(checks, dict)
    assert "config" in checks
    assert "smoke" in checks
