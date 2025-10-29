# src/pyspark_livy_runner/utils/livy_logs.py
# -*- coding: utf-8 -*-
"""
Fetch and print Livy session information and logs.

Usage:
    python -m pyspark_livy_runner.utils.livy_logs            # auto-resolve session id by priority
    python -m pyspark_livy_runner.utils.livy_logs <session_id>  # explicit session id (highest priority)

Priority (from weakest to strongest):
  1) If nothing provided: pick the latest Livy session owned by the current user
  2) If a .livy_session or .livy_session_id file exists: use its id
  3) If FORCE_SESSION_ID (below) is set in code: use it
  4) If <session_id> is passed as CLI arg: use it

This uses the same config and HTTP client as the main runner.
"""

import json
import os
import sys
from typing import Optional, Dict, Any, List

from pyspark_livy_runner.config import load_config
from pyspark_livy_runner.session import LivySession

# --- (3) Code override: set a session id here if you want to hard-force it
FORCE_SESSION_ID: Optional[int] = None


# ---------------- Core helpers ----------------

def dump_session_debug(http, session_id: int, tail: int = 2000) -> None:
    """Print session info and logs for the given Livy session."""
    # --- Session info ---
    try:
        info = http.request("GET", f"/sessions/{session_id}")
        info.raise_for_status()
        print("\n=== /sessions/{id} ===")
        print(json.dumps(info.json(), indent=2))
    except Exception as e:
        print(f"Failed to fetch /sessions/{session_id}: {e}")

    # --- Logs ---
    try:
        log = http.request("GET", f"/sessions/{session_id}/log?from=0&size={tail}")
        log.raise_for_status()
        payload = log.json()
        print(f"\n=== /sessions/{id}/log (last {tail} lines) ===")
        for line in payload.get("log", []):
            print(line)
    except Exception as e:
        print(f"Failed to fetch /sessions/{session_id}/log: {e}")


def ensure_idle(http, session_id: int) -> None:
    """Raise if the session is not IDLE (and dump logs)."""
    r = http.request("GET", f"/sessions/{session_id}")
    r.raise_for_status()
    st = r.json().get("state")
    if st != "idle":
        print(f"Session {session_id} state is '{st}', expected 'idle'. Dumping logs…")
        dump_session_debug(http, session_id)
        raise RuntimeError(f"Session {session_id} not idle: {st}")


# ---------------- Resolution logic ----------------

def _project_root_from_here() -> str:
    """Go from utils/ to project root (…/src/pyspark_livy_runner/utils -> project root)."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # utils -> pyspark_livy_runner -> src -> project_root
    return os.path.abspath(os.path.join(base_dir, "../../.."))


def _find_config_path() -> str:
    """
    Try to locate config file at project root: prefer config.yaml, then config.yml.
    If neither exists, default to 'config.yaml' (load_config will raise if missing).
    """
    root = _project_root_from_here()
    yaml = os.path.join(root, "config.yaml")
    yml = os.path.join(root, "config.yml")
    if os.path.exists(yaml):
        return yaml
    if os.path.exists(yml):
        return yml
    return "config.yaml"  # fallback (may raise in load_config)


def _read_saved_session_id(project_root: str) -> Optional[int]:
    """
    Read session id from .livy_session or .livy_session_id if present at project root.
    """
    candidates = [".livy_session", ".livy_session_id"]
    for name in candidates:
        path = os.path.join(project_root, name)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    txt = f.read().strip()
                    if txt:
                        return int(txt)
            except Exception:
                # ignore and continue with next candidate
                pass
    return None


def _list_sessions(http) -> List[Dict[str, Any]]:
    """GET /sessions and return list of session dicts."""
    r = http.request("GET", "/sessions?from=0&size=1000")
    r.raise_for_status()
    payload = r.json() or {}
    return payload.get("sessions", [])


def _username_from_cfg(cfg: Any) -> Optional[str]:
    """
    Extract the username from cfg (supports both dot and dict styles).
    """
    if hasattr(cfg, "username"):
        return getattr(cfg, "username")
    if isinstance(cfg, dict):
        return cfg.get("username")
    return None


def _latest_user_session_id(http, username: Optional[str]) -> Optional[int]:
    """
    Pick the latest (highest id) session owned by `username`.
    If username is None or no owned sessions exist, return None.
    """
    try:
        sessions = _list_sessions(http)
    except Exception:
        return None

    if not sessions or not username:
        return None

    # Livy session objects may have owner/proxyUser fields; check both.
    owned = []
    for s in sessions:
        owner = s.get("owner") or s.get("proxyUser") or s.get("proxyUserName")
        if owner == username:
            owned.append(s)

    if not owned:
        return None

    # Choose the one with the highest numeric id (usually newest)
    try:
        return max(owned, key=lambda x: int(x.get("id", -1))).get("id")
    except Exception:
        # fallback: take the last item
        return owned[-1].get("id")


def _resolve_session_id(http, cfg: Any, cli_arg: Optional[int]) -> int:
    """
    Resolve session id with the required priority (weak -> strong):
      1) Latest session of the user
      2) .livy_session or .livy_session_id file
      3) FORCE_SESSION_ID (code variable)
      4) CLI arg
    BUT since we want strongest to win, we check in reverse order and return on first hit.
    """
    # (4) CLI arg
    if cli_arg is not None:
        return int(cli_arg)

    # (3) Code variable
    if FORCE_SESSION_ID is not None:
        return int(FORCE_SESSION_ID)

    # (2) Saved file
    saved = _read_saved_session_id(_project_root_from_here())
    if saved is not None:
        return int(saved)

    # (1) Latest for current user
    user = _username_from_cfg(cfg)
    latest = _latest_user_session_id(http, user)
    if latest is not None:
        return int(latest)

    raise RuntimeError("Unable to resolve a Livy session id (no CLI arg, no FORCE_SESSION_ID, no saved file, no user-owned sessions).")


# ---------------- Main ----------------

def _main():
    # CLI arg: optional positional session_id
    cli_arg: Optional[int] = None
    if len(sys.argv) == 2:
        try:
            cli_arg = int(sys.argv[1])
        except ValueError:
            print("Usage: python -m pyspark_livy_runner.utils.livy_logs <session_id>", file=sys.stderr)
            sys.exit(1)
    elif len(sys.argv) > 2:
        print("Usage: python -m pyspark_livy_runner.utils.livy_logs <session_id>", file=sys.stderr)
        sys.exit(1)

    # Load config from project root (supports config.yaml or config.yml)
    cfg_path = _find_config_path()
    cfg = load_config(cfg_path)

    # Reuse the exact same HTTP client as the runner
    sess = LivySession(cfg)  # no session created; we just need .http
    http = sess.http

    # Resolve session id using the specified priority
    session_id = _resolve_session_id(http, cfg, cli_arg)

    # Dump info + logs
    dump_session_debug(http, session_id)


if __name__ == "__main__":
    _main()
