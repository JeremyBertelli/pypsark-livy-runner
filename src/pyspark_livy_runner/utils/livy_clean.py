# -*- coding: utf-8 -*-
"""
Clean (delete) all active Livy sessions using the same config and HTTP setup.

Usage:
    python -m pyspark_livy_runner.utils.livy_clean
"""

import sys
import os
from pyspark_livy_runner.config import load_config
from pyspark_livy_runner.session import LivySession
from pyspark_livy_runner.utils.livy_list import list_sessions


def delete_session(http, sid: int) -> None:
    """Send DELETE /sessions/{id}."""
    r = http.request("DELETE", f"/sessions/{sid}")
    if r.status_code in (200, 201, 202, 204):
        print(f"✅ Deleted session {sid}")
    elif r.status_code == 404:
        print(f"⚠️  Session {sid} not found (maybe already gone)")
    else:
        print(f"❌ Failed to delete session {sid}: HTTP {r.status_code} - {r.text}")


def _main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, "../../.."))
    config_path = os.path.join(project_root, "config.yaml")

    if not os.path.exists(config_path):
        print(f"[livy_clean] Config file not found at {config_path}", file=sys.stderr)
        sys.exit(1)

    cfg = load_config(config_path)
    sess = LivySession(cfg)
    http = sess.http

    try:
        sessions = list_sessions(http)
        if not sessions:
            print("No sessions found.")
            sys.exit(0)

        print(f"Found {len(sessions)} session(s). Deleting them...")
        for s in sessions:
            sid = s.get("id")
            if sid is not None:
                delete_session(http, sid)

        print("✅ Cleanup complete.")
    except Exception as e:
        print(f"[livy_clean] Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    _main()
