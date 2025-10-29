# src/pyspark_livy_runner/utils/livy_list.py
# -*- coding: utf-8 -*-
"""
List Livy sessions using the project's existing HTTP (same config, same auth).

Usage:
    python -m pyspark_livy_runner.utils.livy_list
"""

from typing import List, Dict, Any
import sys
import os

from pyspark_livy_runner.config import load_config
from pyspark_livy_runner.session import LivySession


def list_sessions(http) -> List[Dict[str, Any]]:
    r = http.request("GET", "/sessions?from=0&size=1000")
    r.raise_for_status()
    payload = r.json() or {}
    return payload.get("sessions", [])


def print_sessions(sessions: List[Dict[str, Any]]) -> None:
    if not sessions:
        print("No sessions.")
        return

    headers = ["id", "state", "kind", "appId", "user"]
    rows = []
    for s in sessions:
        rows.append({
            "id": str(s.get("id")),
            "state": s.get("state", ""),
            "kind": s.get("kind", ""),
            "appId": s.get("appId") or (s.get("appInfo") or {}).get("applicationId", ""),
            "user": s.get("owner") or s.get("proxyUser") or "",
        })

    # compute column widths
    widths = {h: max(len(h), *(len(r[h]) for r in rows)) for h in headers}

    print("  ".join(h.upper().ljust(widths[h]) for h in headers))
    print("  ".join("-" * widths[h] for h in headers))
    for r in rows:
        print("  ".join(r[h].ljust(widths[h]) for h in headers))


def _main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, "../../.."))
    config_path = os.path.join(project_root, "config.yaml")

    if not os.path.exists(config_path):
        print(f"[livy_list] Config file not found at {config_path}", file=sys.stderr)
        sys.exit(1)

    cfg = load_config(config_path)
    sess = LivySession(cfg)
    http = sess.http

    try:
        sessions = list_sessions(http)
        print_sessions(sessions)
    except Exception as e:
        print(f"[livy_list] Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    _main()