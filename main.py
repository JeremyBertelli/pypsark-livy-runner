# main.py
import os
import json
import time
from pyspark_livy_runner.config import load_config
from pyspark_livy_runner.session import LivySession
from pyspark_livy_runner.utils.livy_logs import dump_session_debug, ensure_idle


def run_statement(http, session_id, code):
    """POST /sessions/{id}/statements, then poll until finished (with error surfacing)."""
    r = http.request(
        "POST",
        f"/sessions/{session_id}/statements",
        json={"code": code},
        headers={"Content-Type": "application/json"},
    )
    r.raise_for_status()
    sid = r.json()["id"]
    print(f"Statement {sid} submitted")

    while True:
        s = http.request("GET", f"/sessions/{session_id}/statements/{sid}")
        s.raise_for_status()
        js = s.json()
        state = js.get("state", "")
        if state == "available":
            out = js.get("output", {})
            data = out.get("data", {}).get("text/plain", "")
            ename = out.get("ename")
            evalue = out.get("evalue")
            if ename or evalue:
                print("\n=== Statement Error ===")
                print(ename, evalue)
                print(data)
                raise RuntimeError(f"Statement {sid} failed: {ename} {evalue}")
            print("Output:\n", data)
            return js
        if state in ("error", "dead", "cancelling", "cancelled"):
            print("\n=== Statement Terminal State ===")
            print(json.dumps(js, indent=2))
            raise RuntimeError("Statement failed")
        time.sleep(3)


def create_session(sess: LivySession) -> int:
    """Create a new Livy session and wait for IDLE (dump logs on failure)."""
    print("Creating Livy session...")

    js = sess.open(kind="pyspark")  # <-- removed conf argument
    sid = js["id"]
    print(f"Session created (id={sid}) - waiting for IDLE...")
    try:
        sess.wait_idle(timeout_s=900, interval_s=10)
    except RuntimeError:
        dump_session_debug(sess.http, sid, tail=2000)
        raise
    print("Session ready")
    return sid


def attach_or_create(sess: LivySession, reuse: bool) -> int:
    """Attach to existing session or create a new one."""
    if not reuse:
        return create_session(sess)

    existing = sess.load_session_id()
    if not existing:
        sid = create_session(sess)
        sess.save_session_id()
        return sid

    print(f"Attaching to existing session {existing}")
    try:
        sess.attach(existing)
        ensure_idle(sess.http, existing)
        return sess.session_id
    except Exception as e:
        print(f"Attach check failed ({e}); creating a fresh session…")
        sess.clear_session_id()
        sid = create_session(sess)
        sess.save_session_id()
        return sid




def main():
    cfg = load_config("config.yaml")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(base_dir, "examples", "test_hive_job.py")
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Cannot find Spark job file: {script_path}")

    with LivySession(cfg) as sess:
        sid = attach_or_create(sess, cfg.runner.reuse_session)

        with open(script_path, "r", encoding="utf-8") as f:
            code = f.read()

        print("Sending Spark job…")
        try:
            run_statement(sess.http, sid, code)
            print("Job finished")
        except Exception:
            dump_session_debug(sess.http, sid, tail=2000)
            raise


if __name__ == "__main__":
    main()
