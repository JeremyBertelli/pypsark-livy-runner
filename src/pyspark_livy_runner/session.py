import time
import signal
import os
from .config import AppConfig
from .http import HttpClient


class LivySession:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.http = HttpClient(cfg)
        self.session_id = None
        self._closed = False

    # -----------------------------
    # Persistence helpers
    # -----------------------------
    def save_session_id(self):
        if self.session_id is not None:
            with open(self.cfg.runner.session_file, "w", encoding="utf-8") as f:
                f.write(str(self.session_id))

    def load_session_id(self):
        p = self.cfg.runner.session_file
        if not os.path.exists(p):
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                return int(f.read().strip())
        except Exception:
            return None

    def clear_session_id(self):
        p = self.cfg.runner.session_file
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    # -----------------------------
    # Signal handling
    # -----------------------------
    def _install_signal_handlers(self):
        def _handler(signum, frame):
            if not self.cfg.runner.reuse_session:
                try:
                    self.close()
                except Exception:
                    pass
            raise SystemExit(128 + signum)

        for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
            if sig is not None:
                try:
                    signal.signal(sig, _handler)
                except Exception:
                    pass

    # -----------------------------
    # Attach to an existing session
    # -----------------------------
    def attach(self, session_id: int):
        self.session_id = session_id
        r = self.http.request("GET", f"/sessions/{self.session_id}")
        if r.status_code == 404:
            self.session_id = None
            raise RuntimeError(f"Session {session_id} not found on Livy")
        r.raise_for_status()

        st = r.json().get("state", "").lower()
        if st in ("dead", "error", "killed", "shutting_down"):
            self.session_id = None
            raise RuntimeError(f"Session {session_id} is in terminal state: {st}")

        self._install_signal_handlers()
        return r.json()

    # -----------------------------
    # Open a new Livy session
    # -----------------------------
    def open(self, kind: str = "pyspark"):
        """
        Create a new Livy session using configuration from YAML.
        Fully configurable via cfg.spark.* and cfg.spark.extra_conf.
        """

        # Base Spark conf
        spark_conf = {
            "spark.master": self.cfg.spark.master,
            "spark.yarn.queue": self.cfg.spark.queue,
            "spark.dynamicAllocation.enabled": str(self.cfg.spark.dynamic_allocation).lower(),
            **(self.cfg.spark.extra_conf or {}),
        }

        body = {
            "kind": kind,
            "driverMemory": self.cfg.spark.driver_memory,
            "executorMemory": self.cfg.spark.executor_memory,
            "executorCores": self.cfg.spark.executor_cores,
            "numExecutors": self.cfg.spark.executor_instances,
            "conf": spark_conf,
        }

        print("[DEBUG] Livy session creation payload:")
        for k, v in body["conf"].items():
            print(f"  {k} = {v}")

        r = self.http.request(
            "POST",
            "/sessions",
            json=body,
            headers={"Content-Type": "application/json"},
        )

        try:
            r.raise_for_status()
        except Exception:
            try:
                print("Livy error payload:", r.text)
            except Exception:
                pass
            raise

        js = r.json()
        self.session_id = js["id"]
        self._install_signal_handlers()
        return js

    # -----------------------------
    # Query / Wait / Close
    # -----------------------------
    def state(self):
        r = self.http.request("GET", f"/sessions/{self.session_id}")
        r.raise_for_status()
        return r.json()

    def wait_idle(self, timeout_s: int = 900, interval_s: int = 5):
        start = time.time()
        while True:
            st = self.state()
            s = st.get("state", "").lower()
            if s == "idle":
                return st
            if s in ("dead", "error", "killed", "shutting_down"):
                raise RuntimeError(f"Session {self.session_id} in terminal state: {s}")
            if time.time() - start > timeout_s:
                raise TimeoutError("Timed out waiting for session to become idle")
            time.sleep(interval_s)

    def close(self):
        if self._closed:
            return
        if self.session_id is None:
            self._closed = True
            return
        r = self.http.request("DELETE", f"/sessions/{self.session_id}")
        try:
            r.raise_for_status()
        finally:
            self._closed = True
            self.session_id = None

    # -----------------------------
    # Context manager
    # -----------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if not self.cfg.runner.reuse_session:
            try:
                self.close()
            finally:
                self.clear_session_id()
        return False
