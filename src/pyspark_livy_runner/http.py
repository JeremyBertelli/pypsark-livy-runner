import requests
from .config import AppConfig

class HttpClient:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.s = requests.Session()

        # Auth
        if cfg.livy.auth.method == "basic":
            self.s.auth = (cfg.livy.auth.username, cfg.livy.auth.password)
        elif cfg.livy.auth.method == "bearer":
            self.s.headers.update({"Authorization": f"Bearer {cfg.livy.auth.bearer_token}"})

        # SSL
        self.verify = cfg.livy.ssl.verify
        self.cert = None
        if cfg.livy.ssl.client_cert and cfg.livy.ssl.client_key:
            self.cert = (cfg.livy.ssl.client_cert, cfg.livy.ssl.client_key)
        elif cfg.livy.ssl.client_cert:
            self.cert = cfg.livy.ssl.client_cert

    def request(self, method: str, path: str, **kw):
        url = self.cfg.livy.endpoint.rstrip("/") + "/" + path.lstrip("/")
        return self.s.request(method, url, verify=self.verify, cert=self.cert, **kw)
