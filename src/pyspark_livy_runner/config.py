from dataclasses import dataclass, field
from typing import Dict, Optional, Union
import os, yaml

class ConfigError(Exception):
    pass

@dataclass
class AuthConfig:
    method: str = "basic"
    username: str = ""
    password: str = ""
    bearer_token: str = ""

@dataclass
class SSLConfig:
    verify: Union[bool, str, None] = True
    client_cert: str = ""
    client_key: str = ""

@dataclass
class LivyConfig:
    endpoint: str = ""
    auth: AuthConfig = field(default_factory=AuthConfig)
    ssl: SSLConfig = field(default_factory=SSLConfig)

@dataclass
class SparkConfig:
    master: str = "yarn"
    deploy_mode: str = "cluster"   # ignoré si blacklisté
    queue: str = "default"
    dynamic_allocation: bool = False
    executor_instances: int = 2
    executor_cores: int = 2
    executor_memory: str = "4g"
    driver_memory: str = "2g"
    extra_conf: Dict[str, str] = field(default_factory=dict)

@dataclass
class RunnerConfig:
    reuse_session: bool = True
    session_file: str = ".livy_session"

@dataclass
class AppConfig:
    livy: LivyConfig = field(default_factory=LivyConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    runner: RunnerConfig = field(default_factory=RunnerConfig)

def _as_auth(d: dict) -> AuthConfig:
    return AuthConfig(**d) if d else AuthConfig()

def _as_ssl(d: dict) -> SSLConfig:
    return SSLConfig(**d) if d else SSLConfig()

def _as_livy(d: dict) -> LivyConfig:
    if not d:
        return LivyConfig()
    auth = _as_auth(d.get("auth", {}))
    ssl = _as_ssl(d.get("ssl", {}))
    return LivyConfig(endpoint=d.get("endpoint", ""), auth=auth, ssl=ssl)

def _as_spark(d: dict) -> SparkConfig:
    """Load SparkConfig and preserve nested extra_conf."""
    if not d:
        return SparkConfig()
    base = {k: v for k, v in d.items() if k != "extra_conf"}
    extra_conf = d.get("extra_conf", {}) or {}
    cfg = SparkConfig(**base)
    cfg.extra_conf = extra_conf
    return cfg

def _as_runner(d: dict) -> RunnerConfig:
    return RunnerConfig(**d) if d else RunnerConfig()

def _validate_basic_auth(auth: AuthConfig) -> None:
    if auth.method.lower() != "basic":
        raise ConfigError("Only 'basic' auth is supported for now. Set livy.auth.method: basic")
    if not auth.username or not auth.password:
        raise ConfigError("Basic auth requires username and password")

def _validate_endpoint(url: str) -> None:
    if not url or not url.startswith(("http://", "https://")):
        raise ConfigError("livy.endpoint must be a valid http(s) URL")

def load_config(path: str) -> AppConfig:
    if not os.path.exists(path):
        raise ConfigError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    livy = _as_livy(data.get("livy", {}))
    spark = _as_spark(data.get("spark", {}))
    runner = _as_runner(data.get("runner", {}))

    _validate_endpoint(livy.endpoint)
    _validate_basic_auth(livy.auth)

    return AppConfig(livy=livy, spark=spark, runner=runner)
