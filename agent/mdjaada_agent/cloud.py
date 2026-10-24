"""Cloud SDK clients shared by node types (created lazily; the SDKs are optional)."""
from typing import Any

from .checks import get

_clients: dict[tuple, Any] = {}


def aws_client(cfg: dict, service: str):
    """boto3 client using the standard AWS credential chain (instance role, env, profile) unless keys are set."""
    import boto3  # type: ignore
    kw = {k: get(cfg, k) for k in ("endpoint_url", "aws_access_key_id", "aws_secret_access_key") if get(cfg, k)}
    if get(cfg, "region"):
        kw["region_name"] = get(cfg, "region")
    key = (service, tuple(sorted(kw.items())))
    if key not in _clients:
        _clients[key] = boto3.session.Session().client(service, **kw)
    return _clients[key]
