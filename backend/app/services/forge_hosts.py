"""Explicit forge-host registry for Release publishing.

Hosts are never inferred from name substrings. github.com and gitlab.com are
always registered; additional GitLab hosts must be listed in PRISM_FORGE_HOSTS.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Mapping

ForgeKind = Literal["github", "gitlab"]

_HOST_RE = re.compile(
    r"^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$"
)
_PAIR_RE = re.compile(r"^([^=]+)=([^=]+)$")


@dataclass(frozen=True)
class ForgeHost:
    host: str
    kind: ForgeKind
    api_root: str
    token_name: str
    display_name: str


_DEFAULT_HOSTS: tuple[ForgeHost, ...] = (
    ForgeHost(
        host="github.com",
        kind="github",
        api_root="https://api.github.com",
        token_name="GITHUB_TOKEN",
        display_name="GitHub",
    ),
    ForgeHost(
        host="gitlab.com",
        kind="gitlab",
        api_root="https://gitlab.com/api/v4",
        token_name="GITLAB_TOKEN",
        display_name="GitLab",
    ),
)


class ForgeHostConfigError(ValueError):
    """PRISM_FORGE_HOSTS is not a valid host=kind list."""


def default_forge_hosts() -> dict[str, ForgeHost]:
    return {item.host: item for item in _DEFAULT_HOSTS}


def parse_forge_hosts(raw: str) -> dict[str, ForgeHost]:
    """Return built-in hosts plus validated extras from ``host=kind`` pairs."""

    hosts = default_forge_hosts()
    for index, part in enumerate(_split_pairs(raw), start=1):
        match = _PAIR_RE.match(part)
        if match is None:
            raise ForgeHostConfigError(
                f"PRISM_FORGE_HOSTS entry {index} must be host=gitlab"
            )
        host = _require_host(match.group(1).strip())
        _require_kind(match.group(2).strip())
        if host in hosts:
            raise ForgeHostConfigError(
                f"PRISM_FORGE_HOSTS cannot redefine {host}; built-in hosts stay as they are"
            )
        hosts[host] = ForgeHost(
            host=host,
            kind="gitlab",
            api_root=f"https://{host}/api/v4",
            token_name="GITLAB_TOKEN",
            display_name="GitLab",
        )
    return hosts


def resolve_forge_host(
    hostname: str,
    raw_extra: str,
    *,
    registry: Mapping[str, ForgeHost] | None = None,
) -> ForgeHost | None:
    hosts = registry if registry is not None else parse_forge_hosts(raw_extra)
    return hosts.get(hostname.casefold())


def _split_pairs(raw: str) -> list[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


def _require_host(value: str) -> str:
    host = value.casefold().removeprefix("[").removesuffix("]")
    if host.startswith("http://") or host.startswith("https://") or "/" in host or ":" in host:
        raise ForgeHostConfigError(
            "PRISM_FORGE_HOSTS hosts must be bare hostnames, not URLs"
        )
    if not _HOST_RE.match(host):
        raise ForgeHostConfigError(f"PRISM_FORGE_HOSTS host {value!r} is not a valid hostname")
    return host


def _require_kind(value: str) -> ForgeKind:
    kind = value.strip().casefold()
    if kind != "gitlab":
        raise ForgeHostConfigError(
            "PRISM_FORGE_HOSTS kinds must be gitlab"
        )
    return "gitlab"
