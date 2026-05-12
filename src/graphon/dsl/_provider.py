from __future__ import annotations


def canonical_vendor(provider: str | None) -> str | None:
    if not provider:
        return None
    parts = [part for part in provider.split("/") if part]
    return parts[-1] if parts else provider
