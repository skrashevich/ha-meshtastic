# SPDX-FileCopyrightText: 2024-2025 Pascal Brogle @broglep
#
# SPDX-License-Identifier: MIT

"""Helpers for remote admin access via Meshtastic security admin keys."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Iterable
from typing import Any

NODE_DATA_ADMIN_MANAGED = "adminManaged"


def normalize_key_bytes(key: bytes | str | None) -> bytes | None:
    """Normalize admin/public key material to raw bytes."""
    if key is None:
        return None
    if isinstance(key, str):
        raw = key.strip()
        if raw.startswith("base64:"):
            raw = raw.removeprefix("base64:")
        try:
            return base64.b64decode(raw, validate=True)
        except (ValueError, binascii.Error):
            return raw.encode("utf-8")
    if isinstance(key, (bytes, bytearray)):
        return bytes(key)
    return None


def admin_key_list_contains(admin_keys: Iterable[bytes | str | None], gateway_public_key: bytes | str | None) -> bool:
    """Return True when the gateway public key is listed as an admin key on a remote node."""
    gateway_key = normalize_key_bytes(gateway_public_key)
    if not gateway_key:
        return False

    for admin_key in admin_keys:
        normalized = normalize_key_bytes(admin_key)
        if normalized and normalized == gateway_key:
            return True
    return False


def security_config_admin_keys(security_config: Any) -> list[bytes | str]:
    """Extract admin keys from a protobuf SecurityConfig or dict representation."""
    if security_config is None:
        return []

    if isinstance(security_config, dict):
        keys = security_config.get("adminKey") or security_config.get("admin_key") or []
        return list(keys)

    admin_keys = getattr(security_config, "admin_key", None)
    if admin_keys is None:
        return []
    return list(admin_keys)
