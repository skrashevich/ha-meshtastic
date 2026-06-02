# SPDX-License-Identifier: MIT

"""Tests for Meshtastic admin key helpers."""

from __future__ import annotations

import base64

from custom_components.meshtastic.admin_access import (
    admin_key_list_contains,
    normalize_key_bytes,
    security_config_admin_keys,
)


def test_normalize_key_bytes_from_base64_string() -> None:
    raw = b"\x01\x02\x03\xab\xcd"
    encoded = "base64:" + base64.b64encode(raw).decode("ascii")
    assert normalize_key_bytes(encoded) == raw


def test_admin_key_list_contains_matches_gateway_public_key() -> None:
    gateway_key = b"gateway-public-key-32-bytes-long!!"
    admin_keys = [b"other-key", gateway_key]
    assert admin_key_list_contains(admin_keys, gateway_key)


def test_admin_key_list_contains_from_security_dict() -> None:
    gateway_key = b"gateway-public-key-32-bytes-long!!"
    security = {"adminKey": [base64.b64encode(gateway_key).decode("ascii")]}
    assert admin_key_list_contains(security_config_admin_keys(security), gateway_key)


def test_admin_key_list_contains_rejects_missing_key() -> None:
    gateway_key = b"gateway-public-key-32-bytes-long!!"
    assert not admin_key_list_contains([b"another-key"], gateway_key)
