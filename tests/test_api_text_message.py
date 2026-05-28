# SPDX-License-Identifier: MIT

"""Tests for MeshtasticApiClient text message events."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from custom_components.meshtastic.aiomeshtastic.interface import MeshInterface
from custom_components.meshtastic.aiomeshtastic.packet import Packet
from custom_components.meshtastic.aiomeshtastic.protobuf import channel_pb2, mesh_pb2

# api.py imports homeassistant at module level; conftest registers stubs first.
from custom_components.meshtastic.api import (
    EVENT_MESHTASTIC_API_TEXT_MESSAGE,
    MeshtasticApiClient,
)


@pytest.fixture
def api_client():
    hass = MagicMock()
    hass.bus = MagicMock()
    hass.bus.async_fire = MagicMock()
    interface = MagicMock()
    interface.connected_node_channels.return_value = []
    client = MeshtasticApiClient.__new__(MeshtasticApiClient)
    client._hass = hass
    client._config_entry_id = "entry-id"
    client._interface = interface
    client.get_own_node = MagicMock(return_value={"num": 0x11223344})
    client._build_event_data = MeshtasticApiClient._build_event_data.__get__(client, MeshtasticApiClient)
    return client


def _broadcast_text_packet(channel_index: int = 2) -> Packet:
    from_radio = mesh_pb2.FromRadio()
    mesh_packet = from_radio.packet
    mesh_packet.id = 99
    setattr(mesh_packet, "from", 0xAABBCCDD)
    mesh_packet.to = MeshInterface.BROADCAST_NUM
    mesh_packet.channel = channel_index
    mesh_packet.decoded.payload = b"hello mesh"
    return Packet(from_radio)


@pytest.mark.asyncio
async def test_text_message_includes_channel_name(api_client):
    longfast = channel_pb2.Channel()
    longfast.index = 2
    longfast.settings.name = "LongFast"
    api_client._interface.connected_node_channels.return_value = [channel_pb2.Channel(), channel_pb2.Channel(), longfast]

    packet = _broadcast_text_packet(channel_index=2)
    node = MagicMock()
    node.id = 0xAABBCCDD

    await api_client._on_text_message(node, packet)

    api_client._hass.bus.async_fire.assert_called_once()
    event_type, event_data = api_client._hass.bus.async_fire.call_args[0]
    assert event_type == EVENT_MESHTASTIC_API_TEXT_MESSAGE
    data = event_data["data"]
    assert data["to"]["channel"] == 2
    assert data["to"]["channel_name"] == "LongFast"


@pytest.mark.asyncio
async def test_text_message_channel_name_empty_when_unknown(api_client):
    api_client._interface.connected_node_channels.return_value = []

    packet = _broadcast_text_packet(channel_index=5)
    node = MagicMock()
    node.id = 0xAABBCCDD

    await api_client._on_text_message(node, packet)

    data = api_client._hass.bus.async_fire.call_args[0][1]["data"]
    assert data["to"]["channel_name"] == ""
