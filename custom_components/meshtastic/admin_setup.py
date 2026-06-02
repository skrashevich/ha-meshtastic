# SPDX-FileCopyrightText: 2024-2025 Pascal Brogle @broglep
#
# SPDX-License-Identifier: MIT

"""Background setup for admin-managed child nodes."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.typing import UNDEFINED

from .const import LOGGER
from .data import DATA_COMPONENT
from .entity import GatewayEntity

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .api import MeshtasticApiClient
    from .data import MeshtasticConfigEntry
    from .entity import MeshtasticEntity


async def _add_entities_for_entry(
    hass: HomeAssistant, entities: list[MeshtasticEntity], entry: MeshtasticConfigEntry
) -> None:
    entity_registry = er.async_get(hass)
    device_registry = dr.async_get(hass)

    await hass.data[DATA_COMPONENT].async_add_entities(entities)
    for entity in entities:
        device_id = UNDEFINED
        if entity.device_info:
            device = device_registry.async_get_device(identifiers=entity.device_info["identifiers"])
            if device:
                device_id = device.id
        try:
            entity_registry.async_update_entity(
                entity.entity_id, config_entry_id=entry.entry_id, device_id=device_id
            )
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to update entity %s", entity, exc_info=True)


async def async_add_admin_gateway_entities(
    hass: HomeAssistant,
    entry: MeshtasticConfigEntry,
    client: MeshtasticApiClient,
) -> None:
    """Add gateway diagnostic entities for admin-managed child nodes (non-blocking)."""
    gateway_node = await client.async_get_own_node()
    gateway_node_id = cast("int", gateway_node["num"])
    nodes = await client.async_get_all_nodes()

    admin_entities: list[MeshtasticEntity] = []
    for node_id in entry.runtime_data.admin_managed_nodes:
        if node_id == gateway_node_id:
            continue
        if node_id in entry.runtime_data.admin_gateway_entities_added:
            continue
        node = nodes.get(node_id)
        if node is None:
            continue

        local_config = await client.async_get_remote_local_config(node_id)
        module_config = await client.async_get_remote_module_config(node_id)
        admin_entities.append(
            GatewayEntity(
                config_entry_id=entry.entry_id,
                node=node_id,
                long_name=node["user"]["longName"],
                short_name=node["user"]["shortName"],
                local_config=local_config,
                module_config=module_config,
            )
        )
        entry.runtime_data.admin_gateway_entities_added.add(node_id)

    if admin_entities:
        await _add_entities_for_entry(hass, admin_entities, entry)
