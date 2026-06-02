# SPDX-FileCopyrightText: 2024-2025 Pascal Brogle @broglep
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
import contextlib
from copy import deepcopy
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

import google
from google.protobuf.json_format import MessageToDict
from homeassistant.exceptions import IntegrationError

from .aiomeshtastic import (
    BluetoothConnection as AioBluetoothConnection,
)
from .aiomeshtastic import (
    MeshInterface,
)
from .aiomeshtastic import (
    MeshInterface as AioMeshInterface,
)
from .aiomeshtastic import (
    SerialConnection as AioSerialConnection,
)
from .aiomeshtastic import (
    TcpConnection as AioTcpConnection,
)
from .aiomeshtastic.errors import MeshRoutingError, MeshtasticError
from .aiomeshtastic.interface import TelemetryType
from .aiomeshtastic.protobuf import portnums_pb2
from .const import (
    ADMIN_POLL_TIMEOUT,
    CONF_CONNECTION_BLUETOOTH_ADDRESS,
    CONF_CONNECTION_SERIAL_PORT,
    CONF_CONNECTION_TCP_HOST,
    CONF_CONNECTION_TCP_PORT,
    CONF_CONNECTION_TYPE,
    DOMAIN,
    LOGGER,
    ConnectionType,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine, Mapping, MutableMapping
    from types import MappingProxyType, TracebackType

    from google.protobuf.message import Message
    from homeassistant.core import HomeAssistant

    from .aiomeshtastic.interface import MeshNode, TelemetryType
    from .aiomeshtastic.packet import Packet

_LOGGER = LOGGER.getChild(__name__)


EVENT_MESHTASTIC_API_BASE = f"{DOMAIN}_api"
EVENT_MESHTASTIC_API_NODE_UPDATED = EVENT_MESHTASTIC_API_BASE + "_node_updated"
EVENT_MESHTASTIC_API_TELEMETRY = EVENT_MESHTASTIC_API_BASE + "_telemetry"
EVENT_MESHTASTIC_API_PACKET = EVENT_MESHTASTIC_API_BASE + "_packet"
EVENT_MESHTASTIC_API_TEXT_MESSAGE = EVENT_MESHTASTIC_API_BASE + "_text_message"
EVENT_MESHTASTIC_API_POSITION = EVENT_MESHTASTIC_API_BASE + "_position"

ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID = "config_entry_id"
ATTR_EVENT_MESHTASTIC_API_NODE = "node"
ATTR_EVENT_MESHTASTIC_API_DATA = "data"
ATTR_EVENT_MESHTASTIC_API_TELEMETRY_TYPE = "telemetry_type"
ATTR_EVENT_MESHTASTIC_API_NODE_INFO = "node_info"


class EventMeshtasticApiTelemetryType(StrEnum):
    DEVICE_METRICS = "device_metrics"
    LOCAL_STATS = "local_stats"
    ENVIRONMENT_METRICS = "environment_metrics"
    POWER_METRICS = "power_metrics"
    AIR_QUALITY_METRICS = "air_quality_metrics"


class MeshtasticApiClientError(IntegrationError):
    """Exception to indicate a general API error."""


class MeshtasticApiClientCommunicationError(
    MeshtasticApiClientError,
):
    """Exception to indicate a communication error."""


class MeshtasticApiClient:
    def __init__(  # noqa: PLR0913
        self,
        data: MappingProxyType[str, Any],
        hass: HomeAssistant,
        config_entry_id: str | None,
        *,
        no_nodes: bool = False,
        enable_mqtt_downlink: bool = False,
        enable_mqtt_uplink_relay: bool = False,
    ) -> None:
        self._logger = LOGGER.getChild(self.__class__.__name__)
        self._connected = asyncio.Event()
        self._hass = hass
        self._config_entry_id = config_entry_id

        connection_type = data[CONF_CONNECTION_TYPE]

        if connection_type == ConnectionType.TCP.value:
            connection = AioTcpConnection(host=data[CONF_CONNECTION_TCP_HOST], port=data[CONF_CONNECTION_TCP_PORT])
        elif connection_type == ConnectionType.BLUETOOTH.value:
            ble_address = data[CONF_CONNECTION_BLUETOOTH_ADDRESS]
            ble_device = None
            if hass:
                from homeassistant.components.bluetooth import async_ble_device_from_address

                ble_device = async_ble_device_from_address(hass, ble_address, connectable=True)
            connection = AioBluetoothConnection(ble_address=ble_address, ble_device=ble_device)
        elif connection_type == ConnectionType.SERIAL.value:
            connection = AioSerialConnection(device=data[CONF_CONNECTION_SERIAL_PORT])
        else:
            msg = f"Unsupported connection type {connection_type}"
            raise ValueError(msg)

        self._interface = AioMeshInterface(
            connection=connection,
            no_nodes=no_nodes,
            heartbeat_interval=timedelta(minutes=5),
            enable_mqtt_downlink=enable_mqtt_downlink,
            enable_mqtt_uplink_relay=enable_mqtt_uplink_relay,
        )
        self._packet_processor: asyncio.Task | None = None
        self._background_tasks: set[asyncio.Task] = set()

        self._interface.add_packet_app_listener(
            packet_type=portnums_pb2.PortNum.NODEINFO_APP, callback=self._on_node_info, as_dict=True
        )
        self._interface.add_packet_app_listener(
            packet_type=portnums_pb2.PortNum.TEXT_MESSAGE_APP, callback=self._on_text_message, as_packet=True
        )
        self._interface.add_packet_app_listener(
            packet_type=portnums_pb2.PortNum.TELEMETRY_APP, callback=self._on_telemetry, as_dict=True
        )
        self._interface.add_packet_app_listener(
            packet_type=portnums_pb2.PortNum.POSITION_APP, callback=self._on_position, as_dict=True
        )

    async def connect(self) -> None:
        try:
            await asyncio.wait_for(self._interface.start(), timeout=30)
        except Exception as e:
            raise MeshtasticApiClientCommunicationError from e

        try:
            ready = await asyncio.wait_for(self._interface.connected_node_ready(), timeout=60)
            exception = None
        except Exception as e:  # noqa: BLE001
            ready = False
            exception = e

        if not ready:
            with contextlib.suppress(Exception):
                await self._interface.stop()
            if exception:
                raise MeshtasticApiClientCommunicationError from exception
            raise MeshtasticApiClientCommunicationError

        self._packet_processor = asyncio.create_task(self._process_meshtastic_packet())

        async def send_time() -> None:
            await asyncio.sleep(1)
            try:
                await self._interface.send_time()
                await self._interface.write_timezone_if_needed()
            except:  # noqa: E722
                self._logger.debug("Send time failed", exc_info=True)

        self._add_background_task(send_time())

    async def disconnect(self) -> None:
        try:
            self._packet_processor.cancel()
            await self._interface.stop()
        except Exception as e:
            raise MeshtasticApiClientCommunicationError from e

    async def async_get_channels(self) -> list[Mapping[str, Any]]:
        if not await self._interface.connected_node_ready():
            return []
        return [self._message_to_dict(c) for c in self._interface.connected_node_channels()]

    async def async_get_node_local_config(self) -> dict:
        if not await self._interface.connected_node_ready():
            return {}
        return self._message_to_dict(self._interface.connected_node_local_config())

    async def async_get_node_module_config(self) -> dict:
        if not await self._interface.connected_node_ready():
            return {}
        return self._message_to_dict(self._interface.connected_node_module_config())

    async def async_get_own_node(self) -> Mapping[str, Any]:
        if not await self._interface.connected_node_ready():
            return {}
        return self.get_own_node()

    def get_own_node(self) -> Mapping[str, Any]:
        return self._interface.connected_node() or {}

    def get_node_info(self, node_id: int) -> MeshNode | None:
        return self._interface.find_node(node_id=node_id)

    async def async_get_all_nodes(self) -> Mapping[int, Mapping[str, Any]]:
        await self._interface.connected_node_ready()
        return {node_id: self._transform_node_info(node_info) for node_id, node_info in self._interface.nodes().items()}

    def _transform_node_info(self, node_info: Mapping[str, Any]) -> Mapping[str, Any]:
        transformed = deepcopy(node_info)
        if "position" in transformed:
            self._modify_position(transformed["position"])

        return transformed

    async def send_text(
        self,
        text: str,
        destination_id: int | str = MeshInterface.BROADCAST_ADDR,
        *,
        want_ack: bool = False,
        channel_index: int | None = None,
    ) -> bool:
        try:
            await asyncio.wait_for(
                self._interface.send_text_message(
                    text,
                    destination=destination_id,
                    want_ack=want_ack,
                    channel_index=channel_index,
                ),
                timeout=30,
            )
        except TimeoutError:
            return False
        except Exception as e:
            raise MeshtasticApiClientError from e
        else:
            return True

    @property
    def metadata(self) -> Mapping[str, Any]:
        metadata = self._interface.connected_node_metadata()
        return MessageToDict(metadata) if metadata is not None else {}

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.disconnect()

    def _build_event_data(self, node_id: int, data: Mapping[str, Any]) -> MutableMapping[str, Any]:
        return {
            ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID: self._config_entry_id,
            ATTR_EVENT_MESHTASTIC_API_NODE: node_id,
            ATTR_EVENT_MESHTASTIC_API_DATA: data,
        }

    async def _on_node_info(self, node: MeshNode, info: dict[str, Any]) -> None:
        event_data = self._build_event_data(node.id, info)
        position = event_data.get(ATTR_EVENT_MESHTASTIC_API_DATA, {}).get("position", {})
        if position:
            self._modify_position(position)

        self._hass.bus.async_fire(EVENT_MESHTASTIC_API_NODE_UPDATED, event_data)

    def _channel_name(self, channel_index: int | None) -> str | None:
        if channel_index is None:
            return None
        channels = self._interface.connected_node_channels()
        if not channels or channel_index >= len(channels):
            return None
        return channels[channel_index].settings.name or "LongFast"

    async def _on_text_message(self, node: MeshNode, packet: Packet) -> None:
        if packet.to_id == MeshInterface.BROADCAST_NUM:
            to_channel = packet.channel_index
            to_node = None
        else:
            to_channel = None
            to_node = packet.to_id

        to: dict[str, Any] = {"node": to_node, "channel": to_channel}
        if to_channel is not None:
            to["channel_name"] = self._channel_name(to_channel) or ""

        payload: dict[str, Any] = {
            "from": packet.from_id,
            "to": to,
            "gateway": self.get_own_node()["num"],
            "message": packet.app_payload,
        }

        event_data = self._build_event_data(node.id, payload)

        event_data["message_id"] = packet.mesh_packet.id
        self._hass.bus.async_fire(EVENT_MESHTASTIC_API_TEXT_MESSAGE, event_data)

    async def _on_telemetry(self, node: MeshNode, telemetry: dict[str, Any]) -> None:
        device_metrics = telemetry.get("deviceMetrics")
        local_stats = telemetry.get("localStats")
        environment_metrics = telemetry.get("environmentMetrics")
        power_metrics = telemetry.get("powerMetrics")

        node_info = {"name": node.long_name}
        if device_metrics:
            event_data = self._build_event_data(node.id, device_metrics)
            event_data[ATTR_EVENT_MESHTASTIC_API_NODE_INFO] = node_info
            event_data[ATTR_EVENT_MESHTASTIC_API_TELEMETRY_TYPE] = EventMeshtasticApiTelemetryType.DEVICE_METRICS
            self._hass.bus.async_fire(EVENT_MESHTASTIC_API_TELEMETRY, event_data)

        if local_stats:
            event_data = self._build_event_data(node.id, local_stats)
            event_data[ATTR_EVENT_MESHTASTIC_API_NODE_INFO] = node_info
            event_data[ATTR_EVENT_MESHTASTIC_API_TELEMETRY_TYPE] = EventMeshtasticApiTelemetryType.LOCAL_STATS
            self._hass.bus.async_fire(EVENT_MESHTASTIC_API_TELEMETRY, event_data)

        if environment_metrics:
            event_data = self._build_event_data(node.id, environment_metrics)
            event_data[ATTR_EVENT_MESHTASTIC_API_NODE_INFO] = node_info
            event_data[ATTR_EVENT_MESHTASTIC_API_TELEMETRY_TYPE] = EventMeshtasticApiTelemetryType.ENVIRONMENT_METRICS
            self._hass.bus.async_fire(EVENT_MESHTASTIC_API_TELEMETRY, event_data)

        if power_metrics:
            event_data = self._build_event_data(node.id, power_metrics)
            event_data[ATTR_EVENT_MESHTASTIC_API_NODE_INFO] = node_info
            event_data[ATTR_EVENT_MESHTASTIC_API_TELEMETRY_TYPE] = EventMeshtasticApiTelemetryType.POWER_METRICS
            self._hass.bus.async_fire(EVENT_MESHTASTIC_API_TELEMETRY, event_data)

        air_quality_metrics = telemetry.get("airQualityMetrics")
        if air_quality_metrics:
            event_data = self._build_event_data(node.id, air_quality_metrics)
            event_data[ATTR_EVENT_MESHTASTIC_API_NODE_INFO] = node_info
            event_data[ATTR_EVENT_MESHTASTIC_API_TELEMETRY_TYPE] = EventMeshtasticApiTelemetryType.AIR_QUALITY_METRICS
            self._hass.bus.async_fire(EVENT_MESHTASTIC_API_TELEMETRY, event_data)

    async def _publish_telemetry_dict(self, node_id: int, telemetry: Mapping[str, Any]) -> None:
        node = self.get_node_info(node_id) or self._interface.find_node(node_id=node_id)
        if node is None:
            from .aiomeshtastic.interface import MeshNode

            node = MeshNode.stub_node(node_id)
        await self._on_telemetry(node, dict(telemetry))

    async def _publish_position_dict(self, node_id: int, position: Mapping[str, Any]) -> None:
        node = self.get_node_info(node_id) or self._interface.find_node(node_id=node_id)
        if node is None:
            from .aiomeshtastic.interface import MeshNode

            node = MeshNode.stub_node(node_id)
        await self._on_position(node, dict(position))

    async def async_node_has_gateway_admin_access(self, node_id: int) -> bool:
        return await self._interface.node_has_gateway_admin_access(node_id)

    async def _poll_telemetry_type(self, node_id: int, telemetry_type: TelemetryType) -> None:
        try:
            telemetry = await self.request_telemetry(node_id, telemetry_type, timeout=ADMIN_POLL_TIMEOUT)
        except MeshtasticApiClientError:
            self._logger.debug(
                "Admin telemetry poll failed for node %s (%s)",
                node_id,
                telemetry_type,
                exc_info=True,
            )
        else:
            await self._interface.apply_telemetry_dict(node_id, telemetry)
            await self._publish_telemetry_dict(node_id, telemetry)

    async def poll_admin_managed_node(self, node_id: int) -> None:
        """Request telemetry and position from a node that trusts this gateway as admin."""
        await asyncio.gather(
            *(self._poll_telemetry_type(node_id, telemetry_type) for telemetry_type in TelemetryType),
            return_exceptions=True,
        )

        try:
            position = await self.request_position(node_id, timeout=ADMIN_POLL_TIMEOUT)
        except MeshtasticApiClientError:
            self._logger.debug("Admin position poll failed for node %s", node_id, exc_info=True)
        else:
            await self._interface.apply_position_dict(node_id, position)
            await self._publish_position_dict(node_id, position)

    async def async_get_remote_local_config(self, node_id: int) -> dict[str, Any]:
        from .aiomeshtastic.protobuf import admin_pb2

        local_config: dict[str, Any] = {}
        config_field_map = {
            admin_pb2.AdminMessage.ConfigType.DEVICE_CONFIG: "device",
            admin_pb2.AdminMessage.ConfigType.POSITION_CONFIG: "position",
            admin_pb2.AdminMessage.ConfigType.POWER_CONFIG: "power",
            admin_pb2.AdminMessage.ConfigType.NETWORK_CONFIG: "network",
            admin_pb2.AdminMessage.ConfigType.DISPLAY_CONFIG: "display",
            admin_pb2.AdminMessage.ConfigType.LORA_CONFIG: "lora",
            admin_pb2.AdminMessage.ConfigType.BLUETOOTH_CONFIG: "bluetooth",
            admin_pb2.AdminMessage.ConfigType.SECURITY_CONFIG: "security",
        }
        for config_type, field_name in config_field_map.items():
            config = await self._interface.get_remote_config(node_id, config_type)
            if config is not None and config.HasField(field_name):
                local_config[field_name] = self._message_to_dict(getattr(config, field_name))
        return local_config

    async def async_get_remote_module_config(self, node_id: int) -> dict[str, Any]:
        from .aiomeshtastic.protobuf import admin_pb2

        module_config: dict[str, Any] = {}
        module_field_map = {
            admin_pb2.AdminMessage.ModuleConfigType.MQTT_CONFIG: "mqtt",
            admin_pb2.AdminMessage.ModuleConfigType.SERIAL_CONFIG: "serial",
            admin_pb2.AdminMessage.ModuleConfigType.EXTNOTIF_CONFIG: "external_notification",
            admin_pb2.AdminMessage.ModuleConfigType.STOREFORWARD_CONFIG: "store_forward",
            admin_pb2.AdminMessage.ModuleConfigType.RANGETEST_CONFIG: "range_test",
            admin_pb2.AdminMessage.ModuleConfigType.TELEMETRY_CONFIG: "telemetry",
            admin_pb2.AdminMessage.ModuleConfigType.CANNEDMSG_CONFIG: "canned_message",
            admin_pb2.AdminMessage.ModuleConfigType.AUDIO_CONFIG: "audio",
            admin_pb2.AdminMessage.ModuleConfigType.REMOTEHARDWARE_CONFIG: "remote_hardware",
            admin_pb2.AdminMessage.ModuleConfigType.NEIGHBORINFO_CONFIG: "neighbor_info",
            admin_pb2.AdminMessage.ModuleConfigType.AMBIENTLIGHTING_CONFIG: "ambient_lighting",
            admin_pb2.AdminMessage.ModuleConfigType.DETECTIONSENSOR_CONFIG: "detection_sensor",
            admin_pb2.AdminMessage.ModuleConfigType.PAXCOUNTER_CONFIG: "paxcounter",
        }
        for module_type, field_name in module_field_map.items():
            config = await self._interface.get_remote_module_config(node_id, module_type)
            if config is not None and config.HasField(field_name):
                module_config[field_name] = self._message_to_dict(getattr(config, field_name))
        return module_config

    async def async_refresh_admin_managed_nodes(
        self,
        node_ids: list[int],
        admin_managed_nodes: set[int],
        admin_denied_nodes: set[int],
    ) -> tuple[set[int], set[int]]:
        """Detect admin-managed child nodes and poll their restricted data."""
        gateway_id = self.get_own_node().get("num")
        node_id_set = set(node_ids)
        updated_admin_nodes = {node for node in admin_managed_nodes if node in node_id_set}
        updated_denied_nodes = {node for node in admin_denied_nodes if node in node_id_set}

        try:
            for node_id in node_id_set:
                if node_id == gateway_id:
                    continue

                if node_id not in updated_admin_nodes:
                    if node_id in updated_denied_nodes:
                        continue
                    if not await self.async_node_has_gateway_admin_access(node_id):
                        updated_denied_nodes.add(node_id)
                        continue
                    updated_admin_nodes.add(node_id)
                    self._logger.info("Node %s grants admin access to this gateway", node_id)

                await self.poll_admin_managed_node(node_id)
        except asyncio.CancelledError:
            self._logger.debug("Admin refresh cancelled while polling nodes")
            raise

        return updated_admin_nodes, updated_denied_nodes

    async def _on_position(self, node: MeshNode, position: dict[str, Any]) -> None:
        self._modify_position(position)

        event_data = self._build_event_data(node.id, position)
        node_info = {"name": node.long_name}
        event_data[ATTR_EVENT_MESHTASTIC_API_NODE_INFO] = node_info
        self._hass.bus.async_fire(EVENT_MESHTASTIC_API_POSITION, event_data)

    def _modify_position(self, position: dict[str, Any]) -> None:
        if "latitudeI" in position:
            position["latitude"] = float(position["latitudeI"] * 10**-7)
        if "longitudeI" in position:
            position["longitude"] = float(position["longitudeI"] * 10**-7)

    async def _process_meshtastic_packet(self) -> None:
        async for mesh_packet in self._interface.packet_stream():
            try:
                from_node = getattr(mesh_packet, "from", None)
                if from_node is None:
                    continue

                packet_clone = dict(self._message_to_dict(mesh_packet))
                packet_clone.setdefault("from", from_node)
                self._hass.bus.async_fire(
                    EVENT_MESHTASTIC_API_PACKET,
                    self._build_event_data(from_node, packet_clone),
                )
            except Exception:  # noqa: BLE001
                self._logger.debug("Failed to process packet %s", mesh_packet, exc_info=True)

    def _add_background_task(self, coro: Coroutine[Any, Any, None], name: str | None = None) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _message_to_dict(self, message: Message) -> Mapping[str, Any]:
        try:
            return MessageToDict(message, always_print_fields_with_no_presence=True)
        except TypeError:
            # older protobuf version
            return MessageToDict(message, including_default_value_fields=True)

    async def request_telemetry(
        self, node: int, telemetry_type: TelemetryType, timeout: float | None = None
    ) -> Mapping[str, Any]:
        try:
            kwargs = {} if timeout is None else {"timeout": timeout}
            response = await self._interface.request_telemetry(node, telemetry_type=telemetry_type, **kwargs)
            return self._message_to_dict(response)
        except MeshRoutingError as e:
            msg = f"No response for {telemetry_type}"
            raise MeshtasticApiClientError(msg) from e
        except MeshtasticError as e:
            raise MeshtasticApiClientError(str(e)) from e

    async def request_position(self, node: int, timeout: float | None = None) -> Mapping[str, Any]:
        try:
            kwargs = {} if timeout is None else {"timeout": timeout}
            response = await self._interface.request_position(node, **kwargs)
            return self._message_to_dict(response)
        except MeshtasticError as e:
            raise MeshtasticApiClientError(str(e)) from e

    async def request_traceroute(self, node: int) -> Mapping[str, Any]:
        try:
            response = await self._interface.request_traceroute(node)
            return self._message_to_dict(response)
        except MeshtasticError as e:
            raise MeshtasticApiClientError(str(e)) from e
