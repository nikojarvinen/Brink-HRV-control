"""Support for the Brink-home API."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_SCAN_INTERVAL, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import DeviceEntry
from homeassistant.helpers.event import async_track_time_change

_LOGGER = logging.getLogger(__name__)

from .automation_controller import boost_store
from .const import (
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MIN_SCAN_INTERVAL,
    PARAM_DAYS_SINCE_FILTER_RESET,
    PARAMETER_REFRESH_HOUR,
    VALUE_STATE_PENDING,
)
from .coordinator import BrinkDataCoordinator
from .core.brink_home_cloud import BrinkAuthError, BrinkHomeCloud, BrinkRefreshError

PLATFORMS = [
    Platform.BINARY_SENSOR,
    Platform.SELECT,
    Platform.SENSOR,
    Platform.SWITCH,
]


@dataclass
class BrinkRuntimeData:
    """Runtime data for the Brink integration."""

    client: BrinkHomeCloud
    coordinator: BrinkDataCoordinator


type BrinkConfigEntry = ConfigEntry[BrinkRuntimeData]


def _migrate_options(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Migrate options from v1.x to v2.0.

    - Clamp scan_interval to the new minimum (45s).
    """
    opts = dict(entry.options)
    changed = False

    scan_interval = opts.get(CONF_SCAN_INTERVAL)
    if scan_interval is not None and int(scan_interval) < MIN_SCAN_INTERVAL:
        _LOGGER.info(
            "Migrating scan_interval from %s to %s (new minimum)",
            scan_interval,
            MIN_SCAN_INTERVAL,
        )
        opts[CONF_SCAN_INTERVAL] = MIN_SCAN_INTERVAL
        changed = True

    if changed:
        hass.config_entries.async_update_entry(entry, options=opts)


def _migrate_device_identifiers(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Migrate device identifiers from v1.x 3-tuple to v2.0 2-tuple format.

    v1.x used (DOMAIN, system_id, gateway_id) — a non-standard 3-element tuple.
    v2.0 uses (DOMAIN, str(system_id)) — the standard 2-element format.
    Without this migration, HA would create duplicate devices on upgrade.
    """
    dev_reg = dr.async_get(hass)
    for device in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
        new_ids: set[tuple[str, str]] = set()
        migrated = False
        for ident in device.identifiers:
            if ident[0] == DOMAIN and len(ident) == 3:
                # Old format: (DOMAIN, system_id, gateway_id) → (DOMAIN, str(system_id))
                new_ids.add((DOMAIN, str(ident[1])))
                migrated = True
            else:
                new_ids.add(ident)

        if migrated:
            _LOGGER.info(
                "Migrating device identifiers for %s: %s → %s",
                device.name,
                device.identifiers,
                new_ids,
            )
            dev_reg.async_update_device(device.id, new_identifiers=new_ids)


async def async_setup_entry(hass: HomeAssistant, entry: BrinkConfigEntry) -> bool:
    """Set up Brink home from a config entry."""
    # Run v1.x → v2.0 migrations
    _migrate_options(hass, entry)
    _migrate_device_identifiers(hass, entry)

    username: str = entry.data[CONF_USERNAME]
    password: str = entry.data[CONF_PASSWORD]

    session = async_get_clientsession(hass)
    brink_client = BrinkHomeCloud(session, username, password)

    try:
        await brink_client.login()
    except BrinkAuthError as ex:
        await brink_client.close()
        if ex.is_credentials_error:
            raise ConfigEntryAuthFailed from ex
        raise ConfigEntryNotReady from ex
    except aiohttp.ClientResponseError as ex:
        await brink_client.close()
        if ex.status == 401:
            raise ConfigEntryAuthFailed from ex
        raise ConfigEntryNotReady from ex
    except (aiohttp.ClientError, TimeoutError) as ex:
        await brink_client.close()
        raise ConfigEntryNotReady from ex

    coordinator: BrinkDataCoordinator | None = None
    try:
        coordinator = BrinkDataCoordinator(hass, brink_client, entry)
        await coordinator.async_config_entry_first_refresh()

        # Restore Adaptive (HA) mode if it was active before restart
        await coordinator.automation_controller.async_restore_state()

        # Start humidity monitoring (delta sensors) regardless of automation state
        await coordinator.automation_controller.async_start_humidity_monitoring()
    except Exception:
        # Stop timers the controller/coordinator may already have started so
        # they don't keep firing against a closed client.
        if coordinator is not None:
            try:
                await coordinator.automation_controller.async_cleanup()
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Automation cleanup failed during setup abort", exc_info=True)
            try:
                coordinator.cancel_expedited_polling()
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Expedited polling cancel failed during setup abort", exc_info=True)
        await brink_client.close()
        raise

    entry.runtime_data = BrinkRuntimeData(
        client=brink_client,
        coordinator=coordinator,
    )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    @callback
    def _start_parameter_refresh(_now: datetime) -> None:
        entry.async_create_background_task(
            hass,
            _async_refresh_on_demand_parameters(entry),
            f"{DOMAIN} on-demand parameter refresh",
        )

    # Daily, around 05:00 local: make the cloud read values it otherwise
    # never refreshes. A stable per-entry offset (05:00-05:29) keeps all
    # installations from hitting the Brink service in the same second.
    offset = int(hashlib.sha256(entry.entry_id.encode()).hexdigest()[:8], 16)
    entry.async_on_unload(
        async_track_time_change(
            hass,
            _start_parameter_refresh,
            hour=PARAMETER_REFRESH_HOUR,
            minute=offset % 30,
            second=(offset // 30) % 60,
        )
    )

    return True


def _on_demand_value_ids(device: dict[str, Any]) -> list[int]:
    """Return value ids the cloud only refreshes while a client subscribes.

    Days since filter reset is always included (the cloud marks it fresh
    right after a refresh, then lets it go stale); any other parameter the
    API reports as Pending is included too.
    """
    value_ids: list[int] = []
    for component in device.get("components") or []:
        if not isinstance(component, dict):
            continue
        params = component.get("parameters") or {}
        if not isinstance(params, dict):
            continue
        for key, param in params.items():
            if not isinstance(param, dict):
                continue
            value_id = param.get("value_id")
            if not isinstance(value_id, int) or isinstance(value_id, bool):
                continue
            if (
                key == PARAM_DAYS_SINCE_FILTER_RESET
                or param.get("value_state") == VALUE_STATE_PENDING
            ) and value_id not in value_ids:
                value_ids.append(value_id)
    return value_ids


async def _async_refresh_on_demand_parameters(entry: BrinkConfigEntry) -> None:
    """Subscribe briefly to on-demand values, then poll to pick them up.

    Failures are logged and never propagate: this is a best-effort refresh.
    """
    coordinator = entry.runtime_data.coordinator
    client = entry.runtime_data.client
    refreshed = False
    for system_id, device in list((coordinator.data or {}).items()):
        value_ids = _on_demand_value_ids(device)
        if not value_ids:
            _LOGGER.debug("No on-demand values to refresh for system %s", system_id)
            continue
        try:
            await client.refresh_parameter_values(system_id, value_ids)
        except Exception as ex:  # noqa: BLE001 - best effort, never fatal
            # No traceback: aiohttp errors embed the request URL, which
            # carries the SignalR connection id.
            detail = type(ex).__name__
            if isinstance(ex, BrinkRefreshError):
                detail = f"{detail}: {ex}"
            elif (status := getattr(ex, "status", None)) is not None:
                detail = f"{detail} (HTTP {status})"
            _LOGGER.warning(
                "Could not refresh on-demand values (filter days) for system %s: %s",
                system_id,
                detail,
            )
            continue
        refreshed = True
        _LOGGER.debug(
            "Requested fresh values for %d parameter(s) on system %s",
            len(value_ids),
            system_id,
        )
    if refreshed:
        await coordinator.async_request_refresh()


async def _async_update_listener(
    hass: HomeAssistant, entry: BrinkConfigEntry
) -> None:
    """Handle options update — apply new scan interval without reloading."""
    try:
        coordinator = entry.runtime_data.coordinator
    except AttributeError:
        _LOGGER.debug("Coordinator not available, skipping options update")
        return
    new_interval = int(entry.options.get(
        CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL
    ))
    await coordinator.async_update_scan_interval(new_interval)
    await coordinator.automation_controller.async_options_updated()


async def async_unload_entry(
    hass: HomeAssistant, entry: BrinkConfigEntry
) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator = entry.runtime_data.coordinator
        await coordinator.automation_controller.async_cleanup()
        coordinator.cancel_expedited_polling()
        await entry.runtime_data.client.close()

    return unload_ok


async def async_remove_entry(hass: HomeAssistant, entry: BrinkConfigEntry) -> None:
    """Delete persisted boost state when the config entry is removed."""
    try:
        await boost_store(hass, entry.entry_id).async_remove()
    except Exception:  # noqa: BLE001
        _LOGGER.debug("Could not remove persisted boost state", exc_info=True)


async def async_remove_config_entry_device(
    hass: HomeAssistant,
    entry: BrinkConfigEntry,
    device_entry: DeviceEntry,
) -> bool:
    """Allow removal of devices no longer returned by the API."""
    coordinator = entry.runtime_data.coordinator
    current_system_ids: set[str] = {
        str(sys_id) for sys_id in (coordinator.data or {})
    }
    device_identifiers: set[str] = {
        identifier[1]
        for identifier in device_entry.identifiers
        if identifier[0] == DOMAIN
    }
    # Only allow removal if the device is NOT in the current data
    return not device_identifiers.intersection(current_system_ids)
