"""Tests for the Brink HRV automation controller."""
from __future__ import annotations

import logging
import math
import time
from datetime import timedelta
from collections.abc import AsyncGenerator
from typing import Any
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from custom_components.brink_ventilation.automation_controller import (
    AutomationState,
    BrinkAutomationController,
)
from custom_components.brink_ventilation.const import (
    BOOST_TRIGGER_HUMIDITY,
    CONF_ADAPTIVE_ACTIVE,
    CONF_ADAPTIVE_ACTIVE_LEGACY,
    CONF_AUTO_SUMMER_BASE_LEVEL,
    CONF_AUTO_WINTER_BASE_LEVEL,
    CONF_EXTRA_VENT_DURATION,
    CONF_EXTRA_VENT_SUMMER_LEVEL,
    CONF_EXTRA_VENT_WINTER_LEVEL,
    CONF_FREEZING_THRESHOLD,
    CONF_HUMIDITY_SENSOR_1,
    CONF_HUMIDITY_SENSOR_2,
    CONF_HUMIDITY_SENSOR_3,
    CONF_HUMIDITY_SPIKE_THRESHOLD,
    CONF_TEMPERATURE_SOURCE_ENTITY,
    DEFAULT_AUTO_SUMMER_BASE_LEVEL,
    DEFAULT_AUTO_WINTER_BASE_LEVEL,
    DEFAULT_EXTRA_VENT_DURATION,
    DEFAULT_EXTRA_VENT_SUMMER_LEVEL,
    DEFAULT_EXTRA_VENT_WINTER_LEVEL,
    DEFAULT_FREEZING_THRESHOLD,
    DEFAULT_HUMIDITY_SPIKE_THRESHOLD,
    DOMAIN,
    EVENT_BOOST_ACTIVATED,
    EVENT_BOOST_DEACTIVATED,
    EVENT_WRITE_FAILED,
    PARAM_FRESH_AIR_TEMP,
    PARAM_OPERATING_MODE,
    PARAM_VENTILATION_LEVEL,
    SEASON_SUMMER,
    SEASON_WINTER,
)
from custom_components.brink_ventilation.core.brink_home_cloud import BrinkAuthError
from tests.conftest import SYSTEM_ID, build_coordinator_data

from pytest_homeassistant_custom_component.common import MockConfigEntry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_coordinator_data() -> dict[int, dict[str, Any]]:
    """Return sample coordinator data."""
    return build_coordinator_data()


@pytest.fixture
def mock_coordinator(mock_coordinator_data: dict[int, dict[str, Any]]) -> MagicMock:
    """Create a mock coordinator with standard data and client."""
    coordinator = MagicMock()
    coordinator.data = mock_coordinator_data
    coordinator.client = AsyncMock()
    coordinator.client.write_parameters = AsyncMock()
    coordinator.start_expedited_polling = MagicMock()
    coordinator.async_set_updated_data = MagicMock()
    coordinator.async_update_listeners = MagicMock()
    return coordinator


@pytest.fixture
def mock_config_entry() -> MockConfigEntry:
    """Create a mock config entry with default options."""
    return MockConfigEntry(
        domain=DOMAIN,
        title="Test Brink System",
        data={
            "email": "test@example.com",
            "password": "testpassword123",
        },
        options={
            "scan_interval": 60,
            CONF_FREEZING_THRESHOLD: DEFAULT_FREEZING_THRESHOLD,
            CONF_EXTRA_VENT_DURATION: DEFAULT_EXTRA_VENT_DURATION,
            CONF_EXTRA_VENT_SUMMER_LEVEL: DEFAULT_EXTRA_VENT_SUMMER_LEVEL,
            CONF_EXTRA_VENT_WINTER_LEVEL: DEFAULT_EXTRA_VENT_WINTER_LEVEL,
            CONF_AUTO_SUMMER_BASE_LEVEL: DEFAULT_AUTO_SUMMER_BASE_LEVEL,
            CONF_AUTO_WINTER_BASE_LEVEL: DEFAULT_AUTO_WINTER_BASE_LEVEL,
            CONF_HUMIDITY_SPIKE_THRESHOLD: DEFAULT_HUMIDITY_SPIKE_THRESHOLD,
            CONF_ADAPTIVE_ACTIVE: False,
        },
        unique_id="test@example.com",
    )


@pytest.fixture
async def controller(
    hass: HomeAssistant,
    mock_config_entry: MockConfigEntry,
    mock_coordinator: MagicMock,
) -> AsyncGenerator[BrinkAutomationController]:
    """Create an automation controller instance wired to hass, entry, and coordinator."""
    mock_config_entry.add_to_hass(hass)
    ctrl = BrinkAutomationController(hass, mock_coordinator, mock_config_entry)
    yield ctrl
    await ctrl.async_cleanup()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _set_coordinator_param(
    coordinator: MagicMock,
    param_key: str,
    value: str,
) -> None:
    """Set a parameter value inside the mock coordinator data."""
    for device in coordinator.data.values():
        for component in device.get("components", []):
            param = component.get("parameters", {}).get(param_key)
            if param is not None:
                param["value"] = value
                return


def _manual_humidity_tick(controller: BrinkAutomationController) -> None:
    """Call _process_humidity_tick directly for testing.

    Cancels the pending timer first to avoid orphaned timers that trigger
    the HA 'lingering timer' teardown check, then calls the tick which
    reschedules a fresh timer.
    """
    controller._cancel_humidity_timer()
    controller._process_humidity_tick(None)


# =====================================================================
# State machine tests
# =====================================================================


class TestStateMachine:
    """Tests for IDLE / BASE / BOOSTED state transitions."""

    async def test_activate_idle_to_base(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """IDLE -> activate -> BASE, writes seasonal level."""
        assert controller.state == AutomationState.IDLE
        await controller.async_activate()
        assert controller.state == AutomationState.BASE
        # write_parameters should be called with mode=1 and a level value
        mock_coordinator.client.write_parameters.assert_awaited_once()

    async def test_activate_when_not_idle(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """BASE -> activate -> no-op (debug log, no extra write)."""
        await controller.async_activate()
        assert controller.state == AutomationState.BASE
        call_count = mock_coordinator.client.write_parameters.await_count

        # Call activate again while in BASE
        await controller.async_activate()
        assert controller.state == AutomationState.BASE
        # No additional write should have been made
        assert mock_coordinator.client.write_parameters.await_count == call_count

    async def test_deactivate_base_to_idle(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """BASE -> deactivate -> IDLE, clears pending writes."""
        await controller.async_activate()
        assert controller.state == AutomationState.BASE
        # Simulate a pending write
        controller._pending_writes = [(1001, "2")]

        await controller.async_deactivate()
        assert controller.state == AutomationState.IDLE
        assert controller._pending_writes is None

    async def test_deactivate_boosted_to_idle(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """BOOSTED -> deactivate -> IDLE, cancels boost timer."""
        await controller.async_activate()
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED

        await controller.async_deactivate()
        assert controller.state == AutomationState.IDLE
        assert controller._boost_timer_unsub is None
        assert controller._countdown_timer_unsub is None

    async def test_deactivate_when_idle(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """IDLE -> deactivate -> stays IDLE, just updates options."""
        assert controller.state == AutomationState.IDLE
        await controller.async_deactivate()
        assert controller.state == AutomationState.IDLE

    async def test_state_persisted_on_activate(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """activate -> entry.options has adaptive_active=True."""
        await controller.async_activate()
        assert mock_config_entry.options[CONF_ADAPTIVE_ACTIVE] is True

    async def test_state_persisted_on_deactivate(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """deactivate -> entry.options has adaptive_active=False."""
        # First activate to set the flag to True
        await controller.async_activate()
        assert mock_config_entry.options[CONF_ADAPTIVE_ACTIVE] is True

        await controller.async_deactivate()
        assert mock_config_entry.options[CONF_ADAPTIVE_ACTIVE] is False

    async def test_restore_state_active(
        self,
        hass: HomeAssistant,
        mock_coordinator: MagicMock,
    ) -> None:
        """options has adaptive_active=True -> restores to BASE."""
        entry = MockConfigEntry(
            domain=DOMAIN,
            title="Test",
            data={"email": "t@t.com", "password": "p"},
            options={
                CONF_ADAPTIVE_ACTIVE: True,
                CONF_FREEZING_THRESHOLD: DEFAULT_FREEZING_THRESHOLD,
                CONF_EXTRA_VENT_DURATION: DEFAULT_EXTRA_VENT_DURATION,
                CONF_EXTRA_VENT_SUMMER_LEVEL: DEFAULT_EXTRA_VENT_SUMMER_LEVEL,
                CONF_EXTRA_VENT_WINTER_LEVEL: DEFAULT_EXTRA_VENT_WINTER_LEVEL,
                CONF_AUTO_SUMMER_BASE_LEVEL: DEFAULT_AUTO_SUMMER_BASE_LEVEL,
                CONF_AUTO_WINTER_BASE_LEVEL: DEFAULT_AUTO_WINTER_BASE_LEVEL,
                CONF_HUMIDITY_SPIKE_THRESHOLD: DEFAULT_HUMIDITY_SPIKE_THRESHOLD,
            },
            unique_id="restore@test.com",
        )
        entry.add_to_hass(hass)
        ctrl = BrinkAutomationController(hass, mock_coordinator, entry)
        await ctrl.async_restore_state()
        assert ctrl.state == AutomationState.BASE

    async def test_restore_state_inactive(
        self,
        hass: HomeAssistant,
        mock_coordinator: MagicMock,
    ) -> None:
        """options has adaptive_active=False -> stays IDLE."""
        entry = MockConfigEntry(
            domain=DOMAIN,
            title="Test",
            data={"email": "t@t.com", "password": "p"},
            options={CONF_ADAPTIVE_ACTIVE: False},
            unique_id="norestore@test.com",
        )
        entry.add_to_hass(hass)
        ctrl = BrinkAutomationController(hass, mock_coordinator, entry)
        await ctrl.async_restore_state()
        assert ctrl.state == AutomationState.IDLE

    async def test_restore_state_legacy_key(
        self,
        hass: HomeAssistant,
        mock_coordinator: MagicMock,
    ) -> None:
        """options has ha_automated_active=True -> migrates and activates."""
        entry = MockConfigEntry(
            domain=DOMAIN,
            title="Test",
            data={"email": "t@t.com", "password": "p"},
            options={
                CONF_ADAPTIVE_ACTIVE_LEGACY: True,
                CONF_FREEZING_THRESHOLD: DEFAULT_FREEZING_THRESHOLD,
                CONF_EXTRA_VENT_DURATION: DEFAULT_EXTRA_VENT_DURATION,
                CONF_EXTRA_VENT_SUMMER_LEVEL: DEFAULT_EXTRA_VENT_SUMMER_LEVEL,
                CONF_EXTRA_VENT_WINTER_LEVEL: DEFAULT_EXTRA_VENT_WINTER_LEVEL,
                CONF_AUTO_SUMMER_BASE_LEVEL: DEFAULT_AUTO_SUMMER_BASE_LEVEL,
                CONF_AUTO_WINTER_BASE_LEVEL: DEFAULT_AUTO_WINTER_BASE_LEVEL,
                CONF_HUMIDITY_SPIKE_THRESHOLD: DEFAULT_HUMIDITY_SPIKE_THRESHOLD,
            },
            unique_id="legacy@test.com",
        )
        entry.add_to_hass(hass)
        ctrl = BrinkAutomationController(hass, mock_coordinator, entry)
        await ctrl.async_restore_state()
        assert ctrl.state == AutomationState.BASE
        # Legacy key should have been migrated
        assert entry.options.get(CONF_ADAPTIVE_ACTIVE) is True
        assert CONF_ADAPTIVE_ACTIVE_LEGACY not in entry.options


# =====================================================================
# Extra ventilation / boost tests
# =====================================================================


class TestExtraVentilation:
    """Tests for extra ventilation boost activation and cancellation."""

    async def test_activate_extra_vent_from_base(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """BASE -> BOOSTED, starts boost timer."""
        await controller.async_activate()
        assert controller.state == AutomationState.BASE
        mock_coordinator.client.write_parameters.reset_mock()

        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller._was_in_base_before_boost is True
        assert controller._boost_timer_unsub is not None
        mock_coordinator.client.write_parameters.assert_awaited_once()

    async def test_activate_extra_vent_from_idle(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """IDLE -> BOOSTED (direct, _was_in_base_before_boost=False)."""
        assert controller.state == AutomationState.IDLE
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller._was_in_base_before_boost is False

    async def test_activate_extra_vent_manual_trigger(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        """No trigger info -> trigger properties return None."""
        await controller.async_activate()
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller.boost_trigger is None
        assert controller.boost_trigger_entity is None
        assert controller.boost_trigger_rate is None

    async def test_activate_extra_vent_humidity_trigger(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        """With trigger info -> boost_trigger, boost_trigger_entity, boost_trigger_rate set."""
        await controller.async_activate()
        await controller.async_activate_extra_ventilation(
            trigger=BOOST_TRIGGER_HUMIDITY,
            trigger_entity="sensor.bathroom_humidity",
            trigger_rate=2.5,
        )
        assert controller.state == AutomationState.BOOSTED
        assert controller.boost_trigger == BOOST_TRIGGER_HUMIDITY
        assert controller.boost_trigger_entity == "sensor.bathroom_humidity"
        assert controller.boost_trigger_rate == 2.5

    async def test_cancel_extra_vent_returns_to_base(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """BOOSTED (was BASE) -> cancel -> BASE."""
        await controller.async_activate()
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller._was_in_base_before_boost is True

        await controller.async_cancel_extra_ventilation()
        assert controller.state == AutomationState.BASE

    async def test_cancel_extra_vent_returns_to_idle(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """BOOSTED (was IDLE) -> cancel -> IDLE."""
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller._was_in_base_before_boost is False

        await controller.async_cancel_extra_ventilation()
        assert controller.state == AutomationState.IDLE

    async def test_activate_adaptive_during_idle_origin_boost(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Selecting adaptive during an IDLE-origin boost adopts adaptive mode.

        The boost keeps running (no extra write), the snapshot is dropped and
        the boost will return to BASE when it ends.
        """
        await controller.async_activate_extra_ventilation()
        assert controller.adaptive_active is False
        mock_coordinator.client.write_parameters.reset_mock()

        await controller.async_activate()

        assert controller.state == AutomationState.BOOSTED
        assert controller.adaptive_active is True
        assert controller._was_in_base_before_boost is True
        assert controller._pre_boost_snapshot is None
        mock_coordinator.client.write_parameters.assert_not_awaited()

        await controller.async_cancel_extra_ventilation()
        assert controller.state == AutomationState.BASE

    async def test_cancel_extra_vent_when_not_boosted(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Not BOOSTED -> cancel -> no-op."""
        await controller.async_activate()
        assert controller.state == AutomationState.BASE
        call_count = mock_coordinator.client.write_parameters.await_count

        await controller.async_cancel_extra_ventilation()
        assert controller.state == AutomationState.BASE
        # No additional write should have occurred
        assert mock_coordinator.client.write_parameters.await_count == call_count

    async def test_boost_remaining_minutes(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        """Returns correct countdown when boosted."""
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        # Default duration is 120 min; remaining should be approximately 120
        remaining = controller.boost_remaining_minutes
        assert remaining > 0
        assert remaining <= DEFAULT_EXTRA_VENT_DURATION

    async def test_boost_remaining_minutes_not_boosted(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        """Returns 0 when not BOOSTED."""
        assert controller.state == AutomationState.IDLE
        assert controller.boost_remaining_minutes == 0

        await controller.async_activate()
        assert controller.state == AutomationState.BASE
        assert controller.boost_remaining_minutes == 0


# =====================================================================
# Humidity monitoring tests
# =====================================================================


class TestHumidityMonitoring:
    """Tests for humidity delta tracking and spike detection."""

    async def test_humidity_tick_normal_rate(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Rate below threshold -> no boost."""
        # Configure a humidity sensor
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()

        hass.states.async_set("sensor.humidity_1", "60.0")

        # First tick: stores baseline
        _manual_humidity_tick(controller)
        assert controller.state == AutomationState.BASE

        # Advance time slightly and set a small increase
        with patch("custom_components.brink_ventilation.automation_controller.time") as mock_time:
            mock_time.monotonic.return_value = time.monotonic() + 60
            hass.states.async_set("sensor.humidity_1", "60.5")
            _manual_humidity_tick(controller)

        # Rate ~0.5 %/min, below default threshold of 1.5 -> still BASE
        assert controller.state == AutomationState.BASE

    async def test_humidity_tick_spike_triggers_boost(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Rate >= threshold in BASE -> triggers extra vent."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()
        assert controller.state == AutomationState.BASE

        # Seed first reading
        t0 = time.monotonic()
        hass.states.async_set("sensor.humidity_1", "50.0")
        with patch("custom_components.brink_ventilation.automation_controller.time") as mock_time:
            mock_time.monotonic.return_value = t0
            _manual_humidity_tick(controller)

        # Second tick: big spike (50 -> 55 in 1 min = 5.0 %/min > threshold 1.5)
        hass.states.async_set("sensor.humidity_1", "55.0")

        # Patch async_create_task to capture the boost call
        created_tasks = []
        original_create_task = hass.async_create_task

        def capture_task(coro, name=None, eager_start=False):
            task = original_create_task(coro, name=name, eager_start=eager_start)
            created_tasks.append(task)
            return task

        with (
            patch("custom_components.brink_ventilation.automation_controller.time") as mock_time,
            patch.object(hass, "async_create_task", side_effect=capture_task),
        ):
            mock_time.monotonic.return_value = t0 + 60
            _manual_humidity_tick(controller)

        # Let the created task run
        for task in created_tasks:
            try:
                await task
            except Exception:
                pass

        assert controller.state == AutomationState.BOOSTED

    async def test_humidity_tick_spike_not_in_base(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Rate >= threshold but not in BASE -> no boost."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        # Controller stays IDLE
        assert controller.state == AutomationState.IDLE

        t0 = time.monotonic()
        hass.states.async_set("sensor.humidity_1", "50.0")
        with patch("custom_components.brink_ventilation.automation_controller.time") as mock_time:
            mock_time.monotonic.return_value = t0
            _manual_humidity_tick(controller)

        hass.states.async_set("sensor.humidity_1", "55.0")
        with patch("custom_components.brink_ventilation.automation_controller.time") as mock_time:
            mock_time.monotonic.return_value = t0 + 60
            _manual_humidity_tick(controller)

        # Should remain IDLE despite spike
        assert controller.state == AutomationState.IDLE

    async def test_humidity_tick_unavailable_sensor(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Sensor unavailable -> skipped."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()

        hass.states.async_set("sensor.humidity_1", "unavailable")
        _manual_humidity_tick(controller)

        # No rates should be recorded
        assert "sensor.humidity_1" not in controller._humidity_rates
        assert controller.state == AutomationState.BASE

    async def test_humidity_tick_non_numeric(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Non-float state -> skipped."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()

        hass.states.async_set("sensor.humidity_1", "not_a_number")
        _manual_humidity_tick(controller)

        assert "sensor.humidity_1" not in controller._humidity_rates

    async def test_humidity_tick_out_of_range(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Value > 100 or < 0 -> skipped."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()

        # Value above 100
        hass.states.async_set("sensor.humidity_1", "105.0")
        _manual_humidity_tick(controller)
        assert "sensor.humidity_1" not in controller._humidity_rates

        # Value below 0
        hass.states.async_set("sensor.humidity_1", "-5.0")
        _manual_humidity_tick(controller)
        assert "sensor.humidity_1" not in controller._humidity_rates

    async def test_humidity_tick_rate_clamping(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Extreme rate -> clamped to [-50, 50]."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()

        t0 = time.monotonic()
        hass.states.async_set("sensor.humidity_1", "10.0")
        with patch("custom_components.brink_ventilation.automation_controller.time") as mock_time:
            mock_time.monotonic.return_value = t0
            _manual_humidity_tick(controller)

        # Huge jump: 10 -> 99 in just 1 second (=5340 %/min raw, clamped to 50)
        hass.states.async_set("sensor.humidity_1", "99.0")
        with patch("custom_components.brink_ventilation.automation_controller.time") as mock_time:
            mock_time.monotonic.return_value = t0 + 1  # 1 second elapsed
            _manual_humidity_tick(controller)

        rate = controller._humidity_rates.get("sensor.humidity_1", 0.0)
        assert rate <= 50.0

    async def test_humidity_delta_tracking(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """humidity_deltas returns per-sensor rates."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={
                **mock_config_entry.options,
                CONF_HUMIDITY_SENSOR_1: "sensor.hum1",
                CONF_HUMIDITY_SENSOR_2: "sensor.hum2",
            },
        )
        await controller.async_activate()

        t0 = time.monotonic()
        hass.states.async_set("sensor.hum1", "50.0")
        hass.states.async_set("sensor.hum2", "60.0")
        with patch("custom_components.brink_ventilation.automation_controller.time") as mock_time:
            mock_time.monotonic.return_value = t0
            _manual_humidity_tick(controller)

        hass.states.async_set("sensor.hum1", "51.0")
        hass.states.async_set("sensor.hum2", "62.0")
        with patch("custom_components.brink_ventilation.automation_controller.time") as mock_time:
            mock_time.monotonic.return_value = t0 + 60
            _manual_humidity_tick(controller)

        deltas = controller.humidity_deltas
        assert "sensor.hum1" in deltas
        assert "sensor.hum2" in deltas
        # hum1: (51-50)/1min = 1.0, hum2: (62-60)/1min = 2.0
        assert deltas["sensor.hum1"] == pytest.approx(1.0, abs=0.2)
        assert deltas["sensor.hum2"] == pytest.approx(2.0, abs=0.2)

    async def test_humidity_timer_restart_on_options_update(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Options changed -> timer restarted and old data cleared."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.hum1"},
        )
        await controller.async_activate()

        # Seed some humidity data
        hass.states.async_set("sensor.hum1", "50.0")
        _manual_humidity_tick(controller)
        assert "sensor.hum1" in controller._humidity_previous

        # Update options with a changed sensor list (simulates config flow change)
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_2: "sensor.hum2"},
        )
        await controller.async_options_updated()

        # Previous data should be cleared
        assert len(controller._humidity_previous) == 0
        assert len(controller._humidity_rates) == 0

    async def test_humidity_tick_nan_value(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """NaN humidity value -> skipped (no crash, no rate recorded)."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()

        hass.states.async_set("sensor.humidity_1", "nan")
        _manual_humidity_tick(controller)
        assert "sensor.humidity_1" not in controller._humidity_rates

    async def test_humidity_tick_inf_value(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Inf humidity value -> skipped (no crash, no rate recorded)."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()

        hass.states.async_set("sensor.humidity_1", "inf")
        _manual_humidity_tick(controller)
        assert "sensor.humidity_1" not in controller._humidity_rates

    async def test_humidity_tick_empty_state(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Empty string state -> skipped."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.humidity_1"},
        )
        await controller.async_activate()

        hass.states.async_set("sensor.humidity_1", "")
        _manual_humidity_tick(controller)
        assert "sensor.humidity_1" not in controller._humidity_rates

    async def test_humidity_tick_nonexistent_sensor(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Sensor not in hass.states (never existed) -> skipped."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.nonexistent"},
        )
        await controller.async_activate()

        _manual_humidity_tick(controller)
        assert "sensor.nonexistent" not in controller._humidity_rates

    async def test_humidity_no_sensors_configured(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        """No humidity sensors configured -> start_humidity_timer is a no-op."""
        # Default mock_config_entry has no humidity sensors
        assert controller.configured_humidity_sensors == []
        await controller.async_start_humidity_monitoring()
        # Timer should not be set (no sensors to monitor)
        assert controller._humidity_timer_unsub is None

    async def test_max_humidity_delta_empty(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        """max_humidity_delta returns 0.0 when no sensors are tracked."""
        assert controller.max_humidity_delta == 0.0

    async def test_configured_humidity_sensors_property(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """configured_humidity_sensors returns only non-empty entity_ids."""
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={
                **mock_config_entry.options,
                CONF_HUMIDITY_SENSOR_1: "sensor.hum1",
                CONF_HUMIDITY_SENSOR_2: "",
                CONF_HUMIDITY_SENSOR_3: "sensor.hum3",
            },
        )
        sensors = controller.configured_humidity_sensors
        assert sensors == ["sensor.hum1", "sensor.hum3"]


# =====================================================================
# Season evaluation tests
# =====================================================================


class TestSeasonEvaluation:
    """Tests for season detection from temperature data."""

    async def test_season_summer(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Temp >= threshold -> SEASON_SUMMER."""
        # Default threshold is -2.0, API reports 15.0 -> summer
        await controller.async_activate()
        assert controller.season == SEASON_SUMMER

    async def test_season_winter(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Temp < threshold -> SEASON_WINTER."""
        _set_coordinator_param(mock_coordinator, PARAM_FRESH_AIR_TEMP, "-5.0")
        await controller.async_activate()
        assert controller.season == SEASON_WINTER

    async def test_season_from_config_entity(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """External temp entity used for season detection."""
        hass.states.async_set("sensor.outdoor_temp", "20.0")
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={
                **mock_config_entry.options,
                CONF_TEMPERATURE_SOURCE_ENTITY: "sensor.outdoor_temp",
            },
        )
        await controller.async_activate()
        assert controller.season == SEASON_SUMMER

    async def test_season_from_api_param(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Falls back to PARAM_FRESH_AIR_TEMP when no temp entity configured."""
        # No temp entity configured (default); API has 15.0 -> summer
        assert mock_config_entry.options.get(CONF_TEMPERATURE_SOURCE_ENTITY, "") == ""
        await controller.async_activate()
        assert controller.season == SEASON_SUMMER

    async def test_season_unavailable_temp(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """No temp available -> season unchanged."""
        hass.states.async_set("sensor.outdoor_temp", "unavailable")
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={
                **mock_config_entry.options,
                CONF_TEMPERATURE_SOURCE_ENTITY: "sensor.outdoor_temp",
            },
        )
        # Remove API temp to simulate total unavailability
        for device in mock_coordinator.data.values():
            for component in device.get("components", []):
                component["parameters"].pop(PARAM_FRESH_AIR_TEMP, None)

        # Set an initial season manually so we can verify it does not change
        controller._season = SEASON_WINTER
        await controller.async_activate()
        assert controller.season == SEASON_WINTER  # unchanged

    async def test_season_change_on_coordinator_update(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Season changes -> level re-applied."""
        await controller.async_activate()
        assert controller.season == SEASON_SUMMER
        mock_coordinator.client.write_parameters.reset_mock()

        # Change API temp to winter and update the ventilation level to the
        # expected winter base level so the watchdog does not also fire.
        _set_coordinator_param(mock_coordinator, PARAM_FRESH_AIR_TEMP, "-10.0")
        _set_coordinator_param(
            mock_coordinator,
            PARAM_VENTILATION_LEVEL,
            str(DEFAULT_AUTO_WINTER_BASE_LEVEL),
        )
        await controller.async_on_coordinator_update()

        assert controller.season == SEASON_WINTER
        # Should have re-applied level for the new season
        mock_coordinator.client.write_parameters.assert_awaited_once()


# =====================================================================
# Level watchdog tests
# =====================================================================


class TestLevelWatchdog:
    """Tests for the level watchdog that corrects drift."""

    async def test_watchdog_level_matches(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Actual level matches expected -> no correction."""
        await controller.async_activate()
        # After activation, expected is summer base level (default=2), mode=1
        # Coordinator data already has ventilation_level=2, operating_mode=1
        mock_coordinator.client.write_parameters.reset_mock()

        await controller.async_on_coordinator_update()
        # No correction write should happen
        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_watchdog_level_mismatch_corrects(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Actual level != expected -> re-applies correct level."""
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()

        # Simulate someone changing the level externally
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")

        await controller.async_on_coordinator_update()
        # Watchdog should correct the level
        mock_coordinator.client.write_parameters.assert_awaited_once()

    async def test_watchdog_mode_mismatch_corrects(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Operating mode != '1' (manual) -> re-applies."""
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()

        # Simulate mode changed externally to automatic (0)
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "0")

        await controller.async_on_coordinator_update()
        mock_coordinator.client.write_parameters.assert_awaited_once()

    async def test_watchdog_skips_when_idle(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """IDLE state -> watchdog does not run."""
        assert controller.state == AutomationState.IDLE

        # Set a level mismatch
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")

        await controller.async_on_coordinator_update()
        # No write should have happened since we are IDLE
        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_watchdog_skips_when_pending_writes(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Pending writes exist -> watchdog skips (tested via _verify_level directly)."""
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()

        # Simulate pending writes
        controller._pending_writes = [(1001, "2")]

        # Change level to trigger mismatch
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")

        # Call _verify_level directly to isolate it from the retry logic
        await controller._verify_level()

        # No correction should have been made because pending_writes is set
        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_watchdog_cooldown(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Correction made -> skip next correction within 5 minutes."""
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()

        # First mismatch -> triggers correction
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        await controller.async_on_coordinator_update()
        mock_coordinator.client.write_parameters.assert_awaited_once()
        mock_coordinator.client.write_parameters.reset_mock()

        # Second mismatch within cooldown -> should be skipped
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        await controller.async_on_coordinator_update()
        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_watchdog_cooldown_expired(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """5+ minutes since last correction -> allows new correction."""
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()

        # First correction
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        await controller.async_on_coordinator_update()
        mock_coordinator.client.write_parameters.assert_awaited_once()
        mock_coordinator.client.write_parameters.reset_mock()

        # Simulate cooldown expired (set last correction to >5 min ago)
        controller._last_level_correction = time.monotonic() - 301

        # Second mismatch after cooldown -> should correct
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        await controller.async_on_coordinator_update()
        mock_coordinator.client.write_parameters.assert_awaited_once()

    async def test_watchdog_no_coordinator_data(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """No data -> watchdog skips."""
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()

        # Empty coordinator data
        mock_coordinator.data = {}

        await controller.async_on_coordinator_update()
        # No correction because there is no data to compare
        mock_coordinator.client.write_parameters.assert_not_awaited()


# =====================================================================
# Write queue tests
# =====================================================================


class TestWriteQueue:
    """Tests for the resilient write queue."""

    async def test_write_params_success(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Writes succeed -> pending cleared, expedited polling started."""
        params = [(1002, "1"), (1001, "2")]
        await controller.async_write_params(params)

        mock_coordinator.client.write_parameters.assert_awaited_once_with(SYSTEM_ID, params)
        mock_coordinator.start_expedited_polling.assert_called_once()
        assert controller._pending_writes is None

    async def test_write_params_auth_error(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Credentials BrinkAuthError -> not retried, event fired."""
        mock_coordinator.client.write_parameters.side_effect = BrinkAuthError(
            "auth fail", is_credentials_error=True
        )

        events = []
        hass.bus.async_listen(EVENT_WRITE_FAILED, lambda e: events.append(e))

        params = [(1002, "1"), (1001, "2")]
        await controller.async_write_params(params)

        # Should NOT be queued for retry
        assert controller._pending_writes is None
        mock_coordinator.start_expedited_polling.assert_not_called()

        # Event should have been fired
        await hass.async_block_till_done()
        assert len(events) == 1
        assert events[0].data["error"] == "BrinkAuthError"

    async def test_write_params_other_error(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Other exception -> queued for retry, event fired."""
        mock_coordinator.client.write_parameters.side_effect = ConnectionError("network")

        events = []
        hass.bus.async_listen(EVENT_WRITE_FAILED, lambda e: events.append(e))

        params = [(1002, "1"), (1001, "2")]
        await controller.async_write_params(params)

        # Should be queued
        assert controller._pending_writes == params
        mock_coordinator.start_expedited_polling.assert_not_called()

        await hass.async_block_till_done()
        assert len(events) == 1
        assert events[0].data["error"] == "ConnectionError"

    async def test_retry_pending_writes(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """On coordinator update -> retries pending writes."""
        await controller.async_activate()

        # Queue a failed write
        params = [(1002, "1"), (1001, "2")]
        mock_coordinator.client.write_parameters.side_effect = ConnectionError("fail")
        await controller.async_write_params(params)
        assert controller._pending_writes == params

        # Now make writes succeed
        mock_coordinator.client.write_parameters.side_effect = None
        mock_coordinator.client.write_parameters.reset_mock()

        await controller.async_on_coordinator_update()

        # Pending writes should have been retried and cleared
        mock_coordinator.client.write_parameters.assert_awaited()
        assert controller._pending_writes is None

    async def test_write_params_no_system_id(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """No system_id -> queued without calling API."""
        mock_coordinator.data = {}  # No data means no system_id

        params = [(1002, "1"), (1001, "2")]
        await controller.async_write_params(params)

        mock_coordinator.client.write_parameters.assert_not_awaited()
        assert controller._pending_writes == params


# =====================================================================
# Seasonal level tests
# =====================================================================


class TestSeasonalLevels:
    """Tests for seasonal level computation."""

    async def test_seasonal_base_level_summer(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns configured summer base level."""
        # Default API temp is 15.0 -> summer, default summer base = 2
        await controller.async_activate()
        assert controller.season == SEASON_SUMMER
        assert controller._get_seasonal_base_level() == DEFAULT_AUTO_SUMMER_BASE_LEVEL

    async def test_seasonal_base_level_winter(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns configured winter base level."""
        _set_coordinator_param(mock_coordinator, PARAM_FRESH_AIR_TEMP, "-5.0")
        await controller.async_activate()
        assert controller.season == SEASON_WINTER
        assert controller._get_seasonal_base_level() == DEFAULT_AUTO_WINTER_BASE_LEVEL

    async def test_seasonal_max_level_summer(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns configured summer max level."""
        await controller.async_activate()
        assert controller.season == SEASON_SUMMER
        assert controller._get_seasonal_max_level() == DEFAULT_EXTRA_VENT_SUMMER_LEVEL

    async def test_seasonal_max_level_winter(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns configured winter max level."""
        _set_coordinator_param(mock_coordinator, PARAM_FRESH_AIR_TEMP, "-5.0")
        await controller.async_activate()
        assert controller.season == SEASON_WINTER
        assert controller._get_seasonal_max_level() == DEFAULT_EXTRA_VENT_WINTER_LEVEL


# =====================================================================
# Cleanup test
# =====================================================================


class TestCleanup:
    """Tests for async_cleanup teardown."""

    async def test_async_cleanup(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """Cancels all timers, resets all state."""
        # Get into a complex state: BASE with humidity monitoring
        hass = controller._hass
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.hum"},
        )
        await controller.async_activate()
        assert controller.state == AutomationState.BASE

        # Add a pending write
        controller._pending_writes = [(1001, "2")]
        controller._humidity_rates["sensor.hum"] = 1.5
        controller._humidity_previous["sensor.hum"] = (time.monotonic(), 50.0)

        await controller.async_cleanup()

        assert controller.state == AutomationState.IDLE
        assert controller.season is None
        assert controller._pending_writes is None
        assert controller._boost_end_monotonic == 0.0
        assert controller._boost_trigger is None
        assert controller._boost_trigger_entity is None
        assert controller._boost_trigger_rate is None
        assert controller._last_level_correction == 0.0
        assert len(controller._humidity_previous) == 0
        assert len(controller._humidity_rates) == 0
        assert controller._boost_timer_unsub is None
        assert controller._countdown_timer_unsub is None
        assert controller._humidity_timer_unsub is None


# =====================================================================
# Boost timer expiry tests
# =====================================================================


class TestBoostTimerExpiry:
    """Tests for boost timer expiration callbacks."""

    async def test_boost_timer_returns_to_base(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Timer expired with _was_in_base -> returns to BASE."""
        await controller.async_activate()
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller._was_in_base_before_boost is True

        # Cancel the real boost timer to avoid lingering timers in teardown,
        # then simulate the callback firing as if it had expired.
        controller._cancel_boost_timer()
        controller._cancel_countdown_timer()
        controller._async_boost_timer_expired(None)
        await hass.async_block_till_done()

        assert controller.state == AutomationState.BASE

    async def test_boost_timer_returns_to_idle(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Timer expired without _was_in_base -> returns to IDLE."""
        # Activate boost directly from IDLE (not from BASE)
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller._was_in_base_before_boost is False

        # Cancel the real boost timer to avoid lingering timers in teardown,
        # then simulate the callback firing as if it had expired.
        controller._cancel_boost_timer()
        controller._cancel_countdown_timer()
        controller._async_boost_timer_expired(None)
        await hass.async_block_till_done()

        assert controller.state == AutomationState.IDLE

    async def test_boost_timer_expired_while_not_boosted(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Timer fires but state is not BOOSTED (edge case) -> no-op."""
        await controller.async_activate()
        assert controller.state == AutomationState.BASE

        # Simulate timer callback firing unexpectedly
        controller._async_boost_timer_expired(None)

        # Should remain in BASE
        assert controller.state == AutomationState.BASE


# =====================================================================
# Parameter lookup helper tests
# =====================================================================


class TestParameterLookup:
    """Tests for _find_param_value_id, _build_mode_and_level_params, etc."""

    async def test_find_param_value_id_success(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Finds the correct value_id for a known parameter."""
        value_id = controller._find_param_value_id(PARAM_VENTILATION_LEVEL)
        assert value_id == 1001

    async def test_find_param_value_id_not_found(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns None for a parameter that does not exist."""
        value_id = controller._find_param_value_id("nonexistent_param")
        assert value_id is None

    async def test_find_param_value_id_no_data(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns None when coordinator data is empty."""
        mock_coordinator.data = {}
        value_id = controller._find_param_value_id(PARAM_VENTILATION_LEVEL)
        assert value_id is None

    async def test_build_mode_and_level_params(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Builds correct param list for mode and level."""
        params = controller._build_mode_and_level_params("1", "2")
        assert len(params) == 2
        # mode_vid=1002, level_vid=1001
        assert (1002, "1") in params
        assert (1001, "2") in params

    async def test_build_mode_and_level_params_missing_ids(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns empty list when value_ids cannot be found."""
        mock_coordinator.data = {}
        params = controller._build_mode_and_level_params("1", "2")
        assert params == []

    async def test_get_system_id(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns the first system_id from coordinator data."""
        system_id = controller._get_system_id()
        assert system_id == SYSTEM_ID

    async def test_get_system_id_no_data(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Returns None when coordinator data is empty."""
        mock_coordinator.data = {}
        system_id = controller._get_system_id()
        assert system_id is None


# =====================================================================
# Options update tests
# =====================================================================


class TestOptionsUpdate:
    """Tests for async_options_updated behavior."""

    async def test_options_update_when_idle(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
    ) -> None:
        """Options update while IDLE -> restarts humidity timer only."""
        assert controller.state == AutomationState.IDLE
        mock_coordinator.client.write_parameters.reset_mock()

        await controller.async_options_updated()

        # No level write should happen when IDLE
        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_options_update_season_change(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
    ) -> None:
        """Options update in BASE with season change -> re-applies level."""
        await controller.async_activate()
        assert controller.season == SEASON_SUMMER
        mock_coordinator.client.write_parameters.reset_mock()

        # Change the freezing threshold so that 15.0C is now below it -> winter
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_FREEZING_THRESHOLD: 20.0},
        )
        await controller.async_options_updated()

        assert controller.season == SEASON_WINTER
        # Should have written winter base level
        mock_coordinator.client.write_parameters.assert_awaited_once()


# =====================================================================
# Regression tests for controller bug-fix batch
# =====================================================================


def _expire_boost(controller: BrinkAutomationController) -> None:
    """Simulate the boost timer firing (cancels the real timers first)."""
    controller._cancel_boost_timer()
    controller._cancel_countdown_timer()
    controller._async_boost_timer_expired(None)


def _written(mock_coordinator: MagicMock) -> list[list[tuple[int, str]]]:
    """Return the params lists passed to write_parameters, in order."""
    return [c.args[1] for c in mock_coordinator.client.write_parameters.await_args_list]


class TestCoordinatorNotification:
    """Controller must notify entities without resetting the poll schedule."""

    async def test_humidity_tick_keeps_refresh_schedule_and_failure_state(
        self,
        hass: HomeAssistant,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        """async_update_listeners only: last_update_success and schedule untouched."""
        coordinator = DataUpdateCoordinator(
            hass,
            logging.getLogger(__name__),
            config_entry=None,
            name="test",
            update_interval=timedelta(seconds=60),
        )
        coordinator.data = build_coordinator_data()
        coordinator.last_update_success = False
        scheduled = MagicMock()
        coordinator._unsub_refresh = scheduled

        mock_config_entry.add_to_hass(hass)
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_HUMIDITY_SENSOR_1: "sensor.hum1"},
        )
        ctrl = BrinkAutomationController(hass, coordinator, mock_config_entry)  # type: ignore[arg-type]
        hass.states.async_set("sensor.hum1", "50.0")
        try:
            ctrl._process_humidity_tick(None)
            ctrl._cancel_humidity_timer()
            ctrl._process_humidity_tick(None)

            assert coordinator.last_update_success is False
            assert coordinator._unsub_refresh is scheduled
            scheduled.assert_not_called()
        finally:
            await ctrl.async_cleanup()

    async def test_boost_paths_never_use_set_updated_data(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Boost start, expiry and cancel notify via async_update_listeners."""
        await controller.async_activate()
        await controller.async_activate_extra_ventilation()
        assert mock_coordinator.async_update_listeners.call_count == 1

        _expire_boost(controller)
        await hass.async_block_till_done()
        assert mock_coordinator.async_update_listeners.call_count == 2

        await controller.async_activate_extra_ventilation()
        await controller.async_cancel_extra_ventilation()
        mock_coordinator.async_set_updated_data.assert_not_called()
        assert mock_coordinator.async_update_listeners.call_count == 4


class TestBoostRetrigger:
    """Re-triggering a boost while BOOSTED keeps the original return path."""

    async def test_retrigger_from_base_still_returns_to_base(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate()
        await controller.async_activate_extra_ventilation()
        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller._was_in_base_before_boost is True

        mock_coordinator.client.write_parameters.reset_mock()
        _expire_boost(controller)
        await hass.async_block_till_done()

        assert controller.state == AutomationState.BASE
        # Base level was written (summer base = default 2)
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "2")]]

    async def test_retrigger_from_idle_keeps_snapshot(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "1")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        await controller.async_activate_extra_ventilation()
        # Device now reports the boost level; a second trigger must not
        # overwrite the snapshot with it.
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot == ("1", "0")
        assert controller.adaptive_active is False

        mock_coordinator.client.write_parameters.reset_mock()
        _expire_boost(controller)
        await hass.async_block_till_done()
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "0")]]


class TestBoostRestoresPreviousState:
    """A boost started from IDLE restores the previous mode/level when it ends."""

    async def test_expiry_restores_manual_mode_and_level(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "1")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot == ("1", "1")

        mock_coordinator.client.write_parameters.reset_mock()
        _expire_boost(controller)
        await hass.async_block_till_done()

        assert controller.state == AutomationState.IDLE
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "1")]]
        assert controller._pre_boost_snapshot is None

    async def test_expiry_restores_only_mode_when_not_manual(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "2")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()

        mock_coordinator.client.write_parameters.reset_mock()
        _expire_boost(controller)
        await hass.async_block_till_done()

        assert _written(mock_coordinator) == [[(1002, "2")]]

    async def test_cancel_restores_previous_state(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "1")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        await controller.async_activate_extra_ventilation()

        mock_coordinator.client.write_parameters.reset_mock()
        await controller.async_cancel_extra_ventilation()

        assert controller.state == AutomationState.IDLE
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "3")]]
        assert controller._pre_boost_snapshot is None

    async def test_no_snapshot_logs_warning_and_does_not_write(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mock_coordinator.data = {}
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot is None

        mock_coordinator.client.write_parameters.reset_mock()
        with caplog.at_level(logging.WARNING):
            _expire_boost(controller)
            await hass.async_block_till_done()

        assert controller.state == AutomationState.IDLE
        mock_coordinator.client.write_parameters.assert_not_awaited()
        assert "no pre-boost" in caplog.text

    async def test_boost_from_base_has_no_snapshot(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        await controller.async_activate()
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot is None

    async def test_deactivate_clears_snapshot_without_restoring(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot is not None

        mock_coordinator.client.write_parameters.reset_mock()
        await controller.async_deactivate()

        assert controller._pre_boost_snapshot is None
        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_cleanup_clears_snapshot(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        await controller.async_activate_extra_ventilation()
        await controller.async_cleanup()
        assert controller._pre_boost_snapshot is None

    async def test_failed_restore_is_retried_while_idle(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """Queued restore write is retried on coordinator update in IDLE."""
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()

        mock_coordinator.client.write_parameters = AsyncMock(
            side_effect=RuntimeError("offline")
        )
        _expire_boost(controller)
        await hass.async_block_till_done()
        assert controller.state == AutomationState.IDLE
        assert controller.has_pending_writes

        # The poll shows the device still at the boost level
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        mock_coordinator.client.write_parameters = AsyncMock()
        await controller.async_on_coordinator_update()

        mock_coordinator.client.write_parameters.assert_awaited_once()
        assert not controller.has_pending_writes
        assert controller.state == AutomationState.IDLE


class TestBoostPendingFlag:
    """_boost_pending must stay set for the duration of the boost write."""

    async def test_pending_cleared_only_after_write(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate()
        gate = asyncio.Event()

        async def slow_write(*_args: Any, **_kwargs: Any) -> None:
            await gate.wait()

        mock_coordinator.client.write_parameters = AsyncMock(side_effect=slow_write)
        controller._boost_pending = True  # as set by the humidity tick

        task = asyncio.ensure_future(controller.async_activate_extra_ventilation())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not task.done()
        assert controller._boost_pending is True

        gate.set()
        await task
        assert controller._boost_pending is False
        assert controller.state == AutomationState.BOOSTED

    async def test_pending_cleared_on_error(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        await controller.async_activate()
        controller._boost_pending = True
        with patch.object(
            controller,
            "_async_apply_seasonal_level",
            AsyncMock(side_effect=RuntimeError("boom")),
        ):
            with pytest.raises(RuntimeError):
                await controller.async_activate_extra_ventilation()
        assert controller._boost_pending is False


class TestHumidityTickResilience:
    """An exception in the tick must not stop humidity monitoring."""

    async def test_tick_reschedules_after_exception(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        controller._cancel_humidity_timer()
        with patch.object(
            controller, "_check_humidity_sensors", side_effect=RuntimeError("boom")
        ):
            controller._process_humidity_tick(None)

        assert controller._humidity_timer_unsub is not None
        assert "Unexpected error in humidity monitoring tick" in caplog.text


class TestWatchdogNormalization:
    """Level watchdog compares normalised values within the controller's system."""

    @pytest.mark.parametrize(
        ("mode", "level"),
        [(1, 2), ("1.0", "2.0"), (1.0, 2), (" 1 ", "2")],
    )
    async def test_equivalent_representations_do_not_trigger_write(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mode: Any,
        level: Any,
    ) -> None:
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, mode)
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, level)

        await controller._verify_level()

        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_real_mismatch_with_numeric_values_still_corrects(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, 0)

        await controller.async_on_coordinator_update()

        mock_coordinator.client.write_parameters.assert_awaited_once()

    async def test_other_system_is_ignored(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """A mismatching second system must not trigger a correction."""
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()
        other = build_coordinator_data(ventilation_level="0", operating_mode="3")
        mock_coordinator.data = {**mock_coordinator.data, 999: other[SYSTEM_ID]}

        await controller._verify_level()

        mock_coordinator.client.write_parameters.assert_not_awaited()


class TestSystemScoping:
    """Parameter lookup is restricted to the controller's own system."""

    async def test_find_param_value_id_ignores_other_systems(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        first = build_coordinator_data()[SYSTEM_ID]
        del first["components"][0]["parameters"][PARAM_OPERATING_MODE]
        second = build_coordinator_data()[SYSTEM_ID]
        mock_coordinator.data = {SYSTEM_ID: first, 999: second}

        assert controller._find_param_value_id(PARAM_OPERATING_MODE) is None
        assert controller._build_mode_and_level_params("1", "2") == []
        assert controller._find_param_value_id(PARAM_VENTILATION_LEVEL) == 1001


class TestTemperatureSource:
    """Temperature source entity: unit conversion and non-finite rejection."""

    @staticmethod
    def _use_entity(
        hass: HomeAssistant, entry: MockConfigEntry
    ) -> None:
        hass.config_entries.async_update_entry(
            entry,
            options={
                **entry.options,
                CONF_TEMPERATURE_SOURCE_ENTITY: "sensor.outdoor",
            },
        )

    async def test_fahrenheit_entity_is_converted(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        self._use_entity(hass, mock_config_entry)
        # 15 F = -9.4 C -> winter (threshold -2 C); raw 15 would read as summer
        hass.states.async_set(
            "sensor.outdoor", "15", {"unit_of_measurement": UnitOfTemperature.FAHRENHEIT}
        )
        assert controller._get_current_temperature() == pytest.approx(-9.444, abs=0.01)
        assert controller._evaluate_season() == SEASON_WINTER

    async def test_celsius_entity_unchanged(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        self._use_entity(hass, mock_config_entry)
        hass.states.async_set(
            "sensor.outdoor", "15", {"unit_of_measurement": UnitOfTemperature.CELSIUS}
        )
        assert controller._get_current_temperature() == 15.0

    @pytest.mark.parametrize("bad", ["nan", "inf", "-inf"])
    async def test_non_finite_entity_falls_back_to_api(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
        bad: str,
    ) -> None:
        self._use_entity(hass, mock_config_entry)
        _set_coordinator_param(mock_coordinator, PARAM_FRESH_AIR_TEMP, "-10.0")
        hass.states.async_set("sensor.outdoor", bad)
        assert controller._get_current_temperature() == -10.0
        assert controller._evaluate_season() == SEASON_WINTER

    @pytest.mark.parametrize("bad", ["nan", "inf"])
    async def test_non_finite_api_value_rejected(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        bad: str,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_FRESH_AIR_TEMP, bad)
        assert controller._get_current_temperature() is None
        assert controller._evaluate_season() is None


class TestOptionsUpdateHumidityReset:
    """Humidity tracking is only reset when the sensor list changes."""

    @staticmethod
    def _set_sensor(
        hass: HomeAssistant, entry: MockConfigEntry, entity_id: str
    ) -> None:
        hass.config_entries.async_update_entry(
            entry,
            options={**entry.options, CONF_HUMIDITY_SENSOR_1: entity_id},
        )

    async def test_unchanged_sensors_keep_history(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        self._set_sensor(hass, mock_config_entry, "sensor.hum1")
        await controller.async_activate()
        hass.states.async_set("sensor.hum1", "50.0")
        _manual_humidity_tick(controller)
        timer = controller._humidity_timer_unsub

        # Same sensors, e.g. the adaptive_active flag changed
        await controller.async_options_updated()

        assert "sensor.hum1" in controller._humidity_previous
        assert controller._humidity_timer_unsub is timer

    async def test_adaptive_toggle_does_not_reset_history(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        self._set_sensor(hass, mock_config_entry, "sensor.hum1")
        await controller.async_options_updated()
        hass.states.async_set("sensor.hum1", "50.0")
        _manual_humidity_tick(controller)

        await controller.async_activate()
        await controller.async_options_updated()
        await controller.async_deactivate()
        await controller.async_options_updated()

        assert "sensor.hum1" in controller._humidity_previous

    async def test_timer_restarted_when_not_running(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        self._set_sensor(hass, mock_config_entry, "sensor.hum1")
        await controller.async_options_updated()
        assert controller._humidity_timer_unsub is not None
        controller._cancel_humidity_timer()

        await controller.async_options_updated()
        assert controller._humidity_timer_unsub is not None

    async def test_removing_all_sensors_stops_timer(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
    ) -> None:
        self._set_sensor(hass, mock_config_entry, "sensor.hum1")
        await controller.async_options_updated()
        assert controller._humidity_timer_unsub is not None

        self._set_sensor(hass, mock_config_entry, "")
        await controller.async_options_updated()
        assert controller._humidity_timer_unsub is None


class TestAdaptiveActiveProperty:
    """adaptive_active reflects whether the controller returns to BASE."""

    async def test_values_per_state(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
    ) -> None:
        assert controller.adaptive_active is False  # IDLE

        await controller.async_activate()
        assert controller.adaptive_active is True  # BASE

        await controller.async_activate_extra_ventilation()
        assert controller.adaptive_active is True  # BOOSTED from BASE

        await controller.async_deactivate()
        assert controller.adaptive_active is False

        await controller.async_activate_extra_ventilation()
        assert controller.state == AutomationState.BOOSTED
        assert controller.adaptive_active is False  # BOOSTED from IDLE


# =====================================================================
# Review round 2 regression tests
# =====================================================================


def _store_key(entry: MockConfigEntry) -> str:
    return f"{DOMAIN}.{entry.entry_id}.boost"


def _persisted(hass_storage: dict[str, Any], entry: MockConfigEntry) -> dict[str, Any]:
    """Return the persisted data; ended boosts leave an empty tombstone."""
    return hass_storage.get(_store_key(entry), {}).get("data") or {}


def _seed_boost(
    hass_storage: dict[str, Any],
    entry: MockConfigEntry,
    *,
    minutes: float,
    was_in_base: bool = False,
    snapshot: list[str | None] | None = None,
    boost_level: int = 3,
    system_id: int | None = None,
) -> None:
    """Pre-populate the persisted boost with an end time `minutes` from now."""
    from homeassistant.util import dt as dt_util

    key = _store_key(entry)
    hass_storage[key] = {
        "version": 1,
        "minor_version": 1,
        "key": key,
        "data": {
            "end": (dt_util.utcnow() + timedelta(minutes=minutes)).isoformat(),
            "was_in_base_before_boost": was_in_base,
            "snapshot": snapshot,
            "boost_level": boost_level,
            "trigger": None,
            "trigger_entity": None,
            "trigger_rate": None,
            "system_id": system_id,
        },
    }


class TestStaleSnapshot:
    """A restore write is not visible in coordinator.data until the next poll."""

    async def test_cancel_then_reboost_snapshots_restore_target(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "1")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        # A poll during the boost shows the boost level
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        await controller.async_cancel_extra_ventilation()  # restore, no poll yet
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot == ("1", "1")

        mock_coordinator.client.write_parameters.reset_mock()
        await controller.async_cancel_extra_ventilation()
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "1")]]

    async def test_snapshot_reads_data_again_after_a_poll(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "1")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        await controller.async_cancel_extra_ventilation()
        # The poll arrives: the unit now really is at the restored level 1
        # ... or the user changed it to 0 meanwhile; data is authoritative.
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        await controller.async_on_coordinator_update()
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot == ("1", "0")

    async def test_queued_restore_replayed_by_poll_still_guards_snapshot(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "1")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        mock_coordinator.client.write_parameters = AsyncMock(
            side_effect=RuntimeError("offline")
        )
        _expire_boost(controller)
        await hass.async_block_till_done()
        assert controller.has_pending_writes

        # Poll shows the boost level; the retry succeeds in this pass
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        mock_coordinator.client.write_parameters = AsyncMock()
        await controller.async_on_coordinator_update()
        assert not controller.has_pending_writes

        # Data (level 3) predates the successful restore
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot == ("1", "1")


class TestQueuedRestoreVersusUser:
    """A queued IDLE restore must never override a later user choice."""

    async def _queue_failed_restore(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "1")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        mock_coordinator.client.write_parameters = AsyncMock(
            side_effect=RuntimeError("offline")
        )
        _expire_boost(controller)
        await hass.async_block_till_done()
        assert controller.has_pending_writes

    async def test_dropped_when_device_level_changed(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await self._queue_failed_restore(hass, controller, mock_coordinator)
        # User (wall panel / Brink app) picks level 0
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        mock_coordinator.client.write_parameters = AsyncMock()

        with caplog.at_level(logging.INFO):
            await controller.async_on_coordinator_update()

        mock_coordinator.client.write_parameters.assert_not_awaited()
        assert not controller.has_pending_writes
        assert "Dropping queued boost restore" in caplog.text

    async def test_dropped_when_device_mode_changed(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await self._queue_failed_restore(hass, controller, mock_coordinator)
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "2")
        mock_coordinator.client.write_parameters = AsyncMock()

        await controller.async_on_coordinator_update()

        mock_coordinator.client.write_parameters.assert_not_awaited()
        assert not controller.has_pending_writes

    async def test_replayed_while_device_still_at_boost_values(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await self._queue_failed_restore(hass, controller, mock_coordinator)
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        mock_coordinator.client.write_parameters = AsyncMock()

        await controller.async_on_coordinator_update()

        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "1")]]
        assert not controller.has_pending_writes

    async def test_user_override_drops_queued_restore(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await self._queue_failed_restore(hass, controller, mock_coordinator)
        controller.async_user_override()
        assert not controller.has_pending_writes

    async def test_user_override_keeps_controller_writes_outside_idle(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        await controller.async_activate()
        controller._pending_writes = [(1002, "1"), (1001, "2")]
        controller.async_user_override()
        assert controller.has_pending_writes

    async def test_early_return_restore_clears_queued_boost_write(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        """No snapshot: a queued boost write must not be replayed after the boost."""
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, None)  # type: ignore[arg-type]
        mock_coordinator.client.write_parameters = AsyncMock(
            side_effect=RuntimeError("offline")
        )
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot is None
        assert controller.has_pending_writes  # the boost write itself

        _expire_boost(controller)
        await hass.async_block_till_done()

        assert controller.state == AutomationState.IDLE
        assert not controller.has_pending_writes

    async def test_restore_without_value_ids_clears_pending(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate_extra_ventilation()
        controller._pending_writes = [(1002, "1"), (1001, "3")]
        mock_coordinator.data = {}
        await controller._async_restore_pre_boost(("1", "1"))
        assert not controller.has_pending_writes


class TestInFlightRaces:
    """Generation counter: a superseded boost must not start timers / state."""

    @staticmethod
    def _gated_write(mock_coordinator: MagicMock) -> tuple[asyncio.Event, asyncio.Event]:
        gate = asyncio.Event()
        started = asyncio.Event()

        async def slow(*_a: Any, **_k: Any) -> None:
            started.set()
            await gate.wait()

        mock_coordinator.client.write_parameters = AsyncMock(side_effect=slow)
        return gate, started

    async def test_adaptive_during_inflight_idle_boost_wins(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        gate, started = self._gated_write(mock_coordinator)
        task = asyncio.ensure_future(controller.async_activate_extra_ventilation())
        await started.wait()
        mock_coordinator.client.write_parameters = AsyncMock()
        await controller.async_activate()
        assert controller.state == AutomationState.BASE
        gate.set()
        await task

        assert controller.state == AutomationState.BASE
        assert controller._boost_timer_unsub is None
        assert controller._countdown_timer_unsub is None
        assert controller.adaptive_active is True
        assert controller._pre_boost_snapshot is None

    async def test_deactivate_during_inflight_boost_wins(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await controller.async_activate()
        gate, started = self._gated_write(mock_coordinator)
        task = asyncio.ensure_future(controller.async_activate_extra_ventilation())
        await started.wait()
        await controller.async_deactivate()
        with caplog.at_level(logging.INFO):
            gate.set()
            await task

        assert controller.state == AutomationState.IDLE
        assert controller._boost_timer_unsub is None
        assert controller._countdown_timer_unsub is None
        assert controller.boost_remaining_minutes == 0
        assert "boost superseded" in caplog.text

    async def test_cleanup_during_inflight_boost_leaks_no_timers(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        gate, started = self._gated_write(mock_coordinator)
        task = asyncio.ensure_future(controller.async_activate_extra_ventilation())
        await started.wait()
        await controller.async_cleanup()
        gate.set()
        await task

        assert controller.state == AutomationState.IDLE
        assert controller._boost_timer_unsub is None
        assert controller._countdown_timer_unsub is None

    async def test_cancel_generation_bump(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        await controller.async_activate_extra_ventilation()
        before = controller._generation
        await controller.async_cancel_extra_ventilation()
        assert controller._generation == before + 1


class TestStalePendingTask:
    """Restore / return-to-base tasks must not land after a newer write."""

    async def test_boost_cancels_stale_restore_task(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        stale = asyncio.ensure_future(asyncio.sleep(3600))
        controller._pending_task = stale
        await controller.async_activate_extra_ventilation()
        await asyncio.sleep(0)
        assert stale.cancelled()

    async def test_activate_cancels_stale_restore_task(
        self,
        controller: BrinkAutomationController,
    ) -> None:
        stale = asyncio.ensure_future(asyncio.sleep(3600))
        controller._pending_task = stale
        await controller.async_activate()
        await asyncio.sleep(0)
        assert stale.cancelled()

    async def test_humidity_boost_does_not_cancel_itself(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
    ) -> None:
        await controller.async_activate()
        # Not eager: _pending_task is assigned before the task body runs, so the
        # guard in _cancel_stale_pending_task is what keeps it from cancelling itself
        task = hass.async_create_task(
            controller._safe_activate_extra_ventilation(
                trigger_entity="sensor.x", trigger_rate=2.0
            ),
            eager_start=False,
        )
        controller._pending_task = task
        await task

        assert not task.cancelled()
        assert controller.state == AutomationState.BOOSTED
        assert controller.boost_trigger == BOOST_TRIGGER_HUMIDITY


class TestWatchdogDuplicateWrites:
    """The watchdog must not act on poll data that predates this pass's writes."""

    async def test_season_change_write_is_not_repeated_by_watchdog(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate()  # summer, base level 2
        mock_coordinator.client.write_parameters.reset_mock()
        _set_coordinator_param(mock_coordinator, PARAM_FRESH_AIR_TEMP, "-10.0")

        await controller.async_on_coordinator_update()

        # Exactly the season-change write (winter base); no watchdog rewrite
        assert len(_written(mock_coordinator)) == 1
        assert controller._last_level_correction == 0.0

    async def test_pending_retry_is_not_repeated_by_watchdog(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate()
        mock_coordinator.client.write_parameters.reset_mock()
        controller._pending_writes = [(1002, "1"), (1001, "2")]
        # Data (level 0) predates the retry and would look like drift
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")

        await controller.async_on_coordinator_update()

        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "2")]]
        assert controller._last_level_correction == 0.0

    async def test_watchdog_still_corrects_on_a_later_poll(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate()
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        await controller.async_on_coordinator_update()  # corrects (no prior write this pass)
        assert len(_written(mock_coordinator)) == 2


class TestStickySystem:
    """The controlled system is pinned; another system never takes over."""

    async def test_does_not_switch_when_pinned_system_missing(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        first = build_coordinator_data()[SYSTEM_ID]
        second = build_coordinator_data()[SYSTEM_ID]
        mock_coordinator.data = {SYSTEM_ID: first, 999: second}
        assert controller._get_system_id() == SYSTEM_ID

        # System A's fetch failed this poll
        mock_coordinator.data = {999: second}
        assert controller._get_system_id() is None

        await controller.async_write_params([(1002, "1"), (1001, "2")])
        mock_coordinator.client.write_parameters.assert_not_awaited()
        assert controller._pending_writes == [(1002, "1"), (1001, "2")]

        # A is back
        mock_coordinator.data = {SYSTEM_ID: first, 999: second}
        assert controller._get_system_id() == SYSTEM_ID

    async def test_cleanup_resets_pinned_system(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        assert controller._get_system_id() == SYSTEM_ID
        await controller.async_cleanup()
        mock_coordinator.data = {999: build_coordinator_data()[SYSTEM_ID]}
        assert controller._get_system_id() == 999


class TestDeactivateNotifies:
    async def test_deactivate_updates_listeners(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate_extra_ventilation()
        mock_coordinator.async_update_listeners.reset_mock()
        await controller.async_deactivate()
        mock_coordinator.async_update_listeners.assert_called()


class TestAuthErrorWrites:
    async def test_transient_auth_error_is_queued(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        mock_coordinator.client.write_parameters.side_effect = BrinkAuthError(
            "Authentication on cooldown"
        )
        events: list[Any] = []
        hass.bus.async_listen(EVENT_WRITE_FAILED, lambda e: events.append(e))

        params = [(1002, "1"), (1001, "2")]
        await controller.async_write_params(params)

        assert controller._pending_writes == params
        await hass.async_block_till_done()
        assert len(events) == 1
        assert events[0].data["error"] == "BrinkAuthError"

    async def test_credentials_auth_error_is_dropped(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        mock_coordinator.client.write_parameters.side_effect = BrinkAuthError(
            "bad password", is_credentials_error=True
        )
        events: list[Any] = []
        hass.bus.async_listen(EVENT_WRITE_FAILED, lambda e: events.append(e))

        await controller.async_write_params([(1002, "1"), (1001, "2")])

        assert controller._pending_writes is None
        await hass.async_block_till_done()
        assert len(events) == 1


class TestBoostPersistence:
    """The active boost survives restart / reload."""

    async def test_boost_is_saved_with_end_time_and_snapshot(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
        hass_storage: dict[str, Any],
    ) -> None:
        from homeassistant.util import dt as dt_util

        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation(
            trigger=BOOST_TRIGGER_HUMIDITY, trigger_entity="sensor.x", trigger_rate=2.5
        )

        data = hass_storage[_store_key(mock_config_entry)]["data"]
        end = dt_util.parse_datetime(data["end"])
        assert end is not None and end.tzinfo is not None
        remaining = (end - dt_util.utcnow()).total_seconds()
        assert DEFAULT_EXTRA_VENT_DURATION * 60 - 5 < remaining <= DEFAULT_EXTRA_VENT_DURATION * 60
        assert data["was_in_base_before_boost"] is False
        assert data["snapshot"] == ["1", "1"]
        assert data["trigger"] == BOOST_TRIGGER_HUMIDITY
        assert data["trigger_entity"] == "sensor.x"
        assert data["trigger_rate"] == 2.5

    async def test_cancel_clears_store(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await controller.async_activate_extra_ventilation()
        assert _store_key(mock_config_entry) in hass_storage
        await controller.async_cancel_extra_ventilation()
        assert _persisted(hass_storage, mock_config_entry) == {}

    async def test_deactivate_clears_store(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await controller.async_activate_extra_ventilation()
        await controller.async_deactivate()
        assert _persisted(hass_storage, mock_config_entry) == {}

    async def test_timer_expiry_clears_store(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await controller.async_activate_extra_ventilation()
        _expire_boost(controller)
        await hass.async_block_till_done()
        assert _persisted(hass_storage, mock_config_entry) == {}

    async def test_cleanup_keeps_store(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await controller.async_activate_extra_ventilation()
        await controller.async_cleanup()
        assert _store_key(mock_config_entry) in hass_storage

    async def test_adaptive_enabled_during_boost_updates_store(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await controller.async_activate_extra_ventilation()
        assert hass_storage[_store_key(mock_config_entry)]["data"]["snapshot"] is not None
        await controller.async_activate()
        data = hass_storage[_store_key(mock_config_entry)]["data"]
        assert data["was_in_base_before_boost"] is True
        assert data["snapshot"] is None

    async def test_remove_persisted_state(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await controller.async_activate_extra_ventilation()
        await controller.async_cleanup()
        await controller.async_remove_persisted_state()
        assert _store_key(mock_config_entry) not in hass_storage

    async def test_restore_resumes_running_boost_without_write(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
        hass_storage: dict[str, Any],
    ) -> None:
        _seed_boost(hass_storage, mock_config_entry, minutes=30, snapshot=["1", "1"])
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        events: list[Any] = []
        hass.bus.async_listen(EVENT_BOOST_ACTIVATED, lambda e: events.append(e))

        await controller.async_restore_state()
        await hass.async_block_till_done()

        assert controller.state == AutomationState.BOOSTED
        assert controller.adaptive_active is False
        assert controller._pre_boost_snapshot == ("1", "1")
        assert 29 <= controller.boost_remaining_minutes <= 30
        assert controller._boost_timer_unsub is not None
        assert controller._countdown_timer_unsub is not None
        mock_coordinator.client.write_parameters.assert_not_awaited()
        assert events == []

        # ... and it ends normally: the snapshot is restored
        _expire_boost(controller)
        await hass.async_block_till_done()
        assert controller.state == AutomationState.IDLE
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "1")]]

    async def test_restore_resumes_boost_from_adaptive_without_base_write(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
        hass_storage: dict[str, Any],
    ) -> None:
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_ADAPTIVE_ACTIVE: True},
        )
        _seed_boost(hass_storage, mock_config_entry, minutes=30, was_in_base=True)

        await controller.async_restore_state()

        assert controller.state == AutomationState.BOOSTED
        assert controller.adaptive_active is True
        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_restore_expired_boost_restores_snapshot(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
        hass_storage: dict[str, Any],
    ) -> None:
        _seed_boost(hass_storage, mock_config_entry, minutes=-5, snapshot=["1", "0"])
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")

        await controller.async_restore_state()

        assert controller.state == AutomationState.IDLE
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "0")]]
        assert _persisted(hass_storage, mock_config_entry) == {}

    async def test_restore_expired_boost_returns_to_base_when_adaptive(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
        hass_storage: dict[str, Any],
    ) -> None:
        hass.config_entries.async_update_entry(
            mock_config_entry,
            options={**mock_config_entry.options, CONF_ADAPTIVE_ACTIVE: True},
        )
        _seed_boost(hass_storage, mock_config_entry, minutes=-5, was_in_base=True)

        await controller.async_restore_state()

        assert controller.state == AutomationState.BASE
        # One write only: the base level (summer default 2)
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "2")]]
        assert _persisted(hass_storage, mock_config_entry) == {}

    async def test_restore_ignores_load_failure(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        with patch.object(
            controller._store, "async_load", AsyncMock(side_effect=OSError("disk"))
        ):
            await controller.async_restore_state()
        assert controller.state == AutomationState.IDLE
        mock_coordinator.client.write_parameters.assert_not_awaited()

    async def test_restore_ignores_malformed_data(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        mock_coordinator: MagicMock,
        hass_storage: dict[str, Any],
    ) -> None:
        key = _store_key(mock_config_entry)
        hass_storage[key] = {
            "version": 1,
            "minor_version": 1,
            "key": key,
            "data": {"end": "not-a-date"},
        }
        await controller.async_restore_state()
        assert controller.state == AutomationState.IDLE
        mock_coordinator.client.write_parameters.assert_not_awaited()


# =====================================================================
# Review round 3 regression tests
# =====================================================================


async def _fail_late(
    mock_coordinator: MagicMock,
) -> tuple[asyncio.Event, asyncio.Event]:
    """Make write_parameters block until released, then time out."""
    gate = asyncio.Event()
    started = asyncio.Event()

    async def slow_fail(*_a: Any, **_k: Any) -> None:
        started.set()
        await gate.wait()
        raise TimeoutError

    mock_coordinator.client.write_parameters = AsyncMock(side_effect=slow_fail)
    return gate, started


class TestSupersededWrites:
    async def test_superseded_boost_write_failing_late_is_not_queued(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        await controller.async_activate()
        gate, started = await _fail_late(mock_coordinator)
        task = asyncio.ensure_future(controller.async_activate_extra_ventilation())
        await started.wait()
        # The user picks a level: override + deactivate
        controller.async_user_override()
        await controller.async_deactivate()
        gate.set()
        await task

        assert controller.state == AutomationState.IDLE
        assert controller._pending_writes is None
        mock_coordinator.client.write_parameters = AsyncMock()
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_on_coordinator_update()
        assert _written(mock_coordinator) == []

    async def test_superseded_failure_keeps_newer_queue(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        gate, started = await _fail_late(mock_coordinator)
        task = asyncio.ensure_future(
            controller.async_write_params(
                [(1002, "1"), (1001, "3")], generation=controller._generation
            )
        )
        await started.wait()
        controller._generation += 1
        controller._pending_writes = [(1002, "1"), (1001, "0")]
        gate.set()
        await task
        assert controller._pending_writes == [(1002, "1"), (1001, "0")]

    async def test_idle_drops_queued_non_restore_write(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        controller._pending_writes = [(1002, "1"), (1001, "3")]
        await controller.async_on_coordinator_update()
        assert _written(mock_coordinator) == []
        assert controller._pending_writes is None

    async def test_override_cancels_inflight_restore_and_late_failure_is_dropped(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        gate, _started = await _fail_late(mock_coordinator)
        _expire_boost(controller)  # restore task now in flight
        await asyncio.sleep(0)
        controller.async_user_override()  # user picks level 3 (== boost level)
        gate.set()
        await hass.async_block_till_done()

        assert controller._pending_restore_expected is None
        assert not controller.has_pending_writes
        mock_coordinator.client.write_parameters = AsyncMock()
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        await controller.async_on_coordinator_update()
        assert _written(mock_coordinator) == []


class TestOwedRestorePersistence:
    async def _fail_restore_at_expiry(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        error: Exception,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        mock_coordinator.client.write_parameters = AsyncMock(side_effect=error)
        _expire_boost(controller)
        await hass.async_block_till_done()

    async def test_queued_restore_survives_restart(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await self._fail_restore_at_expiry(
            hass, controller, mock_coordinator, aiohttp.ClientError()
        )
        assert controller._pending_writes is not None
        owed = _persisted(hass_storage, mock_config_entry)["restore"]
        assert owed["snapshot"] == ["1", "1"]
        assert owed["expected"] == [1, 3]
        assert owed["system_id"] == SYSTEM_ID

        # Restart: new controller, the unit is still at the boost level
        await controller.async_cleanup()
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        mock_coordinator.client.write_parameters = AsyncMock()
        ctrl2 = BrinkAutomationController(hass, mock_coordinator, mock_config_entry)
        await ctrl2.async_restore_state()

        assert ctrl2.state == AutomationState.IDLE
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "1")]]
        assert _persisted(hass_storage, mock_config_entry) == {}
        await ctrl2.async_cleanup()

    async def test_owed_restore_dropped_when_device_changed_during_downtime(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await self._fail_restore_at_expiry(
            hass, controller, mock_coordinator, aiohttp.ClientError()
        )
        await controller.async_cleanup()
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "0")
        mock_coordinator.client.write_parameters = AsyncMock()
        ctrl2 = BrinkAutomationController(hass, mock_coordinator, mock_config_entry)
        await ctrl2.async_restore_state()

        assert _written(mock_coordinator) == []
        assert _persisted(hass_storage, mock_config_entry) == {}
        await ctrl2.async_cleanup()

    async def test_credential_error_keeps_owed_restore_persisted(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await self._fail_restore_at_expiry(
            hass,
            controller,
            mock_coordinator,
            BrinkAuthError("bad password", is_credentials_error=True),
        )
        assert controller._pending_writes is None
        assert "restore" in _persisted(hass_storage, mock_config_entry)

    async def test_successful_restore_clears_owed(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        await controller.async_cancel_extra_ventilation()
        assert _persisted(hass_storage, mock_config_entry) == {}

    async def test_user_override_drops_owed_restore(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await self._fail_restore_at_expiry(
            hass, controller, mock_coordinator, aiohttp.ClientError()
        )
        controller.async_user_override()
        await hass.async_block_till_done()
        assert _persisted(hass_storage, mock_config_entry) == {}


class TestBoostLevelTracking:
    async def test_season_change_updates_boost_level_and_persisted(
        self,
        hass: HomeAssistant,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()  # summer: level 3
        assert controller._boost_level == 3
        _set_coordinator_param(mock_coordinator, PARAM_FRESH_AIR_TEMP, "-10")
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        await controller.async_on_coordinator_update()  # winter: level 2
        assert _written(mock_coordinator)[-1] == [(1002, "1"), (1001, "2")]
        assert controller._boost_level == 2
        assert _persisted(hass_storage, mock_config_entry)["boost_level"] == 2

        # The boost expires while the cloud is down; the queued restore must
        # still be replayed against the winter boost level
        mock_coordinator.client.write_parameters = AsyncMock(
            side_effect=aiohttp.ClientError()
        )
        _expire_boost(controller)
        await hass.async_block_till_done()
        assert controller._pending_restore_expected == (1, 2)
        mock_coordinator.client.write_parameters = AsyncMock()
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "2")
        await controller.async_on_coordinator_update()
        assert _written(mock_coordinator) == [[(1002, "1"), (1001, "1")]]

    async def test_expired_at_startup_seeds_boost_level_from_persisted(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        _seed_boost(
            hass_storage,
            mock_config_entry,
            minutes=-5,
            snapshot=["1", "1"],
            boost_level=2,
        )
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "2")
        mock_coordinator.client.write_parameters = AsyncMock(
            side_effect=aiohttp.ClientError()
        )
        await controller.async_restore_state()
        assert controller._pending_restore_expected == (1, 2)


class TestStartupDeviceCheck:
    async def test_expired_boost_not_restored_over_user_change(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _seed_boost(
            hass_storage, mock_config_entry, minutes=-60, snapshot=["1", "1"]
        )
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "0")
        with caplog.at_level(logging.INFO):
            await controller.async_restore_state()
        assert _written(mock_coordinator) == []
        assert _persisted(hass_storage, mock_config_entry) == {}
        assert "Dropping persisted boost" in caplog.text

    async def test_running_boost_not_resumed_when_device_changed(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        _seed_boost(
            hass_storage, mock_config_entry, minutes=60, snapshot=["1", "1"]
        )
        _set_coordinator_param(mock_coordinator, PARAM_OPERATING_MODE, "0")
        await controller.async_restore_state()
        assert controller.state == AutomationState.IDLE
        await controller.async_on_coordinator_update()
        assert _written(mock_coordinator) == []
        assert _persisted(hass_storage, mock_config_entry) == {}

    async def test_resumed_remaining_time_is_clamped_to_duration(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        _seed_boost(
            hass_storage, mock_config_entry, minutes=600, snapshot=["1", "1"]
        )
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        await controller.async_restore_state()
        assert controller.state == AutomationState.BOOSTED
        assert controller.boost_remaining_minutes <= DEFAULT_EXTRA_VENT_DURATION

    async def test_stale_persisted_boost_dropped_without_writes(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        _seed_boost(
            hass_storage, mock_config_entry, minutes=-25 * 60, snapshot=["1", "1"]
        )
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        await controller.async_restore_state()
        assert _written(mock_coordinator) == []
        assert _persisted(hass_storage, mock_config_entry) == {}

    async def test_boost_is_saved_with_system_id(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await controller.async_activate_extra_ventilation()
        assert _persisted(hass_storage, mock_config_entry)["system_id"] == SYSTEM_ID

    async def test_boost_of_another_system_is_dropped(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _seed_boost(
            hass_storage,
            mock_config_entry,
            minutes=30,
            snapshot=["1", "1"],
            system_id=999,
        )
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        with caplog.at_level(logging.WARNING):
            await controller.async_restore_state()
        assert controller.state == AutomationState.IDLE
        assert _written(mock_coordinator) == []
        assert _persisted(hass_storage, mock_config_entry) == {}
        assert "Dropping persisted boost state" in caplog.text


class TestStaleSnapshotAfterRestore:
    async def test_poll_started_before_restore_does_not_poison_snapshot(
        self,
        controller: BrinkAutomationController,
        mock_coordinator: MagicMock,
    ) -> None:
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "1")
        await controller.async_activate_extra_ventilation()
        _set_coordinator_param(mock_coordinator, PARAM_VENTILATION_LEVEL, "3")
        await controller.async_cancel_extra_ventilation()  # restore (1, 1) written
        # A poll whose fetch started before the restore lands now (stale data)
        await controller.async_on_coordinator_update()
        await controller.async_activate_extra_ventilation()
        assert controller._pre_boost_snapshot == ("1", "1")


class TestPersistenceSerialization:
    async def test_concurrent_clear_is_not_overtaken_by_stale_save(
        self,
        controller: BrinkAutomationController,
        mock_config_entry: MockConfigEntry,
        hass_storage: dict[str, Any],
    ) -> None:
        await controller.async_activate_extra_ventilation()
        gate = asyncio.Event()
        original_save = controller._store.async_save

        async def slow_save(data: dict[str, Any]) -> None:
            await gate.wait()
            await original_save(data)

        with patch.object(controller._store, "async_save", slow_save):
            save_task = asyncio.ensure_future(controller._async_sync_persisted_boost())
            await asyncio.sleep(0)  # payload (boost) built, save blocked
            controller._state = AutomationState.IDLE
            clear_task = asyncio.ensure_future(controller._async_sync_persisted_boost())
            await asyncio.sleep(0)
            gate.set()
            await asyncio.gather(save_task, clear_task)

        assert _persisted(hass_storage, mock_config_entry) == {}
