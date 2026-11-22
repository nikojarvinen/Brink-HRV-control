"""Tests for Brink Home Ventilation select entities."""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er

from custom_components.brink_ventilation.automation_controller import AutomationState
from custom_components.brink_ventilation.const import (
    BYPASS_OPERATION_MAP,
    DOMAIN,
    OPERATING_MODE_MAP,
    PARAM_OPERATING_MODE,
    PARAM_VENTILATION_LEVEL,
    VENTILATION_LEVEL_MAP,
)
from tests.conftest import SYSTEM_ID, build_coordinator_data


def _get_entity_id(hass: HomeAssistant, unique_id: str, platform: str = "select") -> str:
    """Look up entity_id from the registry by unique_id."""
    registry = er.async_get(hass)
    entry = registry.async_get_entity_id(platform, DOMAIN, unique_id)
    assert entry is not None, f"Entity with unique_id '{unique_id}' not found in registry"
    return entry


async def test_mode_select_options(hass: HomeAssistant, setup_integration) -> None:
    """Test operating mode select lists all 5 operating modes."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_mode")
    state = hass.states.get(entity_id)
    assert state is not None

    options = state.attributes.get("options")
    assert options is not None
    expected = list(OPERATING_MODE_MAP.values())
    assert sorted(options) == sorted(expected)
    assert len(options) == 5


async def test_mode_select_current_value(hass: HomeAssistant, setup_integration) -> None:
    """Test operating mode select shows correct value for API value '1' -> 'manual'."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_mode")
    state = hass.states.get(entity_id)
    assert state is not None
    # Default operating_mode in build_coordinator_data is "1" -> "manual"
    assert state.state == "manual"


async def test_mode_select_write(hass: HomeAssistant, setup_integration) -> None:
    """Test selecting 'holiday' calls write_parameters with correct value."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_mode")
    entry = setup_integration
    coordinator = entry.runtime_data.coordinator

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "holiday"},
        blocking=True,
    )

    # write_parameters should have been called
    coordinator.client.write_parameters.assert_called()
    call_args = coordinator.client.write_parameters.call_args
    system_id_arg = call_args[0][0]
    params_arg = call_args[0][1]

    assert system_id_arg == SYSTEM_ID
    # Operating mode "holiday" maps to value "2" via OPERATING_MODE_REVERSE
    # The param has value_id 1002 in mock data
    assert any(vid == 1002 and val == "2" for vid, val in params_arg)


async def test_bypass_select_options(hass: HomeAssistant, setup_integration) -> None:
    """Test bypass select lists 3 bypass options."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_bypass_operation")
    state = hass.states.get(entity_id)
    assert state is not None

    options = state.attributes.get("options")
    assert options is not None
    expected = list(BYPASS_OPERATION_MAP.values())
    assert sorted(options) == sorted(expected)
    assert len(options) == 3


async def test_bypass_select_write(hass: HomeAssistant, setup_integration) -> None:
    """Test bypass select write calls write_parameters."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_bypass_operation")
    entry = setup_integration
    coordinator = entry.runtime_data.coordinator

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "bypass_open"},
        blocking=True,
    )

    coordinator.client.write_parameters.assert_called()
    call_args = coordinator.client.write_parameters.call_args
    params_arg = call_args[0][1]
    # bypass_operation value_id is 1016, "bypass_open" -> "2"
    assert any(vid == 1016 and val == "2" for vid, val in params_arg)


async def test_ventilation_level_options(hass: HomeAssistant, setup_integration) -> None:
    """Test ventilation level select lists levels 0-3 plus 'adaptive'."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    state = hass.states.get(entity_id)
    assert state is not None

    options = state.attributes.get("options")
    assert options is not None
    expected = list(VENTILATION_LEVEL_MAP.values())
    assert sorted(options) == sorted(expected)
    # 5 options: level_0, level_1, level_2, level_3, adaptive
    assert len(options) == 5


async def test_ventilation_level_write(hass: HomeAssistant, setup_integration) -> None:
    """Test selecting 'level_2' writes level and switches to manual mode."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    entry = setup_integration
    coordinator = entry.runtime_data.coordinator

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "level_2"},
        blocking=True,
    )

    coordinator.client.write_parameters.assert_called()
    call_args = coordinator.client.write_parameters.call_args
    params_arg = call_args[0][1]

    # Should write both operating mode to "1" (manual) and ventilation level to "2"
    value_ids_and_vals = {(vid, val) for vid, val in params_arg}
    # Operating mode value_id=1002, value="1" (manual)
    assert (1002, "1") in value_ids_and_vals
    # Ventilation level value_id=1001, value="2"
    assert (1001, "2") in value_ids_and_vals


async def test_ventilation_level_adaptive(hass: HomeAssistant, setup_integration) -> None:
    """Test selecting 'adaptive' activates the automation controller."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    entry = setup_integration
    coordinator = entry.runtime_data.coordinator
    controller = coordinator.automation_controller

    # Controller should start IDLE
    assert controller.state == AutomationState.IDLE

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "adaptive"},
        blocking=True,
    )

    # Controller should now be in BASE state (activated)
    assert controller.state == AutomationState.BASE

    # The current_option should now show "adaptive"
    state = hass.states.get(entity_id)
    assert state is not None
    assert state.state == "adaptive"


async def test_ventilation_level_from_adaptive(
    hass: HomeAssistant, setup_integration
) -> None:
    """Test switching from adaptive to level_1 deactivates the controller."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    entry = setup_integration
    coordinator = entry.runtime_data.coordinator
    controller = coordinator.automation_controller

    # First activate adaptive mode
    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "adaptive"},
        blocking=True,
    )
    assert controller.state == AutomationState.BASE

    # Reset write_parameters mock so we can check the new call cleanly
    coordinator.client.write_parameters.reset_mock()

    # Now switch to level_1
    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "level_1"},
        blocking=True,
    )

    # Controller should be deactivated (IDLE)
    assert controller.state == AutomationState.IDLE

    # Should have written the level change
    coordinator.client.write_parameters.assert_called()


async def test_select_write_error_reverts(
    hass: HomeAssistant, setup_integration
) -> None:
    """Test that a write failure reverts the optimistic state."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_mode")
    entry = setup_integration
    coordinator = entry.runtime_data.coordinator

    # Verify initial state
    state = hass.states.get(entity_id)
    assert state is not None
    original_value = state.state

    # Make write_parameters fail
    coordinator.client.write_parameters = AsyncMock(
        side_effect=aiohttp.ClientError("Connection failed")
    )

    # Attempt to change mode, expect failure
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            "select",
            "select_option",
            {"entity_id": entity_id, "option": "holiday"},
            blocking=True,
        )

    # The state should have reverted to the original value
    state = hass.states.get(entity_id)
    assert state is not None
    assert state.state == original_value


async def test_expedited_polling_triggered(
    hass: HomeAssistant, setup_integration
) -> None:
    """Test that after a successful write, coordinator.start_expedited_polling is called."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_mode")
    entry = setup_integration
    coordinator = entry.runtime_data.coordinator

    # Patch start_expedited_polling to track calls
    with patch.object(coordinator, "start_expedited_polling") as mock_expedited:
        await hass.services.async_call(
            "select",
            "select_option",
            {"entity_id": entity_id, "option": "holiday"},
            blocking=True,
        )

        mock_expedited.assert_called_once()


async def test_current_option_not_adaptive_during_boost_started_while_off(
    hass: HomeAssistant, setup_integration
) -> None:
    """A manual boost with adaptive off must not display 'adaptive'."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    coordinator = setup_integration.runtime_data.coordinator
    controller = coordinator.automation_controller

    await controller.async_activate_extra_ventilation()
    try:
        assert controller.state == AutomationState.BOOSTED
        assert controller.adaptive_active is False
        state = hass.states.get(entity_id)
        assert state is not None
        # Falls back to the API-reported level (mock data: level 2)
        assert state.state == "level_2"
    finally:
        await controller.async_deactivate()


async def test_current_option_adaptive_during_boost_started_from_adaptive(
    hass: HomeAssistant, setup_integration
) -> None:
    """A boost during adaptive mode still shows 'adaptive'."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    coordinator = setup_integration.runtime_data.coordinator
    controller = coordinator.automation_controller

    await controller.async_activate()
    await controller.async_activate_extra_ventilation()
    try:
        assert controller.state == AutomationState.BOOSTED
        state = hass.states.get(entity_id)
        assert state is not None
        assert state.state == "adaptive"
    finally:
        await controller.async_deactivate()


async def test_level_select_validation_failure_keeps_adaptive_active(
    hass: HomeAssistant, setup_integration
) -> None:
    """If validation fails, adaptive mode must not have been deactivated."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    coordinator = setup_integration.runtime_data.coordinator
    controller = coordinator.automation_controller

    await controller.async_activate()
    assert controller.state == AutomationState.BASE
    coordinator.client.write_parameters.reset_mock()

    # Make the level parameter read-only (no value_id)
    params = coordinator.data[SYSTEM_ID]["components"][0]["parameters"]
    params[PARAM_VENTILATION_LEVEL]["value_id"] = None

    try:
        with pytest.raises(HomeAssistantError):
            await hass.services.async_call(
                "select",
                "select_option",
                {"entity_id": entity_id, "option": "level_1"},
                blocking=True,
            )

        assert controller.state == AutomationState.BASE
        coordinator.client.write_parameters.assert_not_called()
    finally:
        await controller.async_deactivate()


async def test_level_select_during_idle_boost_wins_over_restore(
    hass: HomeAssistant, setup_integration
) -> None:
    """Picking a level during a boost started while off does not restore the old state."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    coordinator = setup_integration.runtime_data.coordinator
    controller = coordinator.automation_controller

    await controller.async_activate_extra_ventilation()
    assert controller.state == AutomationState.BOOSTED
    assert controller._pre_boost_snapshot is not None
    coordinator.client.write_parameters.reset_mock()

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "level_0"},
        blocking=True,
    )

    assert controller.state == AutomationState.IDLE
    assert controller._pre_boost_snapshot is None
    # Exactly one write: the user's choice (mode manual + level 0), no restore
    coordinator.client.write_parameters.assert_called_once()
    params_arg = coordinator.client.write_parameters.call_args[0][1]
    assert (1002, "1") in params_arg
    assert (1001, "0") in params_arg


def _queue_idle_restore(controller, expected=(1, 3)) -> None:
    """Simulate a failed IDLE boost restore waiting for the next poll."""
    assert controller.state == AutomationState.IDLE
    controller._pending_writes = [(1002, "1"), (1001, "1")]
    controller._pending_restore_expected = expected
    assert controller.has_pending_writes


async def test_mode_select_drops_queued_controller_restore(
    hass: HomeAssistant, setup_integration
) -> None:
    """A user mode change must not be overwritten by a queued boost restore."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_mode")
    controller = setup_integration.runtime_data.coordinator.automation_controller
    _queue_idle_restore(controller)

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "holiday"},
        blocking=True,
    )

    assert not controller.has_pending_writes


async def test_level_select_drops_queued_controller_restore(
    hass: HomeAssistant, setup_integration
) -> None:
    """A user level change must not be overwritten by a queued boost restore."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_ventilation_level")
    controller = setup_integration.runtime_data.coordinator.automation_controller
    _queue_idle_restore(controller)

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "level_0"},
        blocking=True,
    )

    assert not controller.has_pending_writes


async def test_bypass_select_keeps_queued_controller_restore(
    hass: HomeAssistant, setup_integration
) -> None:
    """Bypass is unrelated to the restore and must not drop it."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_bypass_operation")
    controller = setup_integration.runtime_data.coordinator.automation_controller
    # Expect the values the mocked device really reports (manual, level 2) so the
    # poll triggered by the write does not drop the restore for another reason
    _queue_idle_restore(controller, expected=(1, 2))

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": BYPASS_OPERATION_MAP["1"]},
        blocking=True,
    )

    # Not dropped: the poll after the write replayed the restore
    coordinator = setup_integration.runtime_data.coordinator
    written = [c.args[1] for c in coordinator.client.write_parameters.await_args_list]
    assert [(1002, "1"), (1001, "1")] in written


async def test_mode_select_during_idle_boost_ends_it_without_restore(
    hass: HomeAssistant, setup_integration
) -> None:
    """An explicit mode choice ends a boost started while adaptive was off."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_mode")
    coordinator = setup_integration.runtime_data.coordinator
    controller = coordinator.automation_controller

    await controller.async_activate_extra_ventilation()
    assert controller.state == AutomationState.BOOSTED
    assert controller.adaptive_active is False
    coordinator.client.write_parameters.reset_mock()

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": entity_id, "option": "holiday"},
        blocking=True,
    )

    assert controller.state == AutomationState.IDLE
    assert controller._pre_boost_snapshot is None
    assert controller.boost_remaining_minutes == 0
    # Only the user's mode write; no restore of the old mode/level
    coordinator.client.write_parameters.assert_called_once()


async def test_mode_select_during_adaptive_boost_keeps_boost(
    hass: HomeAssistant, setup_integration
) -> None:
    """Behavior with adaptive active is unchanged (pending product decision)."""
    entity_id = _get_entity_id(hass, f"{DOMAIN}_{SYSTEM_ID}_mode")
    controller = setup_integration.runtime_data.coordinator.automation_controller

    await controller.async_activate()
    await controller.async_activate_extra_ventilation()
    assert controller.adaptive_active is True
    try:
        await hass.services.async_call(
            "select",
            "select_option",
            {"entity_id": entity_id, "option": "holiday"},
            blocking=True,
        )
        assert controller.state == AutomationState.BOOSTED
    finally:
        await controller.async_deactivate()
