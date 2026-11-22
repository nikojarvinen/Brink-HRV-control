"""Automation controller for Brink Home ventilation.

Implements a 3-state state machine (IDLE / BASE / BOOSTED) that manages
seasonal ventilation levels, humidity-spike detection, and a resilient
write queue that retries failed API writes on the next coordinator refresh.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTemperature
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util
from homeassistant.util.unit_conversion import TemperatureConverter

from .core.brink_home_cloud import BrinkAuthError
from .const import (
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

if TYPE_CHECKING:
    from .coordinator import BrinkDataCoordinator

_LOGGER = logging.getLogger(__name__)

_COUNTDOWN_INTERVAL = 30  # seconds between countdown updates
_BOOST_STORE_VERSION = 1
# A persisted boost that ended longer ago than this is stale: whatever the unit
# shows now is not the controller's business any more.
_STALE_BOOST_SECONDS = 24 * 3600


def boost_store(hass: HomeAssistant, entry_id: str) -> Store[dict[str, Any]]:
    """Return the Store holding the persisted boost state for a config entry."""
    return Store(hass, _BOOST_STORE_VERSION, f"{DOMAIN}.{entry_id}.boost")


class AutomationState(StrEnum):
    """State machine states for the Brink automation controller."""

    IDLE = "idle"
    BASE = "base"
    BOOSTED = "boosted"


class BrinkAutomationController:
    """Manages seasonal ventilation, humidity-spike boost, and write retries.

    State machine:
        IDLE  --(adaptive selected)-->  BASE
        BASE  --(humidity spike / button)-->  BOOSTED
        BOOSTED  --(timer expires)-->  BASE
        BASE / BOOSTED  --(adaptive deselected)-->  IDLE
    """

    def __init__(
        self,
        hass: HomeAssistant,
        coordinator: BrinkDataCoordinator,
        entry: ConfigEntry,
    ) -> None:
        """Initialize the automation controller."""
        self._hass = hass
        self._coordinator = coordinator
        self._entry = entry

        self._state: AutomationState = AutomationState.IDLE
        self._season: str | None = None

        # Two-point humidity tracking: entity_id -> (monotonic_timestamp, value)
        self._humidity_previous: dict[str, tuple[float, float]] = {}
        # Last computed rate per sensor: entity_id -> rate (%/min)
        self._humidity_rates: dict[str, float] = {}

        # Resilient write queue -- stores the most recent desired params on failure
        self._pending_writes: list[tuple[int, str]] | None = None
        # For a queued IDLE boost-restore: the (mode, level) boost values the
        # restore is meant to undo; it is only replayed while the device still
        # reports them (otherwise the user changed something meanwhile).
        self._pending_restore_expected: tuple[int | str | None, int | str | None] | None = None
        # Restore owed to the unit after an IDLE-origin boost. Persisted until
        # it succeeds or is dropped (device guard / user override), so it
        # survives a restart. Keys: snapshot, expected, system_id.
        self._owed_restore: dict[str, Any] | None = None

        # Bumped whenever the state machine is reset from outside an in-flight
        # boost start, so the boost can detect it was superseded.
        self._generation: int = 0

        # Stale-snapshot guard: poll counter, and the last restore target with
        # the poll counter value at the time it was written.
        self._data_seq: int = 0
        self._last_restore_target: tuple[str, str | None] | None = None
        self._last_restore_data_seq: int = -1

        # True when a controller write succeeded since the latest poll data
        # arrived (the data predates it, so the watchdog must not act on it)
        self._wrote_since_data: bool = False

        # Controlled system, pinned on first use
        self._system_id: int | None = None

        # Ventilation level written for the running boost
        self._boost_level: int | None = None

        # Persisted boost (survives restart/reload)
        self._store: Store[dict[str, Any]] = boost_store(hass, entry.entry_id)
        # Serializes every persistence write (payload is built under the lock)
        self._persist_lock = asyncio.Lock()

        # Track whether automation was in BASE before boost (for correct return state)
        self._was_in_base_before_boost: bool = False

        # Operating mode / ventilation level captured when a boost starts from
        # IDLE (adaptive off), restored when that boost ends: (mode, level|None)
        self._pre_boost_snapshot: tuple[str, str | None] | None = None

        self._boost_end_monotonic: float = 0.0

        self._boost_pending: bool = False
        self._pending_task: asyncio.Task[None] | None = None

        # Boost trigger info (None = manual, set only for automation triggers)
        self._boost_trigger: str | None = None
        self._boost_trigger_entity: str | None = None
        self._boost_trigger_rate: float | None = None

        # Level watchdog cooldown (monotonic timestamp of last correction)
        self._last_level_correction: float = 0.0

        # Unsub handles
        self._boost_timer_unsub: CALLBACK_TYPE | None = None
        self._countdown_timer_unsub: CALLBACK_TYPE | None = None
        self._humidity_timer_unsub: CALLBACK_TYPE | None = None

        # Sensor list the humidity timer was last started for
        self._monitored_humidity_sensors: list[str] = []

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def state(self) -> AutomationState:
        """Return the current automation state."""
        return self._state

    @property
    def adaptive_active(self) -> bool:
        """Return True when Adaptive (HA) mode is active.

        True in BASE, and in BOOSTED when the boost started from BASE (the
        controller returns to BASE afterwards).
        """
        return self._state == AutomationState.BASE or (
            self._state == AutomationState.BOOSTED
            and self._was_in_base_before_boost
        )

    @property
    def season(self) -> str | None:
        """Return the current season (SEASON_SUMMER or SEASON_WINTER)."""
        return self._season

    @property
    def boost_remaining_minutes(self) -> int:
        """Return remaining boost minutes, 0 when not boosted."""
        if self._state != AutomationState.BOOSTED:
            return 0
        remaining = self._boost_end_monotonic - time.monotonic()
        return max(0, math.ceil(remaining / 60))

    @property
    def has_pending_writes(self) -> bool:
        """Return True if there are pending writes awaiting retry."""
        return self._pending_writes is not None

    @property
    def boost_trigger(self) -> str | None:
        """Return the trigger type for the current boost, or None for manual."""
        if self._state != AutomationState.BOOSTED:
            return None
        return self._boost_trigger

    @property
    def boost_trigger_entity(self) -> str | None:
        """Return the entity_id that triggered the boost (humidity spike only)."""
        if self._state != AutomationState.BOOSTED:
            return None
        return self._boost_trigger_entity

    @property
    def boost_trigger_rate(self) -> float | None:
        """Return the humidity rate that triggered the boost (%/min)."""
        if self._state != AutomationState.BOOSTED:
            return None
        return self._boost_trigger_rate

    @property
    def configured_humidity_sensors(self) -> list[str]:
        """Return entity_ids of configured humidity sensors from options."""
        options = self._entry.options
        sensors: list[str] = []
        for key in (CONF_HUMIDITY_SENSOR_1, CONF_HUMIDITY_SENSOR_2, CONF_HUMIDITY_SENSOR_3):
            entity_id = options.get(key, "")
            if entity_id:
                sensors.append(entity_id)
        return sensors

    @property
    def humidity_deltas(self) -> dict[str, float]:
        """Return current humidity rate of change (%/min) for each monitored sensor.

        Returns a dict of entity_id -> rate value. Rate is 0.0 when the
        sensor value has not changed since the last timer tick.
        """
        return dict(self._humidity_rates)

    @property
    def max_humidity_delta(self) -> float:
        """Return the maximum humidity rate of change (%/min) across all sensors.

        This is the most useful single number for the user — shows the highest
        spike rate happening right now across all sensors.
        """
        deltas = self.humidity_deltas
        if not deltas:
            return 0.0
        return max(deltas.values())

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def async_start_humidity_monitoring(self) -> None:
        """Start humidity timer for delta tracking, independent of automation state.

        Called during setup so the humidity delta sensors always have data,
        even when Adaptive (HA) mode is not active.
        """
        self._start_humidity_timer()

    async def async_activate(self) -> None:
        """Transition IDLE -> BASE: start seasonal ventilation and humidity monitoring.

        If a boost started outside adaptive mode is running, adaptive mode is
        enabled without interrupting it: the boost keeps running and returns
        to the seasonal base level (instead of the pre-boost snapshot) when
        it ends.
        """
        if (
            self._state == AutomationState.BOOSTED
            and not self._was_in_base_before_boost
        ):
            self._generation += 1
            self._cancel_stale_pending_task()
            self._was_in_base_before_boost = True
            self._pre_boost_snapshot = None
            self._start_humidity_timer()
            self._hass.config_entries.async_update_entry(
                self._entry,
                options={**self._entry.options, CONF_ADAPTIVE_ACTIVE: True},
            )
            self._coordinator.async_update_listeners()
            _LOGGER.info(
                "Adaptive mode enabled during boost; will return to base "
                "level when the boost ends"
            )
            await self._async_sync_persisted_boost()
            return

        if self._state != AutomationState.IDLE:
            _LOGGER.debug(
                "async_activate called in state %s, expected IDLE", self._state
            )
            return

        self._generation += 1
        self._cancel_stale_pending_task()
        self._clear_pending_writes()
        await self._async_drop_owed_restore()
        self._last_restore_target = None
        self._state = AutomationState.BASE

        # Ensure humidity timer is running (may already be from monitoring)
        self._start_humidity_timer()

        # Persist adaptive mode active flag for startup recovery
        self._hass.config_entries.async_update_entry(
            self._entry,
            options={**self._entry.options, CONF_ADAPTIVE_ACTIVE: True},
        )

        base_level = await self._async_apply_seasonal_level(boosted=False)

        _LOGGER.info(
            "Automation activated: season=%s, base_level=%s",
            self._season,
            base_level,
        )

    async def async_deactivate(self) -> None:
        """Transition any state -> IDLE: cancel timers but keep humidity monitoring.

        Humidity timer remains active so the delta sensors continue to
        show data even when Adaptive (HA) mode is not active.
        """
        _LOGGER.info("Automation deactivating from state %s", self._state)

        self._generation += 1
        self._cancel_pending_task()
        self._boost_pending = False
        self._cancel_boost_timer()
        self._cancel_countdown_timer()
        self._clear_pending_writes()
        self._owed_restore = None  # (the sync below persists the tombstone)
        self._was_in_base_before_boost = False
        # The caller's explicit choice wins: drop the snapshot, do not restore
        self._pre_boost_snapshot = None
        self._last_restore_target = None
        self._boost_level = None
        self._boost_end_monotonic = 0.0
        self._clear_boost_trigger()
        self._state = AutomationState.IDLE

        # Clear adaptive mode active flag
        new_opts = {**self._entry.options, CONF_ADAPTIVE_ACTIVE: False}
        new_opts.pop(CONF_ADAPTIVE_ACTIVE_LEGACY, None)  # migrate old key
        if self._entry.options.get(CONF_ADAPTIVE_ACTIVE, False) or self._entry.options.get(CONF_ADAPTIVE_ACTIVE_LEGACY, False):
            self._hass.config_entries.async_update_entry(
                self._entry,
                options=new_opts,
            )

        # Notify entities so the switch / remaining-time sensor update now
        self._coordinator.async_update_listeners()
        await self._async_sync_persisted_boost()

    @callback
    def async_user_override(self) -> None:
        """Called before a user write to mode / ventilation level.

        While IDLE, a restore (queued, in flight or owed) must not overwrite
        the user's choice later, and the last restore target no longer
        describes the unit.
        """
        self._last_restore_target = None
        if self._state != AutomationState.IDLE:
            return
        if self._pending_writes is not None or self._owed_restore is not None:
            _LOGGER.info("User changed mode/level; dropping boost restore")
        # Supersede an in-flight restore (or boost start): it must neither
        # land nor be queued after this point
        self._generation += 1
        self._cancel_pending_task()
        self._clear_pending_writes()
        self._drop_owed_restore()

    async def async_activate_extra_ventilation(
        self,
        trigger: str | None = None,
        trigger_entity: str | None = None,
        trigger_rate: float | None = None,
    ) -> None:
        """Activate extra ventilation boost (shared by button and humidity trigger).

        Sets the device to the seasonal max level in manual mode and starts
        the boost timer.

        Args:
            trigger: Trigger type (e.g. BOOST_TRIGGER_HUMIDITY) or None for manual.
            trigger_entity: Entity ID that caused the trigger (humidity sensor).
            trigger_rate: Humidity rate of change that triggered the boost (%/min).
        """
        generation = self._generation
        try:
            # A stale restore / return-to-base task must not land after our write
            self._cancel_stale_pending_task()

            if self._state != AutomationState.BOOSTED:
                was_in_base = self._state == AutomationState.BASE
                if self._state == AutomationState.IDLE:
                    snapshot = self._snapshot_mode_and_level()
                else:
                    snapshot = None
            else:
                # Re-trigger during a boost keeps the original return path
                # (_was_in_base_before_boost and the pre-boost snapshot).
                was_in_base = self._was_in_base_before_boost
                snapshot = self._pre_boost_snapshot

            duration_minutes: int = int(
                self._entry.options.get(
                    CONF_EXTRA_VENT_DURATION, DEFAULT_EXTRA_VENT_DURATION
                )
            )

            max_level = await self._async_apply_seasonal_level(
                boosted=True, generation=generation
            )

            if generation != self._generation:
                _LOGGER.info("Extra ventilation boost superseded; not starting it")
                return

            # A new boost supersedes any restore still owed from the last one
            self._owed_restore = None
            self._was_in_base_before_boost = was_in_base
            self._pre_boost_snapshot = snapshot
            self._boost_level = max_level

            # Store trigger info (None = manual)
            self._boost_trigger = trigger
            self._boost_trigger_entity = trigger_entity
            self._boost_trigger_rate = trigger_rate

            self._start_boost_timers(duration_minutes * 60)
            self._state = AutomationState.BOOSTED

            # Fire logbook event
            self._fire_boost_activated_event(max_level, duration_minutes)

            # Notify entities (does not touch the coordinator's poll schedule)
            self._coordinator.async_update_listeners()

            _LOGGER.info(
                "Extra ventilation activated: season=%s, max_level=%s, duration=%d min, trigger=%s",
                self._season,
                max_level,
                duration_minutes,
                trigger or "manual",
            )
            await self._async_sync_persisted_boost()
        finally:
            # Cleared only now so the humidity tick cannot start a second
            # boost while the write above is in flight.
            self._boost_pending = False

    def _start_boost_timers(self, seconds: float) -> None:
        """(Re)start the boost expiry timer and the countdown timer."""
        self._cancel_boost_timer()
        self._cancel_countdown_timer()
        self._boost_end_monotonic = time.monotonic() + seconds
        self._boost_timer_unsub = async_call_later(
            self._hass, seconds, self._async_boost_timer_expired
        )
        self._start_countdown_timer()

    async def async_cancel_extra_ventilation(self) -> None:
        """Cancel an active extra ventilation boost.

        Returns to BASE if adaptive mode was active before the boost,
        otherwise returns to IDLE.
        """
        if self._state != AutomationState.BOOSTED:
            return

        self._generation += 1
        self._cancel_stale_pending_task()
        self._cancel_boost_timer()
        self._cancel_countdown_timer()

        # Fire deactivated event before clearing state
        self._fire_boost_deactivated_event("cancelled")

        self._clear_boost_trigger()

        if self._was_in_base_before_boost:
            _LOGGER.info("Extra ventilation cancelled, returning to BASE")
            self._pre_boost_snapshot = None
            self._state = AutomationState.BASE
            await self._async_sync_persisted_boost()
            await self._async_apply_seasonal_level(boosted=False)
        else:
            _LOGGER.info("Extra ventilation cancelled, returning to IDLE")
            snapshot = self._pre_boost_snapshot
            self._pre_boost_snapshot = None
            self._state = AutomationState.IDLE
            # (persists the owed restore, or clears the file)
            await self._async_restore_pre_boost(snapshot)

        # Notify entities so switch/sensor reflect new state
        self._coordinator.async_update_listeners()

    async def async_on_coordinator_update(self) -> None:
        """Handle coordinator data refresh: re-evaluate season and retry writes.

        Season is always evaluated (so the season sensor works regardless of
        automation state), but ventilation level changes only happen when
        Adaptive (HA) mode is active (BASE or BOOSTED).
        """
        # coordinator.data is fresh now: later writes are newer than it
        self._data_seq += 1
        self._wrote_since_data = False

        old_season = self._season
        self._season = self._evaluate_season()

        if self._state == AutomationState.IDLE:
            # Only a guarded boost restore is replayed while IDLE, and only
            # while the device still shows what it was meant to undo
            if self._pending_writes is not None:
                if self._pending_restore_expected is None:
                    _LOGGER.info("Dropping stale queued write while idle")
                    self._clear_pending_writes()
                elif not self._device_shows(self._pending_restore_expected):
                    _LOGGER.info(
                        "Dropping queued boost restore: device mode/level "
                        "changed since the boost"
                    )
                    self._clear_pending_writes()
                    await self._async_drop_owed_restore()
            await self._async_retry_pending_writes()
            return

        if old_season != self._season and self._season is not None:
            _LOGGER.info("Season changed: %s -> %s", old_season, self._season)
            await self._async_reapply_level()

        await self._async_retry_pending_writes()

        await self._verify_level()

    async def async_restore_state(self) -> None:
        """Restore automation state after HA restart.

        Checks entry.options for the adaptive_active flag. Also migrates the
        old ha_automated_active key for backward compatibility.
        """
        active = self._entry.options.get(
            CONF_ADAPTIVE_ACTIVE,
            self._entry.options.get(CONF_ADAPTIVE_ACTIVE_LEGACY, False),
        )
        persisted, owed = await self._async_load_persisted()
        self._owed_restore = owed

        # Only act on state that belongs to the system we control
        stored = persisted or owed
        stored_system = stored.get("system_id") if stored else None
        current_system = self._get_system_id()
        if (
            stored_system is not None
            and current_system is not None
            and stored_system != current_system
        ):
            _LOGGER.warning(
                "Dropping persisted boost state of system %s (controlling %s)",
                stored_system,
                current_system,
            )
            persisted = None
            self._owed_restore = None
            await self._async_sync_persisted_boost()

        remaining = 0.0
        clamped = False
        if persisted is not None:
            remaining = (persisted["end"] - dt_util.utcnow()).total_seconds()
            if remaining < -_STALE_BOOST_SECONDS:
                _LOGGER.info("Dropping stale persisted boost (ended long ago)")
                persisted = None
            elif not persisted["was_in_base"] and not self._device_shows(
                (1, self._normalize_param_value(persisted["boost_level"]))
            ):
                # An adaptive boost is re-asserted by the watchdog; an IDLE-origin
                # one is only valid while the unit still shows it
                _LOGGER.info(
                    "Dropping persisted boost: device mode/level changed "
                    "while Home Assistant was down"
                )
                persisted = None
            else:
                max_seconds = 60 * int(
                    self._entry.options.get(
                        CONF_EXTRA_VENT_DURATION, DEFAULT_EXTRA_VENT_DURATION
                    )
                )
                if remaining > max_seconds:
                    remaining = float(max_seconds)
                    clamped = True
            if persisted is None:
                await self._async_sync_persisted_boost()
        resume = persisted is not None and remaining > 0

        if active:
            _LOGGER.info("Restoring adaptive mode state after restart")
            # Migrate old key if present
            if CONF_ADAPTIVE_ACTIVE_LEGACY in self._entry.options:
                new_opts = {**self._entry.options, CONF_ADAPTIVE_ACTIVE: True}
                new_opts.pop(CONF_ADAPTIVE_ACTIVE_LEGACY, None)
                self._hass.config_entries.async_update_entry(
                    self._entry, options=new_opts
                )
            # Reset state to IDLE so async_activate can transition properly
            self._state = AutomationState.IDLE
            if resume:
                # The unit is still at the boost level: resume without the
                # base-level write async_activate would do; adaptive wins
                # (return to BASE afterwards).
                assert persisted is not None
                self._season = self._evaluate_season()
                self._start_humidity_timer()
                self._resume_boost(persisted, remaining, was_in_base=True)
                if clamped:
                    await self._async_sync_persisted_boost()
                return
            # (drops an owed restore: adaptive writes the base level)
            await self.async_activate()

        if persisted is None:
            if self._owed_restore is not None:
                await self._async_replay_owed_restore()
            return

        if resume:
            self._resume_boost(persisted, remaining, was_in_base=False)
            if clamped:
                await self._async_sync_persisted_boost()
            return

        # The boost ended while we were down: run the normal end transition.
        _LOGGER.info("Persisted boost expired while Home Assistant was down")
        self._boost_level = persisted["boost_level"]
        if not active:
            # (with adaptive on, async_activate above already wrote the base level)
            await self._async_restore_pre_boost(persisted["snapshot"])
        else:
            await self._async_sync_persisted_boost()

    async def _async_replay_owed_restore(self) -> None:
        """Replay a restore that was still owed at shutdown, through the guard."""
        owed = self._owed_restore
        assert owed is not None
        if not self._device_shows(owed["expected"]):
            _LOGGER.info(
                "Dropping owed boost restore: device mode/level changed "
                "since the boost"
            )
            await self._async_drop_owed_restore()
            return
        _LOGGER.info("Replaying boost restore that was still owed at shutdown")
        await self._async_restore_pre_boost(owed["snapshot"], owed["expected"])

    def _resume_boost(
        self, persisted: dict[str, Any], remaining: float, *, was_in_base: bool
    ) -> None:
        """Resume a persisted boost with its remaining duration (no write)."""
        self._was_in_base_before_boost = was_in_base
        self._pre_boost_snapshot = None if was_in_base else persisted["snapshot"]
        self._boost_level = persisted["boost_level"]
        self._boost_trigger = persisted["trigger"]
        self._boost_trigger_entity = persisted["trigger_entity"]
        self._boost_trigger_rate = persisted["trigger_rate"]
        self._start_boost_timers(remaining)
        self._state = AutomationState.BOOSTED
        self._coordinator.async_update_listeners()
        _LOGGER.info(
            "Resumed extra ventilation boost after restart: %d s remaining",
            int(remaining),
        )

    # ------------------------------------------------------------------
    # Boost persistence
    # ------------------------------------------------------------------

    async def _async_load_persisted(
        self,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Load and validate the persisted (boost, owed restore); at most one is set."""
        try:
            raw = await self._store.async_load()
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Could not load persisted boost state", exc_info=True)
            return None, None
        if not raw:  # absent, or the empty tombstone
            return None, None
        try:
            if "restore" in raw:
                owed = raw["restore"]
                snapshot = owed["snapshot"]
                expected = owed["expected"]
                return None, {
                    "snapshot": (
                        str(snapshot[0]),
                        None if snapshot[1] is None else str(snapshot[1]),
                    ),
                    "expected": (expected[0], expected[1]),
                    "system_id": owed.get("system_id"),
                }
            end = dt_util.parse_datetime(raw["end"])
            if end is None:
                raise ValueError("bad end time")
            snapshot = raw.get("snapshot")
            return {
                "end": dt_util.as_utc(end),
                "was_in_base": bool(raw.get("was_in_base_before_boost", False)),
                "snapshot": (
                    (str(snapshot[0]), None if snapshot[1] is None else str(snapshot[1]))
                    if snapshot
                    else None
                ),
                "boost_level": raw.get("boost_level"),
                "trigger": raw.get("trigger"),
                "trigger_entity": raw.get("trigger_entity"),
                "trigger_rate": raw.get("trigger_rate"),
                "system_id": raw.get("system_id"),
            }, None
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Ignoring malformed persisted boost state", exc_info=True)
            return None, None

    async def _async_sync_persisted_boost(self) -> None:
        """Make the persisted state mirror the current one.

        Saves the running boost, or the owed restore, or an empty tombstone.
        Serialized, with the payload built under the lock, so a stale save can
        never overwrite a later clear.
        """
        async with self._persist_lock:
            try:
                data: dict[str, Any]
                if self._state == AutomationState.BOOSTED:
                    remaining = max(0.0, self._boost_end_monotonic - time.monotonic())
                    end = dt_util.utcnow() + timedelta(seconds=remaining)
                    snapshot = self._pre_boost_snapshot
                    data = {
                        "end": end.isoformat(),
                        "was_in_base_before_boost": self._was_in_base_before_boost,
                        "snapshot": list(snapshot) if snapshot else None,
                        "boost_level": self._boost_level,
                        "trigger": self._boost_trigger,
                        "trigger_entity": self._boost_trigger_entity,
                        "trigger_rate": self._boost_trigger_rate,
                        "system_id": self._get_system_id(),
                    }
                elif self._owed_restore is not None:
                    owed = self._owed_restore
                    data = {
                        "restore": {
                            "snapshot": list(owed["snapshot"]),
                            "expected": list(owed["expected"]),
                            "system_id": owed["system_id"],
                        }
                    }
                else:
                    data = {}
                await self._store.async_save(data)
            except Exception:  # noqa: BLE001
                _LOGGER.warning("Could not update persisted boost state", exc_info=True)

    async def _async_drop_owed_restore(self) -> None:
        """Forget the owed restore (done, or no longer wanted) and persist that."""
        if self._owed_restore is None:
            return
        self._owed_restore = None
        await self._async_sync_persisted_boost()

    @callback
    def _drop_owed_restore(self) -> None:
        """Like _async_drop_owed_restore, for synchronous callers."""
        if self._owed_restore is None:
            return
        self._owed_restore = None
        self._hass.async_create_task(
            self._async_sync_persisted_boost(),
            "brink_ventilation_boost_persist",
        )

    async def async_remove_persisted_state(self) -> None:
        """Delete the persisted boost file (call when the config entry is removed)."""
        async with self._persist_lock:
            try:
                await self._store.async_remove()
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Could not remove persisted boost state", exc_info=True)

    async def async_options_updated(self) -> None:
        """Handle options flow update: restart humidity timer and re-evaluate season."""
        # Options updates also fire for the adaptive_active flag this controller
        # writes itself; only reset humidity tracking when the sensors changed.
        sensors = self.configured_humidity_sensors
        if sensors != self._monitored_humidity_sensors or (
            sensors and self._humidity_timer_unsub is None
        ):
            self._cancel_humidity_timer()
            self._humidity_previous.clear()
            self._humidity_rates.clear()
            self._start_humidity_timer()

        if self._state == AutomationState.IDLE:
            return

        # Re-evaluate season with potentially new threshold
        old_season = self._season
        self._season = self._evaluate_season()

        if old_season != self._season and self._season is not None:
            _LOGGER.info(
                "Season changed after options update: %s -> %s",
                old_season,
                self._season,
            )
            await self._async_reapply_level()

    async def async_cleanup(self) -> None:
        """Cancel all timers, clear in-memory state.

        The persisted boost is deliberately kept: unload / restart must resume it.
        """
        self._generation += 1
        self._cancel_pending_task()
        self._boost_pending = False
        self._cancel_boost_timer()
        self._cancel_countdown_timer()
        self._cancel_humidity_timer()
        self._humidity_previous.clear()
        self._humidity_rates.clear()
        self._clear_pending_writes()
        # In memory only: the persisted boost / owed restore must survive
        self._owed_restore = None
        self._pre_boost_snapshot = None
        self._last_restore_target = None
        self._last_restore_data_seq = -1
        self._boost_level = None
        self._wrote_since_data = False
        self._system_id = None
        self._boost_end_monotonic = 0.0
        self._clear_boost_trigger()
        self._last_level_correction = 0.0
        self._state = AutomationState.IDLE
        self._season = None

    # ------------------------------------------------------------------
    # Write queue
    # ------------------------------------------------------------------

    async def async_write_params(
        self,
        params: list[tuple[int, str]],
        restore_expected: tuple[int | str | None, int | str | None] | None = None,
        generation: int | None = None,
    ) -> None:
        """Write parameters to the API with retry-on-failure semantics.

        On success: clears pending_writes, starts expedited polling.
        On failure: stores params as pending_writes for retry on next refresh
        (except for definite credential errors, which are dropped).
        ``restore_expected`` marks a boost-restore write and carries the boost
        (mode, level) it undoes, kept with the queued write. ``generation`` is
        the caller's generation: if it was superseded while the write was in
        flight, a failure neither queues the write nor touches a newer queue.
        """
        system_id = self._get_system_id()
        if system_id is None:
            _LOGGER.warning("Cannot write: system_id unavailable")
            self._pending_writes = params
            self._pending_restore_expected = restore_expected
            return

        try:
            await self._coordinator.client.write_parameters(system_id, params)
        except Exception as ex:
            stale = generation is not None and generation != self._generation
            drop = isinstance(ex, BrinkAuthError) and ex.is_credentials_error
            if drop:
                _LOGGER.error(
                    "Write failed due to authentication error, not retrying: %s",
                    str(ex),
                )
            else:
                _LOGGER.warning(
                    "Write failed, %s: %s",
                    "superseded, not retrying" if stale else "queuing for retry",
                    type(ex).__name__,
                )
            entity_id = self._get_switch_entity_id()
            event_data: dict[str, Any] = {
                "entity_key": "automation_controller",
                "error": type(ex).__name__,
            }
            if entity_id is not None:
                event_data["entity_id"] = entity_id
            self._hass.bus.async_fire(EVENT_WRITE_FAILED, event_data)
            if stale:
                return
            if drop:
                # (an owed restore stays persisted: retried after reauth+reload)
                self._clear_pending_writes()
            else:
                self._pending_writes = params
                self._pending_restore_expected = restore_expected
            return

        stale = generation is not None and generation != self._generation
        if not stale:
            self._clear_pending_writes()
        self._wrote_since_data = True
        if restore_expected is not None:
            # (also covers a queued restore that finally succeeded after polls)
            self._last_restore_data_seq = self._data_seq
        self._coordinator.start_expedited_polling()
        _LOGGER.debug("Write succeeded: %s", params)
        if restore_expected is not None and not stale:
            await self._async_drop_owed_restore()

    def _clear_pending_writes(self) -> None:
        """Drop any queued write (and its restore marker)."""
        self._pending_writes = None
        self._pending_restore_expected = None

    async def _async_retry_pending_writes(self) -> None:
        """Retry pending writes if any exist."""
        if self._pending_writes is not None:
            _LOGGER.debug("Retrying pending writes: %s", self._pending_writes)
            await self.async_write_params(
                self._pending_writes,
                self._pending_restore_expected,
                generation=self._generation,
            )

    def _device_shows(
        self, expected: tuple[int | str | None, int | str | None]
    ) -> bool:
        """Return False only when the device is known to differ from expected (mode, level)."""
        actual = (
            self._normalize_param_value(self._get_param_value(PARAM_OPERATING_MODE)),
            self._normalize_param_value(self._get_param_value(PARAM_VENTILATION_LEVEL)),
        )
        for want, got in zip(expected, actual):
            if want is not None and got is not None and want != got:
                return False
        return True

    # ------------------------------------------------------------------
    # Boost timer callbacks
    # ------------------------------------------------------------------

    @callback
    def _async_boost_timer_expired(self, _now: Any) -> None:
        """Handle boost timer expiration."""
        self._boost_timer_unsub = None
        self._cancel_countdown_timer()

        if self._state == AutomationState.BOOSTED:
            # Fire deactivated event before clearing state
            self._fire_boost_deactivated_event("timer_expired")
            self._clear_boost_trigger()

            if self._was_in_base_before_boost:
                _LOGGER.info("Boost timer expired, transitioning to BASE")
                self._pre_boost_snapshot = None
                self._state = AutomationState.BASE

                self._cancel_pending_task()
                self._pending_task = self._hass.async_create_task(
                    self._safe_apply_seasonal_level(boosted=False),
                    "brink_ventilation_boost_return_to_base",
                )

                # Humidity monitoring resumes automatically since the timer
                # checks for BASE state — no need to restart it
            else:
                _LOGGER.info(
                    "Boost timer expired, transitioning to IDLE "
                    "(boost was triggered outside adaptive mode)"
                )
                snapshot = self._pre_boost_snapshot
                self._pre_boost_snapshot = None
                self._state = AutomationState.IDLE

                self._cancel_pending_task()
                self._pending_task = self._hass.async_create_task(
                    self._safe_restore_pre_boost(snapshot),
                    "brink_ventilation_boost_restore_previous",
                )

            # Notify entities so switch/sensor reflect new state
            self._coordinator.async_update_listeners()
            self._hass.async_create_task(
                self._async_sync_persisted_boost(),
                "brink_ventilation_boost_persist",
            )
        else:
            _LOGGER.debug("Boost timer expired but state is %s", self._state)

    # ------------------------------------------------------------------
    # Countdown timer (forces entity updates for remaining-time display)
    # ------------------------------------------------------------------

    def _start_countdown_timer(self) -> None:
        """Start a periodic 30-second countdown timer to refresh entity states."""
        self._cancel_countdown_timer()

        @callback
        def _countdown_tick(_now: Any) -> None:
            """Trigger an entity state update and reschedule."""
            if self._state != AutomationState.BOOSTED:
                self._countdown_timer_unsub = None
                return

            # Refresh entity states (remaining time) without touching polling
            self._coordinator.async_update_listeners()

            # Reschedule
            self._countdown_timer_unsub = async_call_later(
                self._hass, _COUNTDOWN_INTERVAL, _countdown_tick
            )

        self._countdown_timer_unsub = async_call_later(
            self._hass, _COUNTDOWN_INTERVAL, _countdown_tick
        )

    def _cancel_countdown_timer(self) -> None:
        """Cancel the countdown update timer if active."""
        if self._countdown_timer_unsub is not None:
            self._countdown_timer_unsub()
            self._countdown_timer_unsub = None

    # ------------------------------------------------------------------
    # Humidity monitoring (timer-based two-point comparison)
    # ------------------------------------------------------------------

    _HUMIDITY_CHECK_INTERVAL = 60  # seconds between humidity checks

    def _start_humidity_timer(self) -> None:
        """Start a periodic 60-second timer to check humidity sensors."""
        self._cancel_humidity_timer()
        sensors = self.configured_humidity_sensors
        self._monitored_humidity_sensors = list(sensors)
        if not sensors:
            _LOGGER.debug("No humidity sensors configured, skipping timer")
            return

        self._humidity_timer_unsub = async_call_later(
            self._hass, self._HUMIDITY_CHECK_INTERVAL, self._process_humidity_tick
        )
        _LOGGER.debug("Started humidity timer for: %s", sensors)

    @callback
    def _process_humidity_tick(self, _now: Any) -> None:
        """Run one humidity check; always reschedules, even on unexpected errors."""
        try:
            self._check_humidity_sensors()
        except Exception:
            _LOGGER.exception("Unexpected error in humidity monitoring tick")
        finally:
            self._humidity_timer_unsub = async_call_later(
                self._hass, self._HUMIDITY_CHECK_INTERVAL, self._process_humidity_tick
            )

    def _check_humidity_sensors(self) -> None:
        """Read each humidity sensor, compute rate, check for spikes."""
        now = time.monotonic()
        spike_entity: str | None = None
        spike_rate: float = 0.0

        for entity_id in self.configured_humidity_sensors:
            state = self._hass.states.get(entity_id)
            if state is None or state.state in (None, "", "unavailable", "unknown"):
                continue

            try:
                value = float(state.state)
            except (ValueError, TypeError):
                continue

            if math.isnan(value) or math.isinf(value):
                continue
            if value < 0.0 or value > 100.0:
                continue

            if entity_id not in self._humidity_previous:
                # First reading -- store and move on
                self._humidity_previous[entity_id] = (now, value)
                self._humidity_rates[entity_id] = 0.0
                continue

            old_time, old_value = self._humidity_previous[entity_id]
            elapsed_minutes = (now - old_time) / 60.0

            if elapsed_minutes > 0.0 and abs(value - old_value) > 0.009:
                rate = round((value - old_value) / elapsed_minutes, 1)
                rate = max(-50.0, min(50.0, rate))
                self._humidity_rates[entity_id] = rate

                # Track highest spike for boost trigger
                if rate > spike_rate:
                    spike_rate = rate
                    spike_entity = entity_id
            else:
                self._humidity_rates[entity_id] = 0.0

            # Always replace previous (keeps elapsed ~1 min for sensitivity)
            self._humidity_previous[entity_id] = (now, value)

        # Spike detection -- only trigger boost when in BASE state
        if (
            spike_entity is not None
            and self._state == AutomationState.BASE
            and not self._boost_pending
        ):
            threshold: float = float(
                self._entry.options.get(
                    CONF_HUMIDITY_SPIKE_THRESHOLD,
                    DEFAULT_HUMIDITY_SPIKE_THRESHOLD,
                )
            )
            if spike_rate >= threshold:
                _LOGGER.info(
                    "Humidity spike detected on %s: %.1f %%/min "
                    "(threshold %.1f %%/min)",
                    spike_entity,
                    spike_rate,
                    threshold,
                )
                self._boost_pending = True
                try:
                    self._cancel_pending_task()
                    self._pending_task = self._hass.async_create_task(
                        self._safe_activate_extra_ventilation(
                            trigger_entity=spike_entity,
                            trigger_rate=spike_rate,
                        ),
                        "brink_ventilation_humidity_boost",
                    )
                except Exception:
                    self._boost_pending = False
                    _LOGGER.exception("Failed to create humidity boost task")

        # Notify entities so delta sensors reflect new rates (must not use
        # async_set_updated_data: that would reset the coordinator's poll
        # schedule and mask API failures)
        self._coordinator.async_update_listeners()

    def _cancel_humidity_timer(self) -> None:
        """Cancel the humidity check timer if active."""
        if self._humidity_timer_unsub is not None:
            self._humidity_timer_unsub()
            self._humidity_timer_unsub = None

    # ------------------------------------------------------------------
    # Safe wrappers for fire-and-forget tasks
    # ------------------------------------------------------------------

    async def _safe_activate_extra_ventilation(
        self,
        trigger_entity: str | None = None,
        trigger_rate: float | None = None,
    ) -> None:
        """Activate extra ventilation with error logging for fire-and-forget tasks."""
        try:
            await self.async_activate_extra_ventilation(
                trigger=BOOST_TRIGGER_HUMIDITY if trigger_entity else None,
                trigger_entity=trigger_entity,
                trigger_rate=trigger_rate,
            )
        except Exception:
            _LOGGER.exception("Error activating extra ventilation from humidity spike")

    async def _safe_restore_pre_boost(
        self, snapshot: tuple[str, str | None] | None
    ) -> None:
        """Restore pre-boost state with error logging for fire-and-forget tasks."""
        try:
            await self._async_restore_pre_boost(snapshot)
        except Exception:
            _LOGGER.exception("Error restoring ventilation state after boost")

    # ------------------------------------------------------------------
    # Season evaluation
    # ------------------------------------------------------------------

    def _get_current_temperature(self) -> float | None:
        """Get the current temperature from configured source or coordinator data."""
        # Check for user-configured temperature source entity
        temp_entity_id: str = self._entry.options.get(
            CONF_TEMPERATURE_SOURCE_ENTITY, ""
        )
        if temp_entity_id:
            state = self._hass.states.get(temp_entity_id)
            if state is not None and state.state not in (
                None,
                "unavailable",
                "unknown",
            ):
                try:
                    value: float | None = float(state.state)
                except (ValueError, TypeError):
                    value = None
                if value is not None and math.isfinite(value):
                    unit = state.attributes.get("unit_of_measurement")
                    if unit in (None, UnitOfTemperature.CELSIUS):
                        return value
                    try:
                        converted = TemperatureConverter.convert(
                            value, unit, UnitOfTemperature.CELSIUS
                        )
                    except HomeAssistantError:
                        _LOGGER.debug(
                            "Temperature source %s has unsupported unit %s",
                            temp_entity_id,
                            unit,
                        )
                    else:
                        if math.isfinite(converted):
                            return converted

        # Fall back to PARAM_FRESH_AIR_TEMP from coordinator data
        raw = self._get_param_value(PARAM_FRESH_AIR_TEMP)
        if raw is not None:
            try:
                fallback = float(raw)
            except (ValueError, TypeError):
                return None
            if math.isfinite(fallback):
                return fallback

        return None

    def _evaluate_season(self) -> str | None:
        """Determine the current season based on temperature.

        Returns the current season unchanged if temperature is unavailable.
        """
        temperature = self._get_current_temperature()
        if temperature is None:
            return self._season

        threshold: float = float(
            self._entry.options.get(
                CONF_FREEZING_THRESHOLD, DEFAULT_FREEZING_THRESHOLD
            )
        )

        if temperature >= threshold:
            return SEASON_SUMMER
        return SEASON_WINTER

    # ------------------------------------------------------------------
    # Seasonal level helpers
    # ------------------------------------------------------------------

    def _get_seasonal_level(
        self,
        summer_key: str,
        winter_key: str,
        summer_default: int,
        winter_default: int,
    ) -> int:
        """Return the configured ventilation level for the current season, clamped to [0, 3]."""
        options = self._entry.options
        if self._season == SEASON_WINTER:
            level = int(options.get(winter_key, winter_default))
        else:
            level = int(options.get(summer_key, summer_default))
        return max(0, min(3, level))

    def _get_seasonal_base_level(self) -> int:
        """Return the configured base ventilation level for the current season."""
        return self._get_seasonal_level(
            CONF_AUTO_SUMMER_BASE_LEVEL,
            CONF_AUTO_WINTER_BASE_LEVEL,
            DEFAULT_AUTO_SUMMER_BASE_LEVEL,
            DEFAULT_AUTO_WINTER_BASE_LEVEL,
        )

    def _get_seasonal_max_level(self) -> int:
        """Return the configured max ventilation level for the current season."""
        return self._get_seasonal_level(
            CONF_EXTRA_VENT_SUMMER_LEVEL,
            CONF_EXTRA_VENT_WINTER_LEVEL,
            DEFAULT_EXTRA_VENT_SUMMER_LEVEL,
            DEFAULT_EXTRA_VENT_WINTER_LEVEL,
        )

    async def _async_apply_seasonal_level(
        self, boosted: bool = False, generation: int | None = None
    ) -> int:
        """Evaluate season and write the appropriate ventilation level. Returns the level used."""
        if generation is None:
            generation = self._generation
        self._season = self._evaluate_season()
        level = self._get_seasonal_max_level() if boosted else self._get_seasonal_base_level()
        params = self._build_mode_and_level_params(mode_value="1", level_value=str(level))
        if params:
            await self.async_write_params(params, generation=generation)
        return level

    async def _async_reapply_level(self) -> None:
        """Re-write the seasonal level for BASE / BOOSTED, tracking the boost level."""
        boosted = self._state == AutomationState.BOOSTED
        level = await self._async_apply_seasonal_level(boosted=boosted)
        if (
            boosted
            and self._state == AutomationState.BOOSTED
            and level != self._boost_level
        ):
            # The restore guard and the persisted boost must see the level now in force
            self._boost_level = level
            await self._async_sync_persisted_boost()

    async def _safe_apply_seasonal_level(self, boosted: bool = False) -> None:
        """Apply seasonal level with error logging for fire-and-forget tasks."""
        try:
            await self._async_apply_seasonal_level(boosted=boosted)
        except Exception:
            _LOGGER.exception("Error applying seasonal level from timer callback")

    # ------------------------------------------------------------------
    # Level watchdog
    # ------------------------------------------------------------------

    async def _verify_level(self) -> None:
        """Verify the ventilation level matches the expected state and correct if needed.

        Acts as a safety net: if a timer callback failed or a write was lost,
        this ensures the device converges to the correct level on the next
        coordinator poll.  Only runs in BASE or BOOSTED state with no pending
        writes, and respects a 5-minute cooldown between corrections.
        """
        if self._state == AutomationState.IDLE:
            return

        if self._pending_writes is not None:
            return

        if self._wrote_since_data:
            # A write in this pass is newer than the poll data we would compare
            _LOGGER.debug("Level watchdog: skipping, data predates a controller write")
            return

        # Read actual values from this controller's system, normalised so that
        # 1 / "1" / "1.0" compare equal
        actual_mode = self._normalize_param_value(
            self._get_param_value(PARAM_OPERATING_MODE)
        )
        actual_level = self._normalize_param_value(
            self._get_param_value(PARAM_VENTILATION_LEVEL)
        )

        if actual_mode is None or actual_level is None:
            _LOGGER.debug("Level watchdog: could not read current mode/level from data")
            return

        # Determine expected level based on current state
        if self._state == AutomationState.BOOSTED:
            expected_level = self._get_seasonal_max_level()
        else:
            expected_level = self._get_seasonal_base_level()

        expected_mode = 1  # manual

        if actual_mode == expected_mode and actual_level == expected_level:
            _LOGGER.debug(
                "Level watchdog: OK (mode=%s, level=%s, state=%s)",
                actual_mode,
                actual_level,
                self._state,
            )
            return

        # Mismatch detected -- check cooldown
        now = time.monotonic()
        if now - self._last_level_correction < 300:
            _LOGGER.debug(
                "Level watchdog: mismatch detected but cooldown active "
                "(actual mode=%s level=%s, expected mode=%s level=%s)",
                actual_mode,
                actual_level,
                expected_mode,
                expected_level,
            )
            return

        _LOGGER.warning(
            "Level watchdog: correcting mismatch "
            "(actual mode=%s level=%s, expected mode=%s level=%s, state=%s)",
            actual_mode,
            actual_level,
            expected_mode,
            expected_level,
            self._state,
        )
        self._last_level_correction = now
        await self._async_reapply_level()

    # ------------------------------------------------------------------
    # Parameter lookup helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_param_value(value: Any) -> int | str | None:
        """Normalise an API value so that 1, "1" and "1.0" compare equal."""
        if value is None:
            return None
        try:
            number = float(value)
        except (ValueError, TypeError):
            return str(value).strip()
        if math.isfinite(number) and number == int(number):
            return int(number)
        return str(value).strip()

    def _iter_system_params(self, param_id: str) -> list[dict[str, Any]]:
        """Return all entries for param_id in this controller's system only."""
        data = self._coordinator.data
        system_id = self._get_system_id()
        if not data or system_id is None:
            return []
        device = data.get(system_id) or {}
        found: list[dict[str, Any]] = []
        for component in device.get("components", []):
            param = component.get("parameters", {}).get(param_id)
            if param is not None:
                found.append(param)
        return found

    def _get_param_value(self, param_id: str) -> Any:
        """Return the first non-None value of param_id in this controller's system."""
        for param in self._iter_system_params(param_id):
            value = param.get("value")
            if value is not None:
                return value
        return None

    def _snapshot_mode_and_level(self) -> tuple[str, str | None] | None:
        """Capture the current operating mode and level (None if mode unknown).

        ``coordinator.data`` lags a restore write until the next poll; if a
        restore happened after the latest data (or is still queued), the
        restore target is what the unit actually has, not the data.
        """
        if self._last_restore_target is not None and (
            self._last_restore_data_seq == self._data_seq
            or self._pending_restore_expected is not None
        ):
            return self._last_restore_target
        mode = self._normalize_param_value(self._get_param_value(PARAM_OPERATING_MODE))
        if mode is None:
            return None
        level = self._normalize_param_value(
            self._get_param_value(PARAM_VENTILATION_LEVEL)
        )
        if (
            self._last_restore_target is not None
            and mode == 1
            and self._boost_level is not None
            and level == self._boost_level
        ):
            # Data still showing the last boost's level after a restore: a poll
            # that started before the restore write finished
            return self._last_restore_target
        return str(mode), (str(level) if level is not None else None)

    async def _async_restore_pre_boost(
        self,
        snapshot: tuple[str, str | None] | None,
        expected: tuple[int | str | None, int | str | None] | None = None,
    ) -> None:
        """Write the pre-boost mode (and level, in manual mode) back to the device.

        The restore stays owed (persisted) until the write succeeds or is
        dropped; ``expected`` is the boost (mode, level) it undoes, replayed
        only while the device still shows it.
        """
        generation = self._generation
        params: list[tuple[int, str]] = []
        if snapshot is None:
            _LOGGER.warning(
                "Boost ended but no pre-boost mode/level snapshot is available; "
                "leaving ventilation unchanged"
            )
        else:
            mode, level = snapshot
            if mode == "1" and level is not None:
                params = self._build_mode_and_level_params(
                    mode_value=mode, level_value=level
                )
            else:
                # Other modes: the device controls the level itself
                mode_vid = self._find_param_value_id(PARAM_OPERATING_MODE)
                if mode_vid is not None:
                    params = [(mode_vid, mode)]
            if not params:
                _LOGGER.warning(
                    "Cannot restore pre-boost state: parameter ids unavailable"
                )

        if snapshot is None or not params:
            # A queued boost write must not land after the boost ended
            self._clear_pending_writes()
            self._owed_restore = None
            await self._async_sync_persisted_boost()
            return

        mode, level = snapshot
        _LOGGER.info("Restoring pre-boost state: mode=%s level=%s", mode, level)
        if expected is None:
            boost_level = (
                self._boost_level
                if self._boost_level is not None
                else self._get_seasonal_max_level()
            )
            expected = (1, boost_level)
        self._last_restore_target = snapshot
        self._last_restore_data_seq = self._data_seq
        self._owed_restore = {
            "snapshot": snapshot,
            "expected": expected,
            "system_id": self._get_system_id(),
        }
        await self._async_sync_persisted_boost()
        if generation != self._generation:
            # The user (or adaptive mode) took over meanwhile
            return
        await self.async_write_params(
            params, restore_expected=expected, generation=generation
        )

    def _find_param_value_id(self, param_id: str) -> int | None:
        """Find the value_id for a parameter in this controller's system."""
        for param in self._iter_system_params(param_id):
            value_id = param.get("value_id")
            if value_id is not None:
                return int(value_id)
        return None

    def _build_mode_and_level_params(
        self,
        mode_value: str,
        level_value: str,
    ) -> list[tuple[int, str]]:
        """Build a param list for writing operating mode and ventilation level.

        Returns an empty list if the required value_ids cannot be found.
        """
        mode_vid = self._find_param_value_id(PARAM_OPERATING_MODE)
        level_vid = self._find_param_value_id(PARAM_VENTILATION_LEVEL)

        if mode_vid is None or level_vid is None:
            _LOGGER.warning(
                "Cannot build write params: mode_vid=%s, level_vid=%s",
                mode_vid,
                level_vid,
            )
            return []

        return [(mode_vid, mode_value), (level_vid, level_value)]

    def _get_system_id(self) -> int | None:
        """Return the controlled system_id, pinned to the first system seen.

        If the pinned system is missing from the current data (e.g. its fetch
        failed this poll) None is returned instead of switching to another
        system.
        """
        data = self._coordinator.data
        if not data:
            return None
        if self._system_id is None:
            self._system_id = next(iter(data), None)
            return self._system_id
        return self._system_id if self._system_id in data else None

    # ------------------------------------------------------------------
    # Boost trigger helpers
    # ------------------------------------------------------------------

    def _clear_boost_trigger(self) -> None:
        """Clear boost trigger info."""
        self._boost_trigger = None
        self._boost_trigger_entity = None
        self._boost_trigger_rate = None

    def _fire_boost_activated_event(
        self, level: int, duration_minutes: int
    ) -> None:
        """Fire a logbook event when extra ventilation boost starts."""
        event_data: dict[str, Any] = {
            "duration": duration_minutes,
            "level": level,
            "season": self._season,
        }
        entity_id = self._get_switch_entity_id()
        if entity_id is not None:
            event_data["entity_id"] = entity_id
        if self._boost_trigger == BOOST_TRIGGER_HUMIDITY:
            event_data["trigger"] = BOOST_TRIGGER_HUMIDITY
            if self._boost_trigger_entity:
                event_data["sensor"] = self._boost_trigger_entity
            if self._boost_trigger_rate is not None:
                event_data["rate"] = self._boost_trigger_rate
        self._hass.bus.async_fire(EVENT_BOOST_ACTIVATED, event_data)

    def _fire_boost_deactivated_event(self, reason: str) -> None:
        """Fire a logbook event when extra ventilation boost ends."""
        event_data: dict[str, Any] = {
            "reason": reason,
        }
        entity_id = self._get_switch_entity_id()
        if entity_id is not None:
            event_data["entity_id"] = entity_id
        if self._boost_trigger == BOOST_TRIGGER_HUMIDITY:
            event_data["trigger"] = BOOST_TRIGGER_HUMIDITY
        self._hass.bus.async_fire(EVENT_BOOST_DEACTIVATED, event_data)

    def _get_switch_entity_id(self) -> str | None:
        """Get the extra ventilation switch entity_id from the registry."""
        system_id = self._get_system_id()
        if system_id is None:
            return None
        registry = er.async_get(self._hass)
        unique_id = f"{DOMAIN}_{system_id}_extra_ventilation"
        return registry.async_get_entity_id("switch", DOMAIN, unique_id)

    # ------------------------------------------------------------------
    # Timer and task cancellation helpers
    # ------------------------------------------------------------------

    def _cancel_pending_task(self) -> None:
        """Cancel the pending async task if running."""
        if self._pending_task and not self._pending_task.done():
            self._pending_task.cancel()
        self._pending_task = None

    def _cancel_stale_pending_task(self) -> None:
        """Cancel an in-flight restore / return-to-base task, never the running one.

        The humidity-triggered boost runs *as* ``_pending_task``; cancelling
        it from inside would abort the boost itself.
        """
        if self._pending_task is not None and (
            self._pending_task is asyncio.current_task()
        ):
            return
        self._cancel_pending_task()

    def _cancel_boost_timer(self) -> None:
        """Cancel the boost timer if active."""
        if self._boost_timer_unsub is not None:
            self._boost_timer_unsub()
            self._boost_timer_unsub = None
