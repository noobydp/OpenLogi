//! Validation and application of linked host transitions.

use std::sync::Arc;

use hidpp::{
    channel::HidppChannel,
    device::Device,
    feature::{
        CreatableFeature,
        change_host::ChangeHostFeature,
        hosts_info::{HostIndex, HostSlotStatus, HostsInfoFeature},
    },
};
use tracing::debug;

use crate::{ChannelPool, DeviceRoute};

use super::{HostSwitchError, KeyboardHostTransition, open_channel, timed_device, timed_hidpp};
/// Move reachable targets to `host`, then move the keyboard last.
///
/// Returns whether the keyboard actually changed hosts, or had already
/// departed after an analytics-only host event.
pub async fn switch_linked_hosts(
    keyboard: &DeviceRoute,
    targets: &[DeviceRoute],
    host: u8,
    keyboard_transition: KeyboardHostTransition,
    channel_pool: &ChannelPool,
) -> Result<bool, HostSwitchError> {
    if keyboard_transition == KeyboardHostTransition::AlreadyDeparting {
        debug!(route = %keyboard, host, "keyboard departure announced; switching targets immediately");
        switch_targets_directly(targets, host, channel_pool).await;
        return Ok(true);
    }
    let channel = match open_channel(channel_pool, keyboard, "opening keyboard channel").await {
        Ok(Some(channel)) => channel,
        Ok(None) => {
            debug!(route = %keyboard, host, "keyboard already departed; switching targets only");
            switch_targets_directly(targets, host, channel_pool).await;
            return Ok(true);
        }
        Err(error) if error.is_device_unreachable() => {
            debug!(%error, route = %keyboard, host, "keyboard departed while opening channel; switching targets only");
            switch_targets_directly(targets, host, channel_pool).await;
            return Ok(true);
        }
        Err(error) => return Err(error),
    };
    // Validate the keyboard's own move before touching anything: preparation is
    // read-only, but it is the step that rejects an unpaired host slot, and
    // discovering that *after* the mice have moved would strand them on a host
    // the keyboard never reaches. Applying it still happens last, because once
    // the keyboard leaves this host its channel can no longer command a mouse
    // sharing the same receiver.
    let keyboard_change = match prepare_host_change_on(&channel, keyboard.device_index(), host)
        .await
    {
        Ok(change) => change,
        Err(error) if error.is_device_unreachable() => {
            debug!(%error, route = %keyboard, host, "keyboard unreachable; switching targets only");
            switch_targets_directly(targets, host, channel_pool).await;
            return Ok(true);
        }
        Err(error) => return Err(error),
    };
    for target in targets {
        match prepare_host_change(target, host, keyboard, &channel, channel_pool).await {
            Ok(change) => {
                if let Err(error) = apply_host_change(change).await {
                    debug!(%error, route = %target, host, "linked device host switch failed");
                }
            }
            Err(error) => {
                debug!(%error, route = %target, host, "linked device host switch preparation failed");
            }
        }
    }
    let changed = apply_host_change(keyboard_change).await?;
    if changed {
        debug!(host, route = %keyboard, "keyboard host switched");
    }
    Ok(changed)
}

/// Switch targets through their own channels after the initiating keyboard
/// has already left this host.
async fn switch_targets_directly(targets: &[DeviceRoute], host: u8, channel_pool: &ChannelPool) {
    for target in targets {
        let channel = match open_channel(channel_pool, target, "opening target channel").await {
            Ok(Some(channel)) => channel,
            Ok(None) => {
                debug!(route = %target, host, "target not reachable for direct switch");
                continue;
            }
            Err(error) => {
                debug!(%error, route = %target, host, "target channel open failed");
                continue;
            }
        };
        match prepare_host_change_on(&channel, target.device_index(), host).await {
            Ok(change) => {
                if let Err(error) = apply_host_change(change).await {
                    debug!(%error, route = %target, host, "direct target host switch failed");
                }
            }
            Err(error) => {
                debug!(%error, route = %target, host, "direct target host switch preparation failed");
            }
        }
    }
}

pub(super) struct PreparedHostChange {
    feature: Arc<ChangeHostFeature>,
    device_index: u8,
    host: u8,
    pub(super) required: bool,
}

async fn prepare_host_change(
    target: &DeviceRoute,
    host: u8,
    keyboard: &DeviceRoute,
    keyboard_channel: &Arc<HidppChannel>,
    channel_pool: &ChannelPool,
) -> Result<PreparedHostChange, HostSwitchError> {
    if shares_channel(target, keyboard) {
        prepare_host_change_on(keyboard_channel, target.device_index(), host).await
    } else {
        let channel = open_channel(channel_pool, target, "opening linked device channel")
            .await?
            .ok_or(HostSwitchError::TargetNotFound)?;
        prepare_host_change_on(&channel, target.device_index(), host).await
    }
}

pub(super) async fn prepare_host_change_on(
    channel: &Arc<HidppChannel>,
    device_index: u8,
    host: u8,
) -> Result<PreparedHostChange, HostSwitchError> {
    let mut device = timed_device(
        "opening host-change device",
        Device::new(Arc::clone(channel), device_index),
    )
    .await?;
    let info = timed_hidpp(
        "locating host-change feature",
        device.root().get_feature(ChangeHostFeature::ID),
    )
    .await?
    .ok_or_else(|| HostSwitchError::Hidpp("ChangeHost is unsupported".into()))?;
    let change_host = device.add_feature::<ChangeHostFeature>(info.index);
    let state = timed_hidpp("reading current host", change_host.get_host_info()).await?;
    let required = host_change_required(state.current_host, state.host_count, host)?;
    if required && host_slot_is_empty(&mut device, host).await {
        return Err(HostSwitchError::HostSlotEmpty { host });
    }
    Ok(PreparedHostChange {
        feature: change_host,
        device_index,
        host,
        required,
    })
}

async fn apply_host_change(change: PreparedHostChange) -> Result<bool, HostSwitchError> {
    if !change.required {
        let PreparedHostChange {
            device_index, host, ..
        } = change;
        debug!(device_index, host, "device already uses requested host");
        return Ok(false);
    }
    timed_hidpp(
        "writing current host",
        change.feature.set_current_host(change.host),
    )
    .await?;
    Ok(true)
}

/// Whether the device explicitly reports `host` as an empty slot.
///
/// `ChangeHost`'s `host_count` counts the device's RF channels, not the ones
/// that have a pairing. Switching to an empty slot is not refused by the
/// device: `setCurrentHost` is fire-and-forget and a successful switch usually
/// resets the device, so it simply drops off this host and does not come back
/// until the user pairs that slot or presses the device's own host button. A
/// keyboard with three host keys paired to two machines is enough to hit this.
///
/// `HostsInfo` (`0x1815`) is the only feature that reports per-slot pairing
/// status, and asking is advisory: a device that does not implement it, times
/// out, returns a feature error, or answers with a status byte outside the
/// spec has not said the slot is empty, and must still be allowed to switch.
/// Only an explicit `Empty` refuses, so this returns a plain `bool` — an
/// unreadable status can never abort the transition it was meant to protect.
async fn host_slot_is_empty(device: &mut Device, host: u8) -> bool {
    let feature = timed_hidpp(
        "locating hosts-info feature",
        device.root().get_feature(HostsInfoFeature::ID),
    )
    .await;
    let index = match feature {
        Ok(Some(info)) => info.index,
        Ok(None) => return false,
        Err(error) => {
            debug!(host, %error, "hosts-info lookup failed; treating the slot as usable");
            return false;
        }
    };
    let hosts_info = device.add_feature::<HostsInfoFeature>(index);
    match timed_hidpp(
        "reading host slot status",
        hosts_info.get_host_info(HostIndex::Slot(host)),
    )
    .await
    {
        Ok(slot) => slot.status == HostSlotStatus::Empty,
        Err(error) => {
            debug!(host, %error, "host slot status is unreadable; treating the slot as usable");
            false
        }
    }
}

pub(super) fn host_change_required(
    current_host: u8,
    host_count: u8,
    requested_host: u8,
) -> Result<bool, HostSwitchError> {
    if requested_host >= host_count {
        return Err(HostSwitchError::Hidpp(format!(
            "host {requested_host} is outside device host count {host_count}"
        )));
    }
    Ok(current_host != requested_host)
}

pub(super) fn shares_channel(left: &DeviceRoute, right: &DeviceRoute) -> bool {
    left.shares_transport(right)
}
