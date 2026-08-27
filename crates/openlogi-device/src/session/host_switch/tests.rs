//! Host-switch capture and transition regressions.

use std::sync::Arc;

use hidpp::{channel::HidppChannel, device::DeviceError, nibble::U4, protocol::v20};

use super::{
    ArmedControl, HostSwitchError, HostSwitchRequest, KeyboardHostTransition, ReportingMode,
    change_host_announcement, host_change_required, host_control_request, prepare_host_change_on,
    resolve_change_host_feature_index, restoration_change, shares_channel, switch_linked_hosts,
};
use crate::channel::scripted::{
    ScriptedBackend, ScriptedNode, ScriptedRawHidChannel, ScriptedRawHidHandle, feature_error,
    scripted_node_info,
};
use crate::reprog_controls::{
    self, AnalyticsKeyEvent, CidReporting, ControlId, CtrlIdInfo, ReprogControlsEvent,
};
use crate::{ChannelPool, DeviceRoute, backend::BackendError};

/// Feature index the scripted keyboard reports for `0x1814 ChangeHost`.
const CHANGE_HOST_INDEX: u8 = 0x04;
/// Feature index the scripted keyboard reports for `0x1815 HostsInfo`.
const HOSTS_INFO_INDEX: u8 = 0x05;

/// `ErrorType::Busy`, the failure a scripted device answers with.
const BUSY: u8 = 0x08;

/// What the scripted keyboard's firmware does when asked about `0x1815`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotStatus {
    /// Answers the query: hosts 0 and 1 paired, host 2 empty.
    Reported,
    /// Reports the feature as unimplemented, the usual index-0 lookup miss.
    Unimplemented,
    /// Errors on the lookup itself, as firmware that refuses unknown
    /// feature ids rather than reporting index 0 does.
    LookupErrors,
    /// Implements the feature but errors on the status read.
    ReadErrors,
}

/// A three-channel keyboard currently on host 0, paired on hosts 0 and 1
/// but **not** on host 2 — a keyboard with three host keys and only two
/// machines paired, which is the shape that used to strand devices.
fn keyboard_with_an_empty_third_slot(request: &[u8]) -> Option<Vec<u8>> {
    scripted_keyboard(request, SlotStatus::Reported)
}

/// The same keyboard without `0x1815`, so its slot pairing is unknowable.
fn keyboard_without_hosts_info(request: &[u8]) -> Option<Vec<u8>> {
    scripted_keyboard(request, SlotStatus::Unimplemented)
}

/// The same keyboard, whose firmware errors when asked for `0x1815`.
fn keyboard_erroring_on_hosts_info_lookup(request: &[u8]) -> Option<Vec<u8>> {
    scripted_keyboard(request, SlotStatus::LookupErrors)
}

/// The same keyboard, whose `0x1815` reads come back an error.
fn keyboard_erroring_on_slot_status(request: &[u8]) -> Option<Vec<u8>> {
    scripted_keyboard(request, SlotStatus::ReadErrors)
}

fn scripted_keyboard(request: &[u8], slot_status: SlotStatus) -> Option<Vec<u8>> {
    if request.len() < 7 || !matches!(request[0], 0x10 | 0x11) {
        return None;
    }
    let mut payload = [0u8; 16];
    match (request[2], request[3] >> 4) {
        // Root ping used by Device::new.
        (0x00, 0x01) => payload[0] = 4,
        // Root feature lookup.
        (0x00, 0x00) => {
            payload[0] = match u16::from_be_bytes([request[4], request[5]]) {
                0x1814 => CHANGE_HOST_INDEX,
                0x1815 => match slot_status {
                    SlotStatus::Unimplemented => 0x00,
                    SlotStatus::LookupErrors => return Some(feature_error(request, BUSY)),
                    SlotStatus::Reported | SlotStatus::ReadErrors => HOSTS_INFO_INDEX,
                },
                _ => 0x00,
            };
        }
        // ChangeHost getHostInfo: three RF channels, currently on host 0.
        (CHANGE_HOST_INDEX, 0x00) => payload[..2].copy_from_slice(&[3, 0]),
        // HostsInfo getHostInfo: echo the slot, then its pairing status.
        (HOSTS_INFO_INDEX, 0x01) => {
            if slot_status == SlotStatus::ReadErrors {
                return Some(feature_error(request, BUSY));
            }
            payload[0] = request[4];
            payload[1] = u8::from(request[4] < 2);
        }
        _ => return None,
    }

    let mut response = vec![0u8; 7];
    response[0] = 0x10;
    response[1..4].copy_from_slice(&request[1..4]);
    response[4..].copy_from_slice(&payload[..3]);
    Some(response)
}

async fn scripted_channel(responder: crate::channel::scripted::Responder) -> Arc<HidppChannel> {
    let (raw, _handle) = ScriptedRawHidChannel::with_responder(responder);
    crate::channel::scripted::scripted_channel(raw).await
}

async fn scripted_live_node(
    id: &str,
    product_id: u16,
    responder: crate::channel::scripted::Responder,
) -> (crate::backend::NodeInfo, ScriptedNode, ScriptedRawHidHandle) {
    let (raw, handle) = ScriptedRawHidChannel::with_responder(responder);
    let channel = crate::channel::scripted::scripted_channel(raw).await;
    let mut node = scripted_node_info(id);
    node.product_id = product_id;
    (node, ScriptedNode::Live(channel), handle)
}

fn sent_host_change(handle: &ScriptedRawHidHandle, host: u8) -> bool {
    handle.written_reports().iter().any(|report| {
        report.get(2) == Some(&CHANGE_HOST_INDEX)
            && report.get(3).is_some_and(|function| function >> 4 == 1)
            && report.get(4) == Some(&host)
    })
}

#[tokio::test]
async fn switching_to_an_unpaired_slot_is_refused() {
    // ChangeHost would allow it: host 2 is within the device's channel
    // count. But nothing is paired there, and `setCurrentHost` is
    // fire-and-forget — the device would simply leave and not come back.
    let channel = scripted_channel(keyboard_with_an_empty_third_slot).await;

    let Err(error) = prepare_host_change_on(&channel, 1, 2).await else {
        panic!("an unpaired slot must not be switched to");
    };

    assert!(
        matches!(error, HostSwitchError::HostSlotEmpty { host: 2 }),
        "got {error:?}"
    );
}

#[tokio::test]
async fn switching_to_a_paired_slot_proceeds() {
    let channel = scripted_channel(keyboard_with_an_empty_third_slot).await;

    let change = prepare_host_change_on(&channel, 1, 1)
        .await
        .expect("a paired slot must be switchable");

    assert!(change.required, "host 1 differs from the current host 0");
}

#[tokio::test]
async fn announcement_capture_resolves_the_current_runtime_feature_index() {
    let channel = scripted_channel(keyboard_without_hosts_info).await;

    assert_eq!(
        resolve_change_host_feature_index(&channel, 1)
            .await
            .expect("ChangeHost feature"),
        CHANGE_HOST_INDEX
    );
}

#[tokio::test]
async fn a_departed_keyboard_still_moves_a_reachable_target() {
    const KEYBOARD_PID: u16 = 0xb369;
    const TARGET_PID: u16 = 0xb042;

    let mut keyboard_node = scripted_node_info("departed-keyboard");
    keyboard_node.product_id = KEYBOARD_PID;
    let (target_node, target, target_handle) =
        scripted_live_node("reachable-target", TARGET_PID, keyboard_without_hosts_info).await;
    let pool = ChannelPool::with_backend(ScriptedBackend::new(vec![
        (keyboard_node, ScriptedNode::OpenFails),
        (target_node, target),
    ]));
    let keyboard = DeviceRoute::Direct {
        vendor_id: crate::LOGITECH_VENDOR_ID,
        product_id: KEYBOARD_PID,
    };
    let target = DeviceRoute::Direct {
        vendor_id: crate::LOGITECH_VENDOR_ID,
        product_id: TARGET_PID,
    };

    assert!(
        switch_linked_hosts(
            &keyboard,
            &[target],
            1,
            KeyboardHostTransition::CommandRequired,
            &pool,
        )
        .await
        .expect("a departed analytics-only keyboard should use target-only fallback")
    );
    assert!(
        sent_host_change(&target_handle, 1),
        "the reachable target never received set_current_host(1)"
    );
}

#[tokio::test]
async fn an_empty_keyboard_slot_never_moves_a_target() {
    const KEYBOARD_PID: u16 = 0xb369;
    const TARGET_PID: u16 = 0xb042;

    let (keyboard_node, keyboard, _keyboard_handle) = scripted_live_node(
        "keyboard-with-empty-slot",
        KEYBOARD_PID,
        keyboard_with_an_empty_third_slot,
    )
    .await;
    let (target_node, target, target_handle) =
        scripted_live_node("reachable-target", TARGET_PID, keyboard_without_hosts_info).await;
    let pool = ChannelPool::with_backend(ScriptedBackend::new(vec![
        (keyboard_node, keyboard),
        (target_node, target),
    ]));
    let keyboard = DeviceRoute::Direct {
        vendor_id: crate::LOGITECH_VENDOR_ID,
        product_id: KEYBOARD_PID,
    };
    let target = DeviceRoute::Direct {
        vendor_id: crate::LOGITECH_VENDOR_ID,
        product_id: TARGET_PID,
    };

    let analytics_request = analytics_request(2);
    let error = switch_linked_hosts(
        &keyboard,
        &[target],
        2,
        analytics_request.keyboard_transition,
        &pool,
    )
    .await
    .expect_err("the keyboard has no pairing in host slot 2");
    assert!(matches!(error, HostSwitchError::HostSlotEmpty { host: 2 }));
    assert!(
        !sent_host_change(&target_handle, 2),
        "the target moved even though the keyboard rejected the host slot"
    );
}

#[tokio::test]
async fn a_departure_announcement_moves_the_target_without_probing_the_keyboard() {
    const KEYBOARD_PID: u16 = 0xb369;
    const TARGET_PID: u16 = 0xb042;

    let (keyboard_node, keyboard, _keyboard_handle) = scripted_live_node(
        "keyboard-with-empty-slot",
        KEYBOARD_PID,
        keyboard_with_an_empty_third_slot,
    )
    .await;
    let (target_node, target, target_handle) =
        scripted_live_node("reachable-target", TARGET_PID, keyboard_without_hosts_info).await;
    let pool = ChannelPool::with_backend(ScriptedBackend::new(vec![
        (keyboard_node, keyboard),
        (target_node, target),
    ]));
    let keyboard = DeviceRoute::Direct {
        vendor_id: crate::LOGITECH_VENDOR_ID,
        product_id: KEYBOARD_PID,
    };
    let target = DeviceRoute::Direct {
        vendor_id: crate::LOGITECH_VENDOR_ID,
        product_id: TARGET_PID,
    };

    assert!(
        switch_linked_hosts(
            &keyboard,
            &[target],
            2,
            KeyboardHostTransition::AlreadyDeparting,
            &pool,
        )
        .await
        .expect("the announcement proves the keyboard accepted its own host change")
    );
    assert!(
        sent_host_change(&target_handle, 2),
        "the target was delayed behind an unnecessary keyboard probe"
    );
}

#[tokio::test]
async fn a_device_without_hosts_info_is_still_switched() {
    // 0x1815 is the only source of per-slot pairing status. Without it the
    // guard must not block, or this change would regress every device that
    // does not implement it.
    let channel = scripted_channel(keyboard_without_hosts_info).await;

    let change = prepare_host_change_on(&channel, 1, 2)
        .await
        .expect("a device that cannot report slot status must still switch");

    assert!(change.required);
}

#[tokio::test]
async fn a_failed_hosts_info_lookup_does_not_block_the_switch() {
    // Firmware that answers an unknown feature id with an error rather than
    // index 0 must read the same as not implementing 0x1815 at all: the
    // pairing status is unknowable, which is not a reason to refuse.
    let channel = scripted_channel(keyboard_erroring_on_hosts_info_lookup).await;

    let change = prepare_host_change_on(&channel, 1, 2)
        .await
        .expect("an errored feature lookup must not abort the switch");

    assert!(change.required);
}

#[tokio::test]
async fn an_unreadable_slot_status_does_not_block_the_switch() {
    // The guard is advisory. A device that has 0x1815 but cannot answer for
    // it right now has not said the slot is empty, so refusing here would
    // turn a transient read failure into a dead host key.
    let channel = scripted_channel(keyboard_erroring_on_slot_status).await;

    let change = prepare_host_change_on(&channel, 1, 2)
        .await
        .expect("an errored status read must not abort the switch");

    assert!(change.required);
}

#[tokio::test]
async fn a_switch_to_the_current_host_never_consults_slot_status() {
    // Already-there is decided before the pairing check, so a device on an
    // unpaired-looking slot is not blocked from staying put.
    let channel = scripted_channel(keyboard_with_an_empty_third_slot).await;

    let change = prepare_host_change_on(&channel, 1, 0)
        .await
        .expect("staying on the current host is always fine");

    assert!(!change.required);
}

/// A reporting snapshot with unrelated bits deliberately set, so the
/// cleanup tests prove that only the bits they vary are restored.
fn noisy_reporting() -> CidReporting {
    CidReporting {
        cid: ControlId(0x00d3),
        diverted: false,
        persistently_diverted: true,
        force_raw_xy: true,
        raw_xy: false,
        remap: Some(ControlId(0x1234)),
        analytics_key_events: false,
        raw_wheel: true,
    }
}

fn analytics_request(host: u8) -> HostSwitchRequest {
    let cid = 0x00d1 + u16::from(host);
    let controls = [ArmedControl {
        cid,
        host,
        mode: ReportingMode::Analytics,
        original: noisy_reporting(),
    }];
    let mut events = [AnalyticsKeyEvent::default(); 5];
    events[0] = AnalyticsKeyEvent {
        cid: ControlId(cid),
        event: 1,
    };
    host_control_request(&controls, ReprogControlsEvent::AnalyticsKeyEvents(events))
        .expect("analytics host press")
        .0
}

#[test]
fn receiver_slots_share_one_channel() {
    let keyboard = DeviceRoute::Bolt {
        receiver_uid: "AABB".into(),
        slot: 1,
    };
    let mouse = DeviceRoute::Bolt {
        receiver_uid: "aabb".into(),
        slot: 2,
    };
    assert!(shares_channel(&keyboard, &mouse));
}

#[test]
fn direct_devices_do_not_share_channels() {
    let route = DeviceRoute::Direct {
        vendor_id: 0x046d,
        product_id: 0xb025,
    };
    assert!(!shares_channel(&route, &route));
}

#[test]
fn host_controls_are_recognized_by_task_when_cid_varies() {
    let info = CtrlIdInfo {
        cid: 0x1234,
        task_id: 0x00af,
        flags: 0,
    };
    assert_eq!(reprog_controls::host_switch_channel(info), Some(1));
}

#[test]
fn analytics_event_requires_keyboard_validation() {
    let controls = [ArmedControl {
        cid: 0x00d3,
        host: 2,
        mode: ReportingMode::Analytics,
        original: noisy_reporting(),
    }];
    let mut events = [AnalyticsKeyEvent::default(); 5];
    events[0] = AnalyticsKeyEvent {
        cid: ControlId(0x00d3),
        event: 1,
    };
    let (request, restore_controls) =
        host_control_request(&controls, ReprogControlsEvent::AnalyticsKeyEvents(events))
            .expect("analytics host press");

    assert_eq!(request.host, 2);
    assert_eq!(
        request.keyboard_transition,
        KeyboardHostTransition::CommandRequired,
        "only a ChangeHost announcement proves that firmware is already departing"
    );
    assert!(!restore_controls);
}

#[test]
fn change_host_announcement_selects_the_reported_destination() {
    let mut payload = [0; 16];
    payload[0] = 0;
    payload[1] = 1;
    let announcement = v20::Message::Long(
        v20::MessageHeader {
            device_index: 1,
            feature_index: 9,
            function_id: U4::from_lo(0),
            software_id: U4::from_lo(0),
        },
        payload,
    );

    assert_eq!(change_host_announcement(&announcement, 1, 9, 3), Some(1));
}

#[test]
fn change_host_announcement_rejects_foreign_or_malformed_messages() {
    let announcement = |device_index, feature_index, function_id, software_id, target_host| {
        let mut payload = [0; 16];
        payload[1] = target_host;
        v20::Message::Long(
            v20::MessageHeader {
                device_index,
                feature_index,
                function_id: U4::from_lo(function_id),
                software_id: U4::from_lo(software_id),
            },
            payload,
        )
    };

    for message in [
        announcement(2, 9, 0, 0, 1),
        announcement(1, 8, 0, 0, 1),
        announcement(1, 9, 1, 0, 1),
        announcement(1, 9, 0, 1, 1),
        announcement(1, 9, 0, 0, 3),
    ] {
        assert_eq!(change_host_announcement(&message, 1, 9, 3), None);
    }
}

#[test]
fn current_host_does_not_require_a_change() {
    assert!(matches!(host_change_required(1, 3, 1), Ok(false)));
}

#[test]
fn different_valid_host_requires_a_change() {
    assert!(matches!(host_change_required(0, 3, 2), Ok(true)));
}

#[test]
fn host_outside_device_range_is_rejected() {
    assert!(
        host_change_required(0, 2, 2).is_err(),
        "host 2 is outside a device that reports 2 hosts and must be rejected"
    );
}

#[test]
fn only_departure_errors_enable_target_only_switching() {
    assert!(HostSwitchError::KeyboardNotFound.is_device_unreachable());
    assert!(HostSwitchError::Hid(BackendError::Disconnected).is_device_unreachable());
    assert!(HostSwitchError::Device(DeviceError::DeviceNotFound).is_device_unreachable());
    assert!(
        !HostSwitchError::Hidpp("DeviceNotFound".into()).is_device_unreachable(),
        "formatted debug text must not classify a departure"
    );

    assert!(
        !HostSwitchError::HostSlotEmpty { host: 2 }.is_device_unreachable(),
        "an unpaired slot must never move targets without the keyboard"
    );
    assert!(
        !HostSwitchError::TimedOut {
            operation: "probing"
        }
        .is_device_unreachable()
    );
    assert!(
        !HostSwitchError::Hid(BackendError::Backend("permission denied".into()))
            .is_device_unreachable()
    );
}

#[test]
fn diverted_cleanup_restores_only_the_original_temporary_bits() {
    let change = restoration_change(ArmedControl {
        cid: 0x00d3,
        host: 2,
        mode: ReportingMode::Diverted,
        original: CidReporting {
            diverted: true,
            raw_xy: true,
            ..noisy_reporting()
        },
    });

    assert_eq!(change.diverted, Some(true));
    assert_eq!(change.raw_xy, Some(true));
    assert_eq!(change.analytics_key_events, None);
    assert_eq!(change.persistently_diverted, None);
    assert_eq!(change.remap, None);
}

#[test]
fn analytics_cleanup_restores_the_original_analytics_bit() {
    let change = restoration_change(ArmedControl {
        cid: 0x00d3,
        host: 2,
        mode: ReportingMode::Analytics,
        original: CidReporting {
            analytics_key_events: true,
            ..noisy_reporting()
        },
    });

    assert_eq!(change.analytics_key_events, Some(true));
    assert_eq!(change.diverted, None);
    assert_eq!(change.raw_xy, None);
}
