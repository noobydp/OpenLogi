//! Keep configured keyboard → linked-device host-switch relationships armed.

use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use openlogi_hid::{
    ChannelPool, DeviceRoute, HostSwitchCaptureMode, HostSwitchRequest, HostSwitchStopReason,
    KeyboardHostTransition, run_host_switch_session, switch_linked_hosts,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::receiver_access::{ExclusiveAccessReason, ReceiverAccess};

const DEPARTURE_TIMEOUT: Duration = Duration::from_secs(10);
const DEPARTURE_POLL: Duration = Duration::from_millis(100);

/// One resolved link. Config keys are converted to live routes by the
/// orchestrator so the transport watcher never needs to understand inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostSwitchLink {
    /// Stable physical configuration identity of the initiating keyboard.
    pub keyboard_key: String,
    /// Keyboard whose host switch keys initiate the transition.
    pub keyboard: DeviceRoute,
    /// Devices that follow the keyboard.
    pub targets: Vec<DeviceRoute>,
}

/// Shared resolved links, refreshed with config and inventory.
pub type HostSwitchLinks = Arc<RwLock<Vec<HostSwitchLink>>>;

/// Spawn the host switch session manager.
pub fn spawn(links: HostSwitchLinks, channel_pool: ChannelPool, receiver_access: ReceiverAccess) {
    thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                warn!(%error, "host switch watcher: could not build tokio runtime");
                return;
            }
        };
        runtime.block_on(manage(links, channel_pool, receiver_access));
    });
}

async fn manage(
    links: HostSwitchLinks,
    channel_pool: ChannelPool,
    receiver_access: ReceiverAccess,
) {
    let mut sessions = Vec::new();
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<SessionCompletion>();
    let mut next_generation = 0_u64;
    let mut announcement_keyboards = Vec::<String>::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let wanted = if receiver_access.exclusive_requested() {
                    Vec::new()
                } else {
                    links.read().map_or_else(|_| Vec::new(), |guard| guard.clone())
                };
                stop_unwanted(&mut sessions, &wanted).await;
                for link in wanted {
                    if sessions.iter().any(|session| session.link == link) {
                        continue;
                    }
                    let (stop_tx, stop_rx) = oneshot::channel();
                    let done = done_tx.clone();
                    let pool = channel_pool.clone();
                    let Some(receiver_lease) = receiver_access.try_acquire_for_session() else {
                        break;
                    };
                    next_generation = next_generation.wrapping_add(1);
                    let session_generation = next_generation;
                    let session_link = link.clone();
                    let capture_mode =
                        capture_mode_for(&announcement_keyboards, &link.keyboard_key);
                    debug!(
                        generation = session_generation,
                        route = %session_link.keyboard,
                        targets = session_link.targets.len(),
                        "starting host switch session"
                    );
                    let task = tokio::spawn(async move {
                        let _receiver_lease = receiver_lease;
                        let keyboard = link.keyboard.clone();
                        let request = match run_host_switch_session(
                            link.keyboard.clone(),
                            stop_rx,
                            pool,
                            capture_mode,
                        )
                        .await
                        {
                            Ok(request) => request.map(|request| (link, request)),
                            Err(error) => {
                                debug!(%error, route = %keyboard, "host switch session ended");
                                None
                            }
                        };
                        let _ = done.send(SessionCompletion {
                            generation: session_generation,
                            request,
                        });
                    });
                    sessions.push(RunningSession {
                        link: session_link,
                        generation: session_generation,
                        stop: stop_tx,
                        task,
                    });
                }
            }
            Some(completion) = done_rx.recv() => {
                if let Some(index) = sessions
                    .iter()
                    .position(|session| session.generation == completion.generation)
                {
                    let completed = sessions.remove(index);
                    let _ = completed.task.await;
                    if let Some((link, request)) = completion.request {
                        if request.announcement_observed {
                            remember_announcement_keyboard(
                                &mut announcement_keyboards,
                                link.keyboard_key.clone(),
                            );
                        }
                        debug!(
                            generation = completion.generation,
                            route = %link.keyboard,
                            host = request.host,
                            targets = link.targets.len(),
                            "starting linked transition"
                        );
                        stop_all(&mut sessions, HostSwitchStopReason::Graceful).await;
                        run_transition(&links, &channel_pool, &receiver_access, link, request).await;
                    }
                }
            }
        }
    }
}

struct RunningSession {
    link: HostSwitchLink,
    generation: u64,
    stop: oneshot::Sender<HostSwitchStopReason>,
    task: tokio::task::JoinHandle<()>,
}

struct SessionCompletion {
    generation: u64,
    request: Option<(HostSwitchLink, HostSwitchRequest)>,
}

async fn stop_all(sessions: &mut Vec<RunningSession>, reason: HostSwitchStopReason) {
    let running = std::mem::take(sessions);
    let mut tasks = Vec::with_capacity(running.len());
    for RunningSession { stop, task, .. } in running {
        let _ = stop.send(reason);
        tasks.push(task);
    }
    for task in tasks {
        let _ = task.await;
    }
}

async fn stop_unwanted(sessions: &mut Vec<RunningSession>, wanted: &[HostSwitchLink]) {
    let mut index = 0;
    while index < sessions.len() {
        if wanted.contains(&sessions[index].link) {
            index += 1;
            continue;
        }
        let RunningSession { stop, task, .. } = sessions.remove(index);
        let _ = stop.send(HostSwitchStopReason::Graceful);
        let _ = task.await;
    }
}

async fn run_transition(
    links: &HostSwitchLinks,
    channel_pool: &ChannelPool,
    receiver_access: &ReceiverAccess,
    link: HostSwitchLink,
    request: HostSwitchRequest,
) {
    let _lease = receiver_access
        .acquire_exclusive(ExclusiveAccessReason::HostTransition)
        .await;
    match switch_linked_hosts(
        &link.keyboard,
        &link.targets,
        request.host,
        request.keyboard_transition,
        channel_pool,
    )
    .await
    {
        Ok(changed) if should_wait_for_departure(request, changed) => {
            wait_for_departure(links, &link.keyboard).await;
        }
        Ok(_) => {}
        Err(error) => {
            debug!(%error, route = %link.keyboard, host = request.host, "keyboard host switch failed");
        }
    }
}

fn should_wait_for_departure(request: HostSwitchRequest, changed: bool) -> bool {
    changed && request.keyboard_transition == KeyboardHostTransition::CommandRequired
}

fn capture_mode_for(
    announcement_keyboards: &[String],
    keyboard_key: &str,
) -> HostSwitchCaptureMode {
    if announcement_keyboards
        .iter()
        .any(|known_key| known_key == keyboard_key)
    {
        HostSwitchCaptureMode::ChangeHostAnnouncement
    } else {
        HostSwitchCaptureMode::Full
    }
}

fn remember_announcement_keyboard(announcement_keyboards: &mut Vec<String>, keyboard_key: String) {
    if !announcement_keyboards.contains(&keyboard_key) {
        announcement_keyboards.push(keyboard_key);
    }
}

async fn wait_for_departure(links: &HostSwitchLinks, keyboard: &DeviceRoute) {
    let deadline = Instant::now() + DEPARTURE_TIMEOUT;
    while Instant::now() < deadline {
        let departed = links.read().map_or(true, |current| {
            !current.iter().any(|link| link.keyboard == *keyboard)
        });
        if departed {
            return;
        }
        tokio::time::sleep(DEPARTURE_POLL).await;
    }
    warn!(route = %keyboard, "host transition departure was not observed");
}

#[cfg(test)]
mod tests {
    use openlogi_hid::{HostSwitchCaptureMode, HostSwitchRequest, KeyboardHostTransition};

    use super::{capture_mode_for, remember_announcement_keyboard, should_wait_for_departure};

    #[test]
    fn observed_announcement_enables_lightweight_reconnect_capture() {
        let mut keyboards = Vec::new();

        assert_eq!(
            capture_mode_for(&keyboards, "keyboard-a"),
            HostSwitchCaptureMode::Full
        );
        remember_announcement_keyboard(&mut keyboards, "keyboard-a".into());
        assert_eq!(
            capture_mode_for(&keyboards, "keyboard-a"),
            HostSwitchCaptureMode::ChangeHostAnnouncement
        );
        assert_eq!(
            capture_mode_for(&keyboards, "replacement-keyboard"),
            HostSwitchCaptureMode::Full,
            "a device reusing the same route must not inherit another keyboard's capture mode"
        );
    }

    #[test]
    fn self_departing_keyboard_does_not_block_session_rearm() {
        let request = HostSwitchRequest {
            host: 1,
            keyboard_transition: KeyboardHostTransition::AlreadyDeparting,
            announcement_observed: true,
        };

        assert!(!should_wait_for_departure(request, true));
    }

    #[test]
    fn commanded_keyboard_waits_until_its_departure_is_observed() {
        let request = HostSwitchRequest {
            host: 1,
            keyboard_transition: KeyboardHostTransition::CommandRequired,
            announcement_observed: false,
        };

        assert!(should_wait_for_departure(request, true));
        assert!(!should_wait_for_departure(request, false));
    }
}
