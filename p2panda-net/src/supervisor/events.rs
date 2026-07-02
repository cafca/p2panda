// SPDX-License-Identifier: MIT OR Apache-2.0

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SupervisorEvent {
    ChildStarted {
        label: String,
    },
    ChildTerminated {
        label: String,
    },
    ChildFailed {
        label: String,
        error: String,
        failures: usize,
    },
    ChildRestarted {
        label: String,
        restarts: usize,
    },
}
