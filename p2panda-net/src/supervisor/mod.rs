// SPDX-License-Identifier: MIT OR Apache-2.0

//! Monitor system with supervisors and restart modules on critical failure.
mod actor;
mod api;
mod builder;
mod config;
mod events;
#[cfg(test)]
mod tests;
mod traits;

pub use api::{Supervisor, SupervisorError};
pub use builder::Builder;
pub use config::RestartStrategy;
pub use events::SupervisorEvent;
pub use traits::{ChildActor, ChildActorFut};
