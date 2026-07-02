// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::time::Instant;

use ractor::thread_local::{ThreadLocalActor, ThreadLocalActorSpawner};
use ractor::{ActorCell, ActorId, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};
use tokio::sync::broadcast;
use tracing::{trace, warn};

use crate::supervisor::config::RestartStrategy;
use crate::supervisor::events::SupervisorEvent;
use crate::supervisor::traits::ChildActor;

pub enum ToSupervisorActor {
    StartChildActor(Box<dyn ChildActor + 'static>, RpcReplyPort<()>),
    Events(RpcReplyPort<broadcast::Receiver<SupervisorEvent>>),
}

struct ChildActorState {
    child: Box<dyn ChildActor + 'static>,
    actor_cell: ActorCell,
    label: String,
    #[allow(unused)]
    first_started: Instant,
    last_restarted: Option<Instant>,
    restarts: usize,
    failures: usize,
}

impl ChildActorState {
    pub fn new(child: Box<dyn ChildActor + 'static>, actor_cell: ActorCell, label: String) -> Self {
        Self {
            child,
            actor_cell,
            label,
            first_started: Instant::now(),
            last_restarted: None,
            restarts: 0,
            failures: 0,
        }
    }
}

pub struct SupervisorActorState {
    restart_strategy: RestartStrategy,
    children: HashMap<ActorId, ChildActorState>,
    events_tx: broadcast::Sender<SupervisorEvent>,
    thread_pool: ThreadLocalActorSpawner,
}

pub type SupervisorActorArgs = (RestartStrategy, ThreadLocalActorSpawner);

#[derive(Default)]
pub struct SupervisorActor;

impl ThreadLocalActor for SupervisorActor {
    type Msg = ToSupervisorActor;

    type State = SupervisorActorState;

    type Arguments = SupervisorActorArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (restart_strategy, thread_pool) = args;
        let (events_tx, _) = broadcast::channel(64);

        Ok(SupervisorActorState {
            restart_strategy,
            children: HashMap::new(),
            events_tx,
            thread_pool,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ToSupervisorActor::StartChildActor(child, reply) => {
                let label = child.label().to_owned();
                let actor_cell = child
                    .on_start(myself.into(), state.thread_pool.clone())
                    .await?;

                state.children.insert(
                    actor_cell.get_id(),
                    ChildActorState::new(child, actor_cell, label.clone()),
                );

                let _ = state
                    .events_tx
                    .send(SupervisorEvent::ChildStarted { label });

                let _ = reply.send(());
            }
            ToSupervisorActor::Events(reply) => {
                let _ = reply.send(state.events_tx.subscribe());
            }
        }

        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorStarted(actor_cell) => {
                trace!(actor_id = %actor_cell.get_id(), "child actor started");
            }
            SupervisionEvent::ActorTerminated(actor_cell, _, _) => {
                trace!(actor_id = %actor_cell.get_id(), "child actor terminated");

                if let Some(child_state) = state.children.remove(&actor_cell.get_id()) {
                    let _ = state.events_tx.send(SupervisorEvent::ChildTerminated {
                        label: child_state.label,
                    });
                }
            }
            SupervisionEvent::ActorFailed(actor_cell, err) => {
                warn!(actor_id = %actor_cell.get_id(), "child actor failed: {err:?}");

                match state.restart_strategy {
                    RestartStrategy::OneForOne => {
                        if let Some(mut child_state) = state.children.remove(&actor_cell.get_id()) {
                            let label = child_state.label.clone();
                            child_state.restarts += 1;
                            child_state.failures += 1;
                            child_state.last_restarted = Some(Instant::now());
                            let _ = state.events_tx.send(SupervisorEvent::ChildFailed {
                                label: label.clone(),
                                error: err.to_string(),
                                failures: child_state.failures,
                            });
                            let next_actor_cell = child_state
                                .child
                                .on_start(myself.clone().into(), state.thread_pool.clone())
                                .await?;
                            let next_actor_id = next_actor_cell.get_id();
                            child_state.actor_cell = next_actor_cell;
                            let _ = state.events_tx.send(SupervisorEvent::ChildRestarted {
                                label,
                                restarts: child_state.restarts,
                            });
                            state.children.insert(next_actor_id, child_state);
                        }
                    }
                    RestartStrategy::OneForAll => {
                        let mut next_children = HashMap::new();
                        let failed_actor_id = actor_cell.get_id();

                        for (_, mut child_state) in state.children.drain() {
                            // Terminate this actor.
                            child_state.actor_cell.stop(None);

                            // .. and restart it directly again.
                            let label = child_state.label.clone();
                            if child_state.actor_cell == actor_cell {
                                child_state.failures += 1;
                                let _ = state.events_tx.send(SupervisorEvent::ChildFailed {
                                    label: label.clone(),
                                    error: err.to_string(),
                                    failures: child_state.failures,
                                });
                            }
                            child_state.restarts += 1;
                            child_state.last_restarted = Some(Instant::now());

                            let next_actor_cell = child_state
                                .child
                                .on_start(myself.clone().into(), state.thread_pool.clone())
                                .await?;
                            let next_actor_id = next_actor_cell.get_id();
                            child_state.actor_cell = next_actor_cell;
                            let _ = state.events_tx.send(SupervisorEvent::ChildRestarted {
                                label,
                                restarts: child_state.restarts,
                            });
                            next_children.insert(next_actor_id, child_state);
                        }

                        trace!(actor_id = %failed_actor_id, "restarted supervised actor set");
                        state.children = next_children;
                    }
                }
            }
            _ => (),
        }

        Ok(())
    }
}
