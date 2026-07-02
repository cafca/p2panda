// SPDX-License-Identifier: MIT OR Apache-2.0

use ractor::ActorCell;
use ractor::thread_local::{ThreadLocalActor, ThreadLocalActorSpawner};

use crate::gossip::actors::GossipManager;
use crate::gossip::{Builder, Gossip, GossipError};
use crate::supervisor::{ChildActor, ChildActorFut, Supervisor};

impl Builder {
    pub async fn spawn_linked(self, supervisor: &Supervisor) -> Result<Gossip, GossipError> {
        let args = self.build_args();
        let gossip = Gossip::new(None, args.2.node_id(), args.1.clone(), args.0.clone(), args);
        supervisor.start_child_actor(gossip.clone()).await?;
        Ok(gossip)
    }
}

impl ChildActor for Gossip {
    fn label(&self) -> &'static str {
        "Gossip"
    }

    fn on_start(
        &self,
        supervisor: ActorCell,
        thread_pool: ThreadLocalActorSpawner,
    ) -> ChildActorFut<'_> {
        Box::pin(async move {
            let (actor_ref, _) =
                GossipManager::spawn_linked(None, self.args.clone(), supervisor, thread_pool)
                    .await?;

            let mut inner = self.inner.write().await;
            inner.actor_ref.replace(actor_ref.clone());

            Ok(actor_ref.into())
        })
    }
}
