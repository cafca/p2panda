//! Ingest event types and item attributes.

use serde::{Deserialize, Serialize};

use crate::scopes::Scope;

/// Milliseconds since the UNIX epoch. The engine never reads the clock;
/// every event and every query carries an explicit timestamp.
pub type Timestamp = u64;

/// Milliseconds per day, used to convert timestamps into fractional days.
pub const MS_PER_DAY: f64 = 86_400_000.0;

/// Convert a [`Timestamp`] into a fractional day since the UNIX epoch.
pub fn day_of(at: Timestamp) -> f64 {
    at as f64 / MS_PER_DAY
}

/// App-defined interaction kind.
///
/// The crate ships conventional constants but accepts any value; the meaning
/// of a kind (and its weight in attention units) is defined by the app via
/// [`WeightProfile`](crate::config::WeightProfile).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InteractionKind(pub u16);

impl InteractionKind {
    pub const REACTION: Self = Self(0);
    pub const REPLY: Self = Self(1);
    pub const SHARE: Self = Self(2);
    pub const CLICK: Self = Self(3);
    pub const MEDIA_PLAY: Self = Self(4);
}

/// Attributes of a content item: the scopes it belongs to (author, content
/// type, topics, ...) and an optional semantic embedding vector.
///
/// The embedding is produced by the app (e.g. a pretrained sentence or image
/// model) and is opaque to this crate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ItemAttrs<S> {
    pub scopes: Vec<Scope<S>>,
    pub embedding: Option<Vec<f32>>,
}

impl<S> Default for ItemAttrs<S> {
    fn default() -> Self {
        Self {
            scopes: Vec::new(),
            embedding: None,
        }
    }
}

impl<S> ItemAttrs<S> {
    pub fn new(scopes: Vec<Scope<S>>) -> Self {
        Self {
            scopes,
            embedding: None,
        }
    }

    pub fn with_embedding(mut self, embedding: Vec<f32>) -> Self {
        self.embedding = Some(embedding);
        self
    }
}

/// An attention observation.
///
/// `Exposure` is the app's bet (opportunity granted, weighted by prominence;
/// `1.0` = one standard impression). `Dwell` and `Interaction` are the
/// measurable byproducts of attention actually spent.
#[derive(Clone, Debug, PartialEq)]
pub enum Event<I> {
    Exposure {
        item: I,
        weight: f64,
        at: Timestamp,
    },
    Dwell {
        item: I,
        millis: u64,
        at: Timestamp,
    },
    Interaction {
        item: I,
        kind: InteractionKind,
        at: Timestamp,
    },
}

impl<I> Event<I> {
    pub fn item(&self) -> &I {
        match self {
            Event::Exposure { item, .. } => item,
            Event::Dwell { item, .. } => item,
            Event::Interaction { item, .. } => item,
        }
    }

    pub fn at(&self) -> Timestamp {
        match self {
            Event::Exposure { at, .. } => *at,
            Event::Dwell { at, .. } => *at,
            Event::Interaction { at, .. } => *at,
        }
    }
}
