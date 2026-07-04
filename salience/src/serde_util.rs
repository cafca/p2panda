//! Serialize maps as sequences of `(key, value)` pairs so that non-string
//! keys (newtypes, tuples, generic ids) survive formats like JSON that only
//! allow string map keys.

use std::collections::BTreeMap;

use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};

pub fn serialize<K, V, S>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
where
    K: Serialize,
    V: Serialize,
    S: Serializer,
{
    serializer.collect_seq(map.iter())
}

pub fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
where
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
    D: Deserializer<'de>,
{
    let pairs: Vec<(K, V)> = Vec::deserialize(deserializer)?;
    Ok(pairs.into_iter().collect())
}
