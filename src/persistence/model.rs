use super::PersistenceError;
use bytes::Bytes;
use onyxdb::clock::unix_seconds;
use onyxdb::engine::{DataEntry, OnyxValue};
use onyxdb::store::ShardedStore;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PersistentEntry {
    pub(crate) value: OnyxValue,
    pub(crate) expires_at: Option<u64>,
}

impl From<DataEntry> for PersistentEntry {
    fn from(entry: DataEntry) -> Self {
        Self {
            value: entry.value,
            expires_at: entry.expires_at,
        }
    }
}

impl PersistentEntry {
    pub(crate) fn into_data_entry(self) -> DataEntry {
        let timestamp = unix_seconds();
        DataEntry {
            value: self.value,
            expires_at: self.expires_at,
            created_at: timestamp,
            last_accessed: timestamp,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CommittedEffect {
    Put { key: Bytes, entry: PersistentEntry },
    Delete { key: Bytes },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CommittedBatch {
    pub(crate) effects: Vec<CommittedEffect>,
}

impl CommittedBatch {
    pub(crate) fn new(effects: Vec<CommittedEffect>) -> Result<Self, PersistenceError> {
        if effects.is_empty() {
            return Err(PersistenceError::new(
                "A committed mutation batch cannot be empty",
            ));
        }
        Ok(Self { effects })
    }
}

pub(crate) fn derive_committed_batch(
    store: &ShardedStore,
    keys: &[Bytes],
    before: &HashMap<Bytes, Option<DataEntry>>,
    evicted_entries: &[(Bytes, DataEntry)],
) -> Option<CommittedBatch> {
    let mut effects = Vec::new();
    let mut deleted = HashSet::new();
    for (key, _) in evicted_entries {
        if deleted.insert(key.clone()) {
            effects.push(CommittedEffect::Delete { key: key.clone() });
        }
    }

    for key in keys {
        let previous = before
            .get(key)
            .cloned()
            .flatten()
            .map(PersistentEntry::from);
        let current = store.peek_entry(key).map(PersistentEntry::from);
        let was_evicted = deleted.contains(key);
        if previous == current && !was_evicted {
            continue;
        }
        match current {
            Some(entry) => effects.push(CommittedEffect::Put {
                key: key.clone(),
                entry,
            }),
            None if !was_evicted => {
                effects.push(CommittedEffect::Delete { key: key.clone() });
            }
            None => {}
        }
    }

    (!effects.is_empty()).then_some(CommittedBatch { effects })
}

pub(crate) fn apply_committed_batch(store: &ShardedStore, batch: &CommittedBatch) {
    for effect in &batch.effects {
        match effect {
            CommittedEffect::Put { key, entry } => {
                store.apply_entry(key.clone(), entry.clone().into_data_entry());
            }
            CommittedEffect::Delete { key } => {
                store.delete_bytes(key);
            }
        }
    }
}
