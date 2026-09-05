//! Module: relation::reverse_index::target_keys
//! Responsibility: carry decoded relation target primary-key sets.
//! Does not own: source-row decoding, reverse-index key encoding, or mutation planning.
//! Boundary: keeps the reverse-index hub focused on relation runtime flow.

use crate::db::key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue};

pub(super) struct RelationTargetKeys {
    values: Vec<PrimaryKeyValue>,
}

impl RelationTargetKeys {
    pub(super) const fn none() -> Self {
        Self { values: Vec::new() }
    }

    pub(super) fn one(value: &PrimaryKeyValue) -> Self {
        Self {
            values: vec![*value],
        }
    }

    const fn from_values(values: Vec<PrimaryKeyValue>) -> Self {
        Self { values }
    }

    pub(super) fn from_scalar_components(components: Vec<PrimaryKeyComponent>) -> Self {
        let mut values = Vec::with_capacity(components.len());
        for component in components {
            values.push(PrimaryKeyValue::Scalar(component));
        }
        Self::from_values(values)
    }

    pub(super) fn contains(&self, target_key: &PrimaryKeyValue) -> bool {
        self.values.iter().any(|key| key == target_key)
    }

    pub(super) const fn len(&self) -> usize {
        self.values.len()
    }

    pub(super) fn into_values(self) -> Vec<PrimaryKeyValue> {
        self.values
    }
}
