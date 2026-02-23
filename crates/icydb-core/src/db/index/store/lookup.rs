use crate::{
    db::{
        data::DataKey,
        index::{
            Direction, IndexKey, continuation_advanced, envelope_is_empty,
            range::anchor_within_envelope,
            resume_bounds,
            store::{IndexStore, RawIndexKey},
        },
        query::predicate::{IndexPredicateExecution, eval_index_execution_on_decoded_key},
    },
    error::InternalError,
    model::index::IndexModel,
    traits::EntityKind,
};
use std::ops::Bound;

impl IndexStore {
    pub(in crate::db) fn resolve_data_values_in_raw_range_limited<E: EntityKind>(
        &self,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation_start_exclusive: Option<&RawIndexKey>,
        direction: Direction,
        limit: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        Self::ensure_anchor_within_envelope(direction, continuation_start_exclusive, bounds)?;

        let (start_raw, end_raw) = match continuation_start_exclusive {
            Some(anchor) => resume_bounds(direction, bounds.0.clone(), bounds.1.clone(), anchor),
            None => (bounds.0.clone(), bounds.1.clone()),
        };

        if envelope_is_empty(&start_raw, &end_raw) {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();

        match direction {
            Direction::Asc => {
                for entry in self.map.range((start_raw, end_raw)) {
                    let raw_key = entry.key();
                    let value = entry.value();

                    Self::ensure_continuation_advanced(
                        direction,
                        raw_key,
                        continuation_start_exclusive,
                    )?;

                    if Self::decode_index_entry_and_push::<E>(
                        index,
                        raw_key,
                        &value,
                        &mut out,
                        Some(limit),
                        "range resolve",
                        index_predicate_execution,
                    )? {
                        return Ok(out);
                    }
                }
            }
            Direction::Desc => {
                for entry in self.map.range((start_raw, end_raw)).rev() {
                    let raw_key = entry.key();
                    let value = entry.value();

                    Self::ensure_continuation_advanced(
                        direction,
                        raw_key,
                        continuation_start_exclusive,
                    )?;

                    if Self::decode_index_entry_and_push::<E>(
                        index,
                        raw_key,
                        &value,
                        &mut out,
                        Some(limit),
                        "range resolve",
                        index_predicate_execution,
                    )? {
                        return Ok(out);
                    }
                }
            }
        }

        Ok(out)
    }

    // Validate strict continuation advancement when an anchor is present.
    //
    // IMPORTANT CROSS-LAYER CONTRACT:
    // - Planner/cursor-spine validation ensures envelope/signature compatibility.
    // - This store-layer guard independently enforces strict monotonic advancement.
    // - Keep both layers explicit; do not collapse this into planner-only checks.
    fn ensure_continuation_advanced(
        direction: Direction,
        candidate: &RawIndexKey,
        anchor: Option<&RawIndexKey>,
    ) -> Result<(), InternalError> {
        if let Some(anchor) = anchor
            && !continuation_advanced(direction, candidate, anchor)
        {
            return Err(InternalError::index_invariant(
                "index-range continuation scan did not advance beyond the anchor",
            ));
        }

        Ok(())
    }

    // Validate that continuation anchor is contained by the original range envelope.
    //
    // Keep this guard in the store layer even though planner/cursor validation already
    // checks containment: this is a defensive contract check against cross-layer misuse.
    fn ensure_anchor_within_envelope(
        direction: Direction,
        anchor: Option<&RawIndexKey>,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
    ) -> Result<(), InternalError> {
        if let Some(anchor) = anchor
            && !anchor_within_envelope(direction, anchor, bounds.0, bounds.1)
        {
            return Err(InternalError::index_invariant(
                "index-range continuation anchor is outside the requested range envelope",
            ));
        }

        Ok(())
    }

    fn decode_index_entry_and_push<E: EntityKind>(
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &super::StoredIndexValue,
        out: &mut Vec<DataKey>,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        #[cfg(debug_assertions)]
        Self::verify_if_debug(raw_key, value);

        let decoded_key = IndexKey::try_from_raw(raw_key).map_err(|err| {
            InternalError::index_corruption(format!("index key corrupted during {context}: {err}"))
        })?;

        if let Some(execution) = index_predicate_execution
            && !eval_index_execution_on_decoded_key(&decoded_key, execution)?
        {
            return Ok(false);
        }

        let storage_keys = value
            .entry
            .decode_keys()
            .map_err(|err| InternalError::index_corruption(err.to_string()))?;

        if index.unique && storage_keys.len() != 1 {
            return Err(InternalError::index_corruption(
                "unique index entry contains an unexpected number of keys",
            ));
        }

        for storage_key in storage_keys {
            out.push(DataKey::from_key::<E>(storage_key));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use crate::{
        db::index::{Direction, store::RawIndexKey},
        error::{ErrorClass, ErrorOrigin},
        traits::Storable,
    };
    use std::{borrow::Cow, ops::Bound};

    use super::IndexStore;

    fn raw_key(byte: u8) -> RawIndexKey {
        <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
    }

    #[test]
    fn continuation_advancement_guard_rejects_non_advanced_candidate_asc() {
        let anchor = raw_key(0x10);
        let candidate = raw_key(0x10);

        let err =
            IndexStore::ensure_continuation_advanced(Direction::Asc, &candidate, Some(&anchor))
                .expect_err("ASC continuation candidate equal to anchor must be rejected");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    fn continuation_advancement_guard_rejects_non_advanced_candidate_desc() {
        let anchor = raw_key(0x10);
        let candidate = raw_key(0x11);

        let err =
            IndexStore::ensure_continuation_advanced(Direction::Desc, &candidate, Some(&anchor))
                .expect_err(
                    "DESC continuation candidate not strictly after anchor must be rejected",
                );

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    fn anchor_containment_guard_rejects_out_of_envelope_anchor() {
        let lower = Bound::Included(raw_key(0x10));
        let upper = Bound::Excluded(raw_key(0x20));
        let anchor = raw_key(0x20);

        let err = IndexStore::ensure_anchor_within_envelope(
            Direction::Asc,
            Some(&anchor),
            (&lower, &upper),
        )
        .expect_err("out-of-envelope continuation anchor must be rejected");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }
}
