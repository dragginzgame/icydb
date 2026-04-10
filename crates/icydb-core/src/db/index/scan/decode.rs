use crate::{
    db::{
        data::DataKey,
        executor::{
            record_row_check_index_entry_scanned, record_row_check_index_membership_key_decoded,
            record_row_check_index_membership_multi_key_entry,
            record_row_check_index_membership_single_key_entry,
        },
        index::{
            IndexKey,
            entry::RawIndexEntry,
            key::RawIndexKey,
            predicate::{IndexPredicateExecution, eval_index_execution_on_decoded_key},
            store::IndexStore,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
};

use crate::db::index::scan::{
    DataKeyComponentRows, DataKeyWitnessRows, SingleComponentCoveringCollector,
};

impl IndexStore {
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::index::scan) fn decode_index_entry_and_push(
        entity: EntityTag,
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut Vec<DataKey>,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: only decode raw key components when an index-only
        // predicate needs them. Plain membership scans only need the entry
        // payload, so they should not pay raw-key decode on every hit.
        if let Some(execution) = index_predicate_execution {
            let decoded_key = IndexKey::try_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }
        }

        // Phase 2: fast-path one-key entries without allocating the full
        // membership vector.
        if let Some(storage_key) = value
            .decode_single_key()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            out.push(DataKey::new(entity, storage_key));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: stream multi-key entry payloads without first allocating
        // a membership vector, but still validate the full entry before
        // returning to the caller.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut storage_keys = value
            .iter_keys()
            .map_err(InternalError::index_entry_decode_failed)?;

        for storage_key in &mut storage_keys {
            let storage_key = storage_key.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            out.push(DataKey::new(entity, storage_key));

            if let Some(limit) = limit
                && out.len() == limit
            {
                halted = true;
            }
        }

        if index.is_unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }

    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::index::scan) fn decode_index_entry_and_push_with_witness(
        entity: EntityTag,
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut DataKeyWitnessRows,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: preserve the older raw-key decode discipline. Membership
        // scans only pay decoded-key cost when an index-only predicate still
        // needs the decoded view.
        if let Some(execution) = index_predicate_execution {
            let decoded_key = IndexKey::try_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }
        }

        // Phase 2: fast-path one-key entries without allocating the full
        // membership vector.
        if let Some(membership) = value
            .decode_single_membership()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            out.push((
                DataKey::new(entity, membership.storage_key()),
                membership.existence_witness(),
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: stream multi-key entry payloads without first allocating a
        // membership vector, but still validate the full entry before
        // returning to the caller.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut memberships = value
            .iter_memberships()
            .map_err(InternalError::index_entry_decode_failed)?;

        for membership in &mut memberships {
            let membership = membership.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            out.push((
                DataKey::new(entity, membership.storage_key()),
                membership.existence_witness(),
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                halted = true;
            }
        }

        if index.is_unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }

    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::index::scan) fn decode_index_entry_and_push_with_components(
        entity: EntityTag,
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut DataKeyComponentRows,
        limit: Option<usize>,
        component_indices: &[usize],
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: decode the raw key once, extract every requested component,
        // and evaluate any optional index-only predicate against that one
        // decoded key view.
        let decoded_key = IndexKey::try_from_raw(raw_key)
            .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
        let mut components = Vec::with_capacity(component_indices.len());
        for component_index in component_indices {
            let Some(component) = decoded_key.component(*component_index) else {
                return Err(InternalError::index_projection_component_required(
                    index.name(),
                    *component_index,
                ));
            };
            components.push(component.to_vec());
        }

        if let Some(execution) = index_predicate_execution
            && !eval_index_execution_on_decoded_key(&decoded_key, execution)?
        {
            return Ok(false);
        }

        // Phase 2: fast-path one-key entries without allocating the full
        // membership vector.
        if let Some(membership) = value
            .decode_single_membership()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            out.push((
                DataKey::new(entity, membership.storage_key()),
                membership.existence_witness(),
                components,
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: stream multi-key entry payloads without first allocating
        // a membership vector, but still validate the full entry before
        // returning to the caller.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut memberships = value
            .iter_memberships()
            .map_err(InternalError::index_entry_decode_failed)?;

        for membership in &mut memberships {
            let membership = membership.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            out.push((
                DataKey::new(entity, membership.storage_key()),
                membership.existence_witness(),
                components.clone(),
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                halted = true;
            }
        }

        if index.is_unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }

    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::index::scan) fn decode_index_entry_and_collect_with_component<T, C>(
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut Vec<T>,
        scanned: &mut usize,
        limit: usize,
        component_index: usize,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        collector: &mut C,
    ) -> Result<bool, InternalError>
    where
        C: SingleComponentCoveringCollector<T>,
    {
        record_row_check_index_entry_scanned();

        // Phase 1: extract the requested component and evaluate any optional
        // index-only predicate. When no predicate needs the full decoded key,
        // validate and read only the requested component segment.
        let predicate_component;
        let component = if let Some(execution) = index_predicate_execution {
            let decoded_key = IndexKey::try_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }

            // Keep the predicate-evaluation branch simple and semantically
            // identical to the older path. The raw-component fast path below
            // is reserved for the hotter no-index-predicate cohort.
            predicate_component = decoded_key
                .component(component_index)
                .ok_or_else(|| {
                    InternalError::index_projection_component_required(
                        index.name(),
                        component_index,
                    )
                })?
                .to_vec();

            predicate_component.as_slice()
        } else {
            raw_key
                .validated_component(component_index)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?
                .ok_or_else(|| {
                    InternalError::index_projection_component_required(
                        index.name(),
                        component_index,
                    )
                })?
        };

        // Phase 2: stream single-key entries directly into the caller-owned
        // collector so narrow covering-read paths can skip intermediate tuple
        // staging.
        if let Some(membership) = value
            .decode_single_membership()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            collector.push(
                membership.storage_key(),
                membership.existence_witness(),
                component,
                out,
            )?;
            *scanned = scanned.saturating_add(1);
            if *scanned == limit {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: preserve the existing multi-key semantics while still
        // streaming directly into the caller-owned collector and validating
        // the full entry before returning.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut memberships = value
            .iter_memberships()
            .map_err(InternalError::index_entry_decode_failed)?;

        for membership in &mut memberships {
            let membership = membership.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            collector.push(
                membership.storage_key(),
                membership.existence_witness(),
                component,
                out,
            )?;
            *scanned = scanned.saturating_add(1);
            if *scanned == limit {
                halted = true;
            }
        }

        if index.is_unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }
}
