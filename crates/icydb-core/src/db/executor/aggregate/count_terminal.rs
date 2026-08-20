//! Module: executor::aggregate::count_terminal
//! Responsibility: exact entity and index-prefix cardinality execution.
//! Does not own: generic aggregate terminals or non-count reducers.
//! Boundary: resolves one accepted exact-cardinality target against store metadata.

use crate::{
    db::{
        Db,
        data::DataStore,
        executor::{
            EntityAuthority,
            budget::{
                charge_current_execution_budget, direct_read_execution_context,
                with_read_execution_budget,
            },
        },
        index::{IndexId, IndexKeyKind, UserIndexPrefixCardinalityKey},
        registry::StoreHandle,
    },
    error::InternalError,
    traits::CanisterKind,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource, DiagnosticExecutionLane};

const EXACT_COUNT_SHAPE_DOMAIN: u64 = 0x6963_7964_622d_6578;

#[cfg(feature = "diagnostics")]
use crate::db::{
    diagnostics::measure_local_instruction_delta as measure_count_terminal_phase,
    executor::plan_metrics::record_rows_scanned_for_path,
};

#[cfg(feature = "diagnostics")]
fn measure_exact_cardinality<T>(run: impl FnOnce() -> T) -> (u64, T) {
    measure_count_terminal_phase(run)
}

#[cfg(not(feature = "diagnostics"))]
fn measure_exact_cardinality<T>(run: impl FnOnce() -> T) -> (u64, T) {
    (0, run())
}

/// One planner-proved exact-cardinality metadata target.
pub(in crate::db) enum ExactCardinalityTarget<'keys> {
    /// Exact visible cardinality for the accepted entity.
    Entity,
    /// Exact visible cardinality summed across one bounded user-index prefix family.
    UserIndexPrefixes(&'keys [UserIndexPrefixCardinalityKey]),
}

impl ExactCardinalityTarget<'_> {
    fn charged_metadata_entries(&self) -> u64 {
        match self {
            Self::Entity => 1,
            Self::UserIndexPrefixes(keys) => u64::try_from(keys.len()).unwrap_or(u64::MAX),
        }
    }
}

/// Execute one fail-closed metadata-only exact cardinality read.
pub(in crate::db) fn execute_exact_cardinality_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    lane: DiagnosticExecutionLane,
    target: ExactCardinalityTarget<'_>,
) -> Result<Option<u64>, InternalError>
where
    C: CanisterKind,
{
    let context = direct_read_execution_context(&authority, lane, EXACT_COUNT_SHAPE_DOMAIN);
    with_read_execution_budget(db.request_execution_scope(), context, || {
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            target.charged_metadata_entries(),
        )?;
        let store = db.recovered_store(authority.store_path())?;
        let index_prefix_target = matches!(&target, ExactCardinalityTarget::UserIndexPrefixes(_));
        let (metadata_local_instructions, output) = measure_exact_cardinality(|| match target {
            ExactCardinalityTarget::Entity => store.exact_entity_count(authority.entity_tag()),
            ExactCardinalityTarget::UserIndexPrefixes(prefix_keys) => {
                exact_user_index_prefix_cardinality_sum(store, prefix_keys)
            }
        });
        let Some(output) = output else {
            return Ok(None);
        };
        charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultRows, 1)?;
        charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultBytes, 32)?;

        #[cfg(not(feature = "diagnostics"))]
        let _ = (index_prefix_target, metadata_local_instructions);
        #[cfg(feature = "diagnostics")]
        {
            record_rows_scanned_for_path(authority.entity_path(), 0);
            if index_prefix_target {
                super::terminal_attribution::record_index_prefix_cardinality_terminal_attribution(
                    metadata_local_instructions,
                );
            }
        }

        Ok(Some(output))
    })
}

fn exact_user_index_prefix_cardinality_sum(
    store: StoreHandle,
    prefix_keys: &[UserIndexPrefixCardinalityKey],
) -> Option<u64> {
    let index_id = common_prefix_cardinality_index_id(prefix_keys)?;
    index_prefix_cardinality_sum(
        store,
        store.with_data(DataStore::generation),
        index_id,
        prefix_keys
            .iter()
            .map(UserIndexPrefixCardinalityKey::prefix_components),
    )
}

fn common_prefix_cardinality_index_id(
    prefix_keys: &[UserIndexPrefixCardinalityKey],
) -> Option<IndexId> {
    let index_id = prefix_keys.first()?.index_id();
    prefix_keys
        .iter()
        .all(|key| key.index_id() == index_id)
        .then_some(index_id)
}

fn index_prefix_cardinality_sum<'a>(
    store: StoreHandle,
    data_generation: u64,
    index_id: IndexId,
    component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
) -> Option<u64> {
    store.exact_user_index_prefix_count_sum(
        data_generation,
        IndexKeyKind::User,
        index_id,
        component_prefixes,
        None,
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        db::index::{IndexId, UserIndexPrefixCardinalityKey},
        types::EntityTag,
    };

    use super::common_prefix_cardinality_index_id;

    #[test]
    fn exact_count_rejects_prefix_keys_from_mixed_index_generations() {
        let entity_tag = EntityTag::new(0xCA7D);
        let current_index = IndexId::new_with_generation(entity_tag, 2, 7);
        let next_index = IndexId::new_with_generation(entity_tag, 2, 8);
        let current =
            UserIndexPrefixCardinalityKey::new(current_index, vec![b"collection-a".to_vec()]);
        let same_generation =
            UserIndexPrefixCardinalityKey::new(current_index, vec![b"collection-b".to_vec()]);
        let next_generation =
            UserIndexPrefixCardinalityKey::new(next_index, vec![b"collection-c".to_vec()]);

        assert_eq!(
            common_prefix_cardinality_index_id(&[current.clone(), same_generation]),
            Some(current_index),
        );
        assert_eq!(
            common_prefix_cardinality_index_id(&[current, next_generation]),
            None,
        );
    }
}
