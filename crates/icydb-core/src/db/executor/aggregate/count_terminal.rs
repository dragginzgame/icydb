//! Module: executor::aggregate::count_terminal
//! Responsibility: direct SQL COUNT index-prefix cardinality execution.
//! Does not own: generic aggregate terminals or non-count reducers.
//! Boundary: resolves one accepted lower prefix family against store metadata.

use crate::{
    db::{
        Db,
        data::DataStore,
        executor::{
            EntityAuthority,
            aggregate::PageSpec,
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

const DIRECT_COUNT_SHAPE_DOMAIN: u64 = 0x6963_7964_622d_636e;

#[cfg(feature = "diagnostics")]
use crate::db::{
    diagnostics::measure_local_instruction_delta as measure_count_terminal_phase,
    executor::plan_metrics::record_rows_scanned_for_path,
};

#[cfg(feature = "diagnostics")]
fn measure_index_prefix_cardinality<T>(run: impl FnOnce() -> T) -> (u64, T) {
    measure_count_terminal_phase(run)
}

#[cfg(not(feature = "diagnostics"))]
fn measure_index_prefix_cardinality<T>(run: impl FnOnce() -> T) -> (u64, T) {
    (0, run())
}

pub(in crate::db) fn execute_direct_count_index_prefix_cardinality_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    page: Option<&PageSpec>,
    prefix_keys: &[UserIndexPrefixCardinalityKey],
) -> Result<Option<u32>, InternalError>
where
    C: CanisterKind,
{
    let context = direct_read_execution_context(
        &authority,
        DiagnosticExecutionLane::TrustedRead,
        DIRECT_COUNT_SHAPE_DOMAIN,
    );
    with_read_execution_budget(db.request_execution_scope(), context, || {
        execute_direct_count_index_prefix_cardinality_inner(db, authority, page, prefix_keys)
    })
}

fn execute_direct_count_index_prefix_cardinality_inner<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    page: Option<&PageSpec>,
    prefix_keys: &[UserIndexPrefixCardinalityKey],
) -> Result<Option<u32>, InternalError>
where
    C: CanisterKind,
{
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        u64::try_from(prefix_keys.len()).unwrap_or(u64::MAX),
    )?;
    let store = db.recovered_store(authority.store_path())?;
    let (metadata_local_instructions, output) = measure_index_prefix_cardinality(|| {
        count_user_index_prefix_cardinality_keys(store, page, prefix_keys)
    });
    let Some(output) = output else {
        return Ok(None);
    };
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultRows, 1)?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultBytes, 32)?;

    #[cfg(not(feature = "diagnostics"))]
    let _ = metadata_local_instructions;
    #[cfg(feature = "diagnostics")]
    {
        record_rows_scanned_for_path(authority.entity_path(), 0);
        super::terminal_attribution::record_index_prefix_cardinality_terminal_attribution(
            metadata_local_instructions,
        );
    }

    Ok(Some(output))
}

fn count_user_index_prefix_cardinality_keys(
    store: StoreHandle,
    page: Option<&PageSpec>,
    prefix_keys: &[UserIndexPrefixCardinalityKey],
) -> Option<u32> {
    count_index_prefix_cardinality_from_sum(page, |required_candidate_rows| {
        let index_id = common_prefix_cardinality_index_id(prefix_keys)?;
        index_prefix_cardinality_sum(
            store,
            store.with_data(DataStore::generation),
            index_id,
            prefix_keys
                .iter()
                .map(UserIndexPrefixCardinalityKey::prefix_components),
            required_candidate_rows,
        )
    })
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
    stop_after: Option<u64>,
) -> Option<u64> {
    store.exact_user_index_prefix_count_sum(
        data_generation,
        IndexKeyKind::User,
        index_id,
        component_prefixes,
        stop_after,
    )
}

fn count_index_prefix_cardinality_from_sum(
    page: Option<&PageSpec>,
    sum: impl FnOnce(Option<u64>) -> Option<u64>,
) -> Option<u32> {
    let candidate_window = CardinalityCandidateWindow::for_count(page);
    if candidate_window.is_empty() {
        return Some(0);
    }

    let available_rows = sum(candidate_window.stop_after())?;
    let available_rows = usize::try_from(available_rows).unwrap_or(usize::MAX);

    Some(count_windowed_candidate_rows(page, available_rows))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CardinalityCandidateWindow {
    Empty,
    Bounded(u64),
    Unbounded,
}

impl CardinalityCandidateWindow {
    fn for_count(page: Option<&PageSpec>) -> Self {
        match page {
            Some(PageSpec { limit: Some(0), .. }) => Self::Empty,
            Some(page) => page.limit.map_or(Self::Unbounded, |limit| {
                Self::Bounded(u64::from(page.offset).saturating_add(u64::from(limit)))
            }),
            None => Self::Unbounded,
        }
    }

    const fn is_empty(self) -> bool {
        matches!(self, Self::Empty)
    }

    const fn stop_after(self) -> Option<u64> {
        match self {
            Self::Empty => Some(0),
            Self::Bounded(rows) => Some(rows),
            Self::Unbounded => None,
        }
    }
}

fn count_windowed_candidate_rows(page: Option<&PageSpec>, available_rows: usize) -> u32 {
    let Some(page) = page else {
        return u32::try_from(available_rows).unwrap_or(u32::MAX);
    };
    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let count = match page.limit {
        Some(limit) => available_rows
            .saturating_sub(offset)
            .min(usize::try_from(limit).unwrap_or(usize::MAX)),
        None => available_rows.saturating_sub(offset),
    };

    u32::try_from(count).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use crate::{
        db::index::{IndexId, UserIndexPrefixCardinalityKey},
        types::EntityTag,
    };

    use super::common_prefix_cardinality_index_id;

    #[test]
    fn direct_count_rejects_prefix_keys_from_mixed_index_generations() {
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
