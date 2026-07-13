use crate::{
    db::{
        Db,
        access::{LoweredIndexPrefixSpec, LoweredIndexRangeSpec},
        data::DecodedDataStoreKey,
        direction::Direction,
        executor::{
            AccessWindow, CoveringProjectionComponentRows, EntityAuthority, PrefixSetMergeSafety,
            apply_offset_limit_window, expand_index_prefix_family_with_exact_child_prefixes,
            projection::covering::contracts::{
                AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
                CoveringReadField, CoveringReadFieldSource, PageSpec,
            },
            resolve_covering_projection_components_from_lowered_specs,
            route::{
                access_preserves_primary_key_order_without_child_expansion,
                index_prefix_child_expansion_hint_for_access_window,
            },
        },
        index::predicate::IndexPredicateExecution,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};
use std::collections::BTreeMap;

pub(super) struct PreparedCoveringIndexScan {
    pub(super) component_indices: Vec<usize>,
    pub(super) raw_pairs: CoveringProjectionComponentRows,
    pub(super) scan_window: CoveringScanWindow,
    pub(super) stream_order_satisfies_projection_order: bool,
    pub(super) store: StoreHandle,
}

pub(super) struct CoveringScanWindow {
    pub(super) direction: Direction,
    pub(super) limit: usize,
    pub(super) page_skip_count: usize,
    pub(super) page_window_applied: bool,
}

#[derive(Clone, Copy)]
pub(super) struct CoveringIndexScanRequest<'a> {
    pub(super) plan: &'a AccessPlannedQuery,
    pub(super) index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    pub(super) index_range_specs: &'a [LoweredIndexRangeSpec],
    pub(super) fields: &'a [CoveringReadField],
    pub(super) order_contract: CoveringProjectionOrder,
    pub(super) existing_row_mode: CoveringExistingRowMode,
    pub(super) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

pub(super) fn covering_residual_filter_supported(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
    index_predicate_execution_available: bool,
) -> bool {
    if plan.has_residual_filter_expr() {
        return false;
    }

    !plan.has_residual_filter_predicate()
        || (strict_predicate_compatible && index_predicate_execution_available)
}

pub(super) fn access_preserves_primary_key_order_for_covering_window(
    plan: &AccessPlannedQuery,
    order_contract: CoveringProjectionOrder,
) -> bool {
    let CoveringProjectionOrder::PrimaryKeyOrder(direction) = order_contract else {
        return false;
    };

    access_preserves_primary_key_order_without_child_expansion(plan, direction)
}

pub(super) fn covering_scan_window(
    order_contract: CoveringProjectionOrder,
    primary_key_order_scan_safe: bool,
    page_window_allowed_for_route: bool,
    distinct: bool,
    page: Option<&PageSpec>,
) -> CoveringScanWindow {
    let page_window_can_apply = page_window_allowed_for_route
        && !distinct
        && covering_scan_order_can_apply_page_window(order_contract, primary_key_order_scan_safe);

    CoveringScanWindow {
        direction: covering_scan_direction(order_contract, primary_key_order_scan_safe),
        limit: covering_scan_limit(page_window_can_apply, page),
        page_skip_count: covering_scan_time_page_skip_count(page_window_can_apply, page),
        page_window_applied: covering_scan_time_page_window_applied(page_window_can_apply, page),
    }
}

const fn covering_scan_direction(
    order_contract: CoveringProjectionOrder,
    primary_key_order_scan_safe: bool,
) -> Direction {
    match order_contract {
        CoveringProjectionOrder::PrimaryKeyOrder(direction) if primary_key_order_scan_safe => {
            direction
        }
        _ => crate::db::executor::covering_projection_scan_direction(order_contract),
    }
}

pub(super) fn resolve_index_backed_covering_scan<C>(
    db: &Db<C>,
    authority: &EntityAuthority,
    request: CoveringIndexScanRequest<'_>,
) -> Result<Option<PreparedCoveringIndexScan>, InternalError>
where
    C: CanisterKind,
{
    if !request.plan.access.has_selected_index_access_path() {
        return Ok(None);
    }

    let component_indices = covering_projection_component_indices(request.fields);
    let store = db.recovered_store(authority.store_path())?;
    let (expanded_index_prefix_specs, primary_key_order_scan_safe) =
        expanded_covering_index_prefix_specs_if_ordered(
            db,
            authority,
            request.plan,
            request.order_contract,
            request.index_prefix_specs,
        )?;
    let index_prefix_specs = expanded_index_prefix_specs
        .as_deref()
        .unwrap_or(request.index_prefix_specs);
    let scan_window = covering_scan_window(
        request.order_contract,
        primary_key_order_scan_safe,
        request.existing_row_mode == CoveringExistingRowMode::ProvenByPlanner,
        request.plan.scalar_plan().distinct,
        request.plan.scalar_plan().page.as_ref(),
    );
    let stream_order_satisfies_projection_order = primary_key_order_scan_safe
        || matches!(
            request.order_contract,
            CoveringProjectionOrder::IndexOrder(_)
        );
    let raw_pairs = if expanded_index_prefix_specs
        .as_ref()
        .is_some_and(Vec::is_empty)
    {
        Vec::new()
    } else {
        resolve_covering_projection_components_from_lowered_specs(
            authority.entity_tag(),
            index_prefix_specs,
            request.index_range_specs,
            scan_window.direction,
            scan_window.limit,
            component_indices.as_slice(),
            request.index_predicate_execution,
            if primary_key_order_scan_safe {
                PrefixSetMergeSafety::OrderedMergeSafe
            } else if matches!(
                request.order_contract,
                CoveringProjectionOrder::IndexOrder(_)
            ) {
                PrefixSetMergeSafety::OrderedConcatSafe
            } else {
                PrefixSetMergeSafety::RequiresMaterialization
            },
            |store_path| db.recovered_store(store_path),
        )?
    };

    Ok(Some(PreparedCoveringIndexScan {
        component_indices,
        raw_pairs,
        scan_window,
        stream_order_satisfies_projection_order,
        store,
    }))
}

fn expanded_covering_index_prefix_specs_if_ordered<C>(
    db: &Db<C>,
    authority: &EntityAuthority,
    plan: &AccessPlannedQuery,
    order_contract: CoveringProjectionOrder,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
) -> Result<(Option<Vec<LoweredIndexPrefixSpec>>, bool), InternalError>
where
    C: CanisterKind,
{
    if access_preserves_primary_key_order_for_covering_window(plan, order_contract) {
        return Ok((None, true));
    }
    // Child-prefix expansion proves primary-key suffix ordering; secondary
    // index-order contracts must keep their branch prefix order instead.
    if !matches!(order_contract, CoveringProjectionOrder::PrimaryKeyOrder(_)) {
        return Ok((None, false));
    }

    let Some(expansion) = index_prefix_child_expansion_hint_for_access_window(
        plan,
        covering_child_prefix_expansion_access_window(plan),
    ) else {
        return Ok((None, false));
    };
    let Some(index) = plan.access_shape_facts().single_path_index_prefix_details() else {
        return Ok((None, false));
    };
    let Some(expanded_family) = expand_index_prefix_family_with_exact_child_prefixes(
        db.recovered_store(authority.store_path())?,
        authority.entity_tag(),
        &index,
        index_prefix_specs,
        expansion,
    )?
    else {
        return Ok((None, false));
    };

    Ok((Some(expanded_family.into_specs()), true))
}

fn covering_child_prefix_expansion_access_window(plan: &AccessPlannedQuery) -> AccessWindow {
    let window = plan.scalar_access_window_plan(false);

    AccessWindow::new(window.lower_bound(), window.limit(), window.fetch_count())
}

pub(super) fn apply_covering_page_window<T>(
    distinct: bool,
    page: Option<&PageSpec>,
    page_window_already_applied: bool,
    rows: &mut Vec<T>,
) {
    if distinct {
        // DISTINCT paging is deferred to the projection materializer after
        // projected-row deduplication over the ordered stream.
        return;
    }
    if page_window_already_applied {
        return;
    }

    let Some(page) = page else {
        return;
    };

    apply_offset_limit_window(rows, page.offset, page.limit);
}

const fn covering_scan_order_can_apply_page_window(
    order_contract: CoveringProjectionOrder,
    primary_key_order_scan_safe: bool,
) -> bool {
    matches!(order_contract, CoveringProjectionOrder::IndexOrder(_))
        || (primary_key_order_scan_safe
            && matches!(order_contract, CoveringProjectionOrder::PrimaryKeyOrder(_)))
}

fn covering_scan_limit(page_window_can_apply: bool, page: Option<&PageSpec>) -> usize {
    let Some(page) = page else {
        return usize::MAX;
    };
    if !page_window_can_apply {
        return usize::MAX;
    }
    let Some(limit) = page.limit else {
        return usize::MAX;
    };

    page.offset
        .saturating_add(limit)
        .max(1)
        .try_into()
        .unwrap_or(usize::MAX)
}

fn covering_scan_time_page_skip_count(
    page_window_can_apply: bool,
    page: Option<&PageSpec>,
) -> usize {
    if !page_window_can_apply {
        return 0;
    }

    page.map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX))
}

fn covering_scan_time_page_window_applied(
    page_window_can_apply: bool,
    page: Option<&PageSpec>,
) -> bool {
    if !page_window_can_apply {
        return false;
    }

    page.is_some_and(|page| page.offset != 0 || page.limit.is_some())
}

pub(super) fn covering_projection_component_indices(fields: &[CoveringReadField]) -> Vec<usize> {
    let mut component_indices = Vec::with_capacity(fields.len());

    for field in fields {
        let component_index = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                component_index
            }
            CoveringReadFieldSource::PrimaryKey { .. }
            | CoveringReadFieldSource::Constant(_)
            | CoveringReadFieldSource::RowField => continue,
        };
        if component_indices.contains(component_index) {
            continue;
        }

        component_indices.push(*component_index);
    }

    component_indices
}

pub(super) fn project_covering_row_from_decoded_values(
    data_key: &DecodedDataStoreKey,
    fields: &[CoveringReadField],
    component_indices: &[usize],
    decoded_values: &[Value],
) -> Result<Vec<Value>, InternalError> {
    if component_indices.len() != decoded_values.len() {
        return Err(InternalError::query_executor_invariant());
    }
    if let Some(projected) = project_single_primary_key_covering_row(data_key, fields)? {
        return Ok(projected);
    }

    let mut projected = Vec::with_capacity(fields.len());

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                let Some(position) = component_indices
                    .iter()
                    .position(|candidate| candidate == component_index)
                else {
                    return Err(InternalError::query_executor_invariant());
                };

                decoded_values
                    .get(position)
                    .cloned()
                    .ok_or_else(InternalError::query_executor_invariant)?
            }
            CoveringReadFieldSource::PrimaryKey { component_index } => {
                data_key.primary_key_component_runtime_value(*component_index)?
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant());
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

fn project_single_primary_key_covering_row(
    data_key: &DecodedDataStoreKey,
    fields: &[CoveringReadField],
) -> Result<Option<Vec<Value>>, InternalError> {
    let [field] = fields else {
        return Ok(None);
    };
    let CoveringReadFieldSource::PrimaryKey { component_index } = &field.source else {
        return Ok(None);
    };

    Ok(Some(vec![
        data_key.primary_key_component_runtime_value(*component_index)?,
    ]))
}

pub(super) fn project_covering_row_from_owned_decoded_values(
    data_key: &DecodedDataStoreKey,
    fields: &[CoveringReadField],
    component_indices: &[usize],
    decoded_values: Vec<Value>,
) -> Result<Vec<Value>, InternalError> {
    if component_indices.len() != decoded_values.len() {
        return Err(InternalError::query_executor_invariant());
    }

    let mut projected = Vec::with_capacity(fields.len());
    let mut decoded_values = decoded_values;
    let mut remaining_component_uses =
        covering_component_position_use_counts(fields, component_indices);

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                let Some(position) = component_indices
                    .iter()
                    .position(|candidate| candidate == component_index)
                else {
                    return Err(InternalError::query_executor_invariant());
                };

                take_or_clone_last_component_value(
                    decoded_values.as_mut_slice(),
                    remaining_component_uses.as_mut_slice(),
                    position,
                )?
            }
            CoveringReadFieldSource::PrimaryKey { component_index } => {
                data_key.primary_key_component_runtime_value(*component_index)?
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant());
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

pub(super) fn project_covering_row_from_single_decoded_value(
    data_key: &DecodedDataStoreKey,
    fields: &[CoveringReadField],
    component_index: usize,
    decoded_value: Value,
) -> Result<Vec<Value>, InternalError> {
    let mut projected = Vec::with_capacity(fields.len());
    let mut decoded_value = Some(decoded_value);

    // Count matching output cells first so the final occurrence can consume the
    // owned decoded value while earlier duplicate columns keep cloning.
    let mut remaining_component_uses = fields
        .iter()
        .filter(|field| {
            matches!(
                &field.source,
                CoveringReadFieldSource::IndexComponent {
                    component_index: field_component_index
                }
                    | CoveringReadFieldSource::IndexExpressionComponent {
                    component_index: field_component_index
                } if *field_component_index == component_index
            )
        })
        .count();

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent {
                component_index: field_component_index,
            }
            | CoveringReadFieldSource::IndexExpressionComponent {
                component_index: field_component_index,
            } => {
                if *field_component_index != component_index {
                    return Err(InternalError::query_executor_invariant());
                }

                // Each projected column owns its value. Duplicate references
                // clone until the last use, where ownership can move into the
                // output row directly.
                remaining_component_uses = remaining_component_uses.saturating_sub(1);
                if remaining_component_uses == 0 {
                    decoded_value
                        .take()
                        .ok_or_else(InternalError::query_executor_invariant)?
                } else {
                    decoded_value
                        .clone()
                        .ok_or_else(InternalError::query_executor_invariant)?
                }
            }
            CoveringReadFieldSource::PrimaryKey { component_index } => {
                data_key.primary_key_component_runtime_value(*component_index)?
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant());
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

fn covering_component_position_use_counts(
    fields: &[CoveringReadField],
    component_indices: &[usize],
) -> Vec<usize> {
    let mut counts = vec![0; component_indices.len()];

    for field in fields {
        let component_index = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                component_index
            }
            CoveringReadFieldSource::PrimaryKey { .. }
            | CoveringReadFieldSource::Constant(_)
            | CoveringReadFieldSource::RowField => continue,
        };
        if let Some(position) = component_indices
            .iter()
            .position(|candidate| candidate == component_index)
        {
            counts[position] += 1;
        }
    }

    counts
}

fn take_or_clone_last_component_value(
    decoded_values: &mut [Value],
    remaining_component_uses: &mut [usize],
    position: usize,
) -> Result<Value, InternalError> {
    let Some(remaining) = remaining_component_uses.get_mut(position) else {
        return Err(InternalError::query_executor_invariant());
    };

    // Projected columns are independently owned. Duplicate references clone
    // until the final component use can move out of the decoded row vector.
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        let Some(value) = decoded_values.get_mut(position) else {
            return Err(InternalError::query_executor_invariant());
        };

        return Ok(std::mem::replace(value, Value::Null));
    }

    decoded_values
        .get(position)
        .cloned()
        .ok_or_else(InternalError::query_executor_invariant)
}

pub(super) fn decode_hybrid_covering_components(
    component_indices: &[usize],
    components: std::sync::Arc<[Vec<u8>]>,
) -> Result<BTreeMap<usize, Value>, InternalError> {
    let mut decoded = BTreeMap::new();

    for (component_index, component) in component_indices.iter().copied().zip(components.iter()) {
        let Some(value) =
            crate::db::executor::decode_covering_projection_component(component.as_slice())?
        else {
            return Err(InternalError::query_executor_invariant());
        };
        decoded.insert(component_index, value);
    }

    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            access::{AccessPath, AccessPlan, SemanticIndexAccessContract},
            predicate::MissingRowPolicy,
            query::plan::AccessPlannedQuery,
        },
        entity::EntityDeclaration,
        model::{field::FieldKind, index::IndexModel},
        types::Ulid,
    };

    const COLLECTION_STAGE_ID_FIELDS: [&str; 3] = ["collection_id", "stage", "id"];
    const COLLECTION_STAGE_ID_INDEX: IndexModel = IndexModel::generated(
        "covering_window::tests::collection_stage_id",
        "covering_window::tests::CoveringWindowEntity",
        &COLLECTION_STAGE_ID_FIELDS,
        false,
    );

    crate::test_schema_entity! {
        ident = CoveringWindowEntity,
        entity_name = "CoveringWindowEntity",
        key_type = Ulid,
        primary_key = [id],
        fields = [
            crate::test_field! { id: Ulid => FieldKind::Ulid },
            crate::test_field! { collection_id: () => FieldKind::Text { max_len: None } },
            crate::test_field! { stage: () => FieldKind::Text { max_len: None } },
        ],
        indexes = [&COLLECTION_STAGE_ID_INDEX],
    }

    fn index_contract() -> SemanticIndexAccessContract {
        SemanticIndexAccessContract::model_only_from_generated_index(COLLECTION_STAGE_ID_INDEX)
    }

    fn finalized_plan(access: AccessPath<Value>) -> AccessPlannedQuery {
        let mut plan = AccessPlannedQuery::new(access, MissingRowPolicy::Ignore);
        plan.finalize_static_execution_planning_contract_for_model_only(
            <CoveringWindowEntity as EntityDeclaration>::MODEL,
        )
        .expect("covering-window tests require frozen primary-key metadata");

        plan
    }

    fn finalized_paged_prefix_plan(page: Option<PageSpec>) -> AccessPlannedQuery {
        let mut plan = finalized_plan(AccessPath::IndexPrefix {
            index: index_contract(),
            values: vec![Value::Text("collection-a".to_string())],
        });
        plan.scalar_plan_mut().page = page;

        plan
    }

    #[test]
    fn covering_window_rejects_prefix_before_primary_key_suffix() {
        let plan = finalized_plan(AccessPath::IndexPrefix {
            index: index_contract(),
            values: vec![Value::Text("collection-a".to_string())],
        });

        assert!(
            !access_preserves_primary_key_order_for_covering_window(
                &plan,
                CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
            ),
            "prefix (collection_id) leaves stage before id, so index order is not global primary-key order",
        );
    }

    #[test]
    fn covering_window_accepts_prefix_at_primary_key_suffix() {
        let plan = finalized_plan(AccessPath::IndexPrefix {
            index: index_contract(),
            values: vec![
                Value::Text("collection-a".to_string()),
                Value::Text("Draft".to_string()),
            ],
        });

        assert!(
            access_preserves_primary_key_order_for_covering_window(
                &plan,
                CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
            ),
            "prefix (collection_id, stage) consumes every non-primary component before id",
        );
    }

    #[test]
    fn covering_window_accepts_branch_set_at_primary_key_suffix() {
        let branch_plan = AccessPlan::index_branch_set_from_contract(
            index_contract(),
            vec![Value::Text("collection-a".to_string())],
            vec![
                Value::Text("Draft".to_string()),
                Value::Text("Review".to_string()),
            ],
        );
        let plan = finalized_plan(
            branch_plan
                .as_path()
                .expect("branch-set helper should produce a path")
                .clone(),
        );

        assert!(
            access_preserves_primary_key_order_for_covering_window(
                &plan,
                CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
            ),
            "branch-set streams consume collection_id and stage before merging by id",
        );
    }

    #[test]
    fn covering_scan_window_does_not_prelimit_unproven_primary_key_order() {
        let scan_window = covering_scan_window(
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
            false,
            true,
            false,
            Some(&PageSpec {
                limit: Some(8),
                offset: 0,
            }),
        );

        assert_eq!(scan_window.limit, usize::MAX);
        assert!(!scan_window.page_window_applied);
    }

    #[test]
    fn covering_scan_window_uses_desc_direction_for_proven_primary_key_order() {
        let scan_window = covering_scan_window(
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc),
            true,
            true,
            false,
            Some(&PageSpec {
                limit: Some(8),
                offset: 0,
            }),
        );

        assert_eq!(scan_window.direction, Direction::Desc);
        assert_eq!(scan_window.limit, 8);
        assert!(scan_window.page_window_applied);
    }

    #[test]
    fn covering_child_prefix_expansion_access_window_matches_scalar_lookahead_window() {
        assert_eq!(
            covering_child_prefix_expansion_access_window(&finalized_paged_prefix_plan(Some(
                PageSpec {
                    limit: Some(50),
                    offset: 0,
                },
            )))
            .fetch_limit(),
            Some(51),
            "covering child-prefix expansion should use the scalar route lookahead fetch",
        );
        assert_eq!(
            covering_child_prefix_expansion_access_window(&finalized_paged_prefix_plan(Some(
                PageSpec {
                    limit: Some(2),
                    offset: 3,
                },
            )))
            .fetch_limit(),
            Some(6),
            "covering child-prefix expansion should include offset and lookahead",
        );
        assert_eq!(
            covering_child_prefix_expansion_access_window(&finalized_paged_prefix_plan(Some(
                PageSpec {
                    limit: Some(0),
                    offset: 4,
                },
            )))
            .fetch_limit(),
            Some(0),
            "LIMIT 0 should remain a zero-fetch expansion window",
        );
        assert_eq!(
            covering_child_prefix_expansion_access_window(&finalized_paged_prefix_plan(None))
                .fetch_limit(),
            None,
            "unpaged covering scans should keep the default child-prefix cap",
        );
    }
}
