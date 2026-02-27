use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        executor::{
            ExecutionPreparation, IndexPredicateCompileMode, aggregate::AggregateKind,
            compile_index_predicate_program_from_slots, load::LoadExecutor,
        },
        plan::AccessPlannedQuery,
    },
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{RouteCapabilities, supports_pk_stream_access_path};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(super) const fn count_pushdown_path_shape_supported(path: &AccessPath<E::Key>) -> bool {
        matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. })
    }

    pub(super) fn count_pushdown_access_shape_supported(access: &AccessPlan<E::Key>) -> bool {
        match access {
            AccessPlan::Path(path) => Self::count_pushdown_path_shape_supported(path),
            AccessPlan::Union(_) | AccessPlan::Intersection(_) => false,
        }
    }

    // Route-owned gate for PK full-scan/key-range ordered fast-path eligibility.
    pub(in crate::db::executor) fn pk_order_stream_fast_path_shape_supported(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        if !plan.mode.is_load() {
            return false;
        }

        let supports_pk_stream_access = plan
            .access
            .as_path()
            .is_some_and(supports_pk_stream_access_path);
        if !supports_pk_stream_access {
            return false;
        }

        let Some(order) = plan.order.as_ref() else {
            return false;
        };

        order.fields.len() == 1 && order.fields[0].0 == E::MODEL.primary_key.name
    }

    pub(super) const fn is_count_pushdown_eligible(
        kind: AggregateKind,
        capabilities: RouteCapabilities,
    ) -> bool {
        matches!(kind, AggregateKind::Count)
            && capabilities.streaming_access_shape_safe
            && capabilities.count_pushdown_access_shape_supported
    }

    pub(super) fn aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
        execution_preparation: &ExecutionPreparation,
    ) -> bool {
        let Some(compiled_predicate) = execution_preparation.compiled_predicate() else {
            return false;
        };
        let Some(slot_map) = execution_preparation.slot_map() else {
            return false;
        };

        // Route strict-mode uncertainty must remain aligned with the shared
        // kernel predicate compiler boundary.
        execution_preparation.strict_mode().is_none()
            && compile_index_predicate_program_from_slots(
                compiled_predicate,
                slot_map,
                IndexPredicateCompileMode::StrictAllOrNone,
            )
            .is_none()
    }
}
