use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        executor::{
            ExecutionKernel, IndexPredicateCompileMode, aggregate::AggregateKind,
            compile_predicate_slots, load::LoadExecutor,
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

    // Aggregate streaming on index-backed predicates requires strict index
    // predicate compilation. If strict compilation fails, route must force
    // materialized execution to avoid optimistic streaming assumptions.
    // Strict compile policy must stay on the shared executor compile boundary.
    pub(super) fn aggregate_force_materialized_due_to_predicate_uncertainty(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        let Some(predicate_slots) = compile_predicate_slots::<E>(plan) else {
            return false;
        };
        let Some(index_slots) = Self::resolved_index_slots_for_access_path(&plan.access) else {
            return false;
        };

        ExecutionKernel::compile_index_predicate_program_from_slots(
            &predicate_slots,
            index_slots.as_slice(),
            IndexPredicateCompileMode::StrictAllOrNone,
        )
        .is_none()
    }
}
