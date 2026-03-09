//! Module: executor::load::fast_stream
//! Responsibility: execute verified fast-path stream requests without restream adapters.
//! Does not own: fast-path eligibility policy or access-path lowering rules.
//! Boundary: stream execution helper used after routing/eligibility gates.

use crate::{
    db::{
        Context,
        executor::{
            AccessExecutionDescriptor, ExecutionOptimization,
            load::{FastPathKeyResult, LoadExecutor, invariant},
            route::RoutedKeyStreamRequest,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one fast-path access stream without materialize/restream adapters.
    ///
    /// Fast-path streams must expose an exact key-count hint for observability parity.
    pub(super) fn execute_fast_stream_request(
        ctx: &Context<'_, E>,
        descriptor: AccessExecutionDescriptor<'_, E::Key>,
        optimization: ExecutionOptimization,
    ) -> Result<FastPathKeyResult, InternalError> {
        // Phase 1: resolve the ordered key stream through the routed access boundary.
        let key_stream = Self::resolve_routed_key_stream(
            ctx,
            RoutedKeyStreamRequest::AccessDescriptor(descriptor),
        )?;

        // Phase 2: enforce exact row-scan count hint required by fast-path observability.
        let rows_scanned = key_stream
            .exact_key_count_hint()
            .ok_or_else(|| invariant("fast-path stream must expose an exact key-count hint"))?;

        Ok(FastPathKeyResult {
            ordered_key_stream: key_stream,
            rows_scanned,
            optimization,
        })
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            Db,
            access::AccessPlan,
            direction::Direction,
            executor::{
                AccessExecutionDescriptor, AccessScanContinuationInput, AccessStreamBindings,
                Context, ExecutionOptimization, load::LoadExecutor,
            },
            registry::StoreRegistry,
        },
        error::ErrorClass,
        model::field::FieldKind,
        types::Ulid,
    };
    use icydb_derive::FieldProjection;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
    struct FastStreamInvariantEntity {
        id: Ulid,
    }

    crate::test_canister! {
        ident = FastStreamInvariantCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = FastStreamInvariantStore,
        canister = FastStreamInvariantCanister,
    }

    crate::test_entity_schema! {
        ident = FastStreamInvariantEntity,
        id = Ulid,
        id_field = id,
        entity_name = "FastStreamInvariantEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [("id", FieldKind::Ulid)],
        indexes = [],
        store = FastStreamInvariantStore,
        canister = FastStreamInvariantCanister,
    }

    thread_local! {
        static FAST_STREAM_INVARIANT_REGISTRY: StoreRegistry = StoreRegistry::new();
    }

    static FAST_STREAM_INVARIANT_DB: Db<FastStreamInvariantCanister> =
        Db::new(&FAST_STREAM_INVARIANT_REGISTRY);

    #[test]
    fn fast_stream_requires_exact_key_count_hint() {
        let ctx = Context::<FastStreamInvariantEntity>::new(&FAST_STREAM_INVARIANT_DB);
        let id1 = Ulid::from_u128(1);
        let id2 = Ulid::from_u128(2);
        let access = AccessPlan::Union(vec![AccessPlan::by_key(id1), AccessPlan::by_key(id2)]);
        let descriptor = AccessExecutionDescriptor::from_executable_bindings(
            access.resolve_strategy().into_executable(),
            AccessStreamBindings {
                index_prefix_specs: &[],
                index_range_specs: &[],
                continuation: AccessScanContinuationInput::new(None, Direction::Asc),
            },
            None,
            None,
        );

        let Err(err) = LoadExecutor::<FastStreamInvariantEntity>::execute_fast_stream_request(
            &ctx,
            descriptor,
            ExecutionOptimization::PrimaryKey,
        ) else {
            panic!("fast-path execution must reject streams without exact count hints")
        };

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "missing exact-count hint must classify as invariant violation"
        );
        assert!(
            err.message
                .contains("fast-path stream must expose an exact key-count hint"),
            "missing exact-count hint must emit a clear invariant message"
        );
    }
}
