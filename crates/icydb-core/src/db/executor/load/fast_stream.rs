use crate::{
    db::{
        Context,
        executor::load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        executor::{AccessPlanStreamRequest, route::RoutedKeyStreamRequest},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Resolve one fast-path access stream without materialize/restream adapters.
    // Fast-path streams are expected to expose an exact key-count hint.
    pub(super) fn execute_fast_stream_request(
        ctx: &Context<'_, E>,
        stream_request: AccessPlanStreamRequest<'_, E::Key>,
        optimization: ExecutionOptimization,
    ) -> Result<FastPathKeyResult, InternalError> {
        let key_stream = Self::resolve_routed_key_stream(
            ctx,
            RoutedKeyStreamRequest::AccessPlan(stream_request),
        )?;
        let rows_scanned = key_stream.exact_key_count_hint().ok_or_else(|| {
            InternalError::query_executor_invariant(
                "fast-path stream must expose an exact key-count hint",
            )
        })?;

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
            direction::Direction,
            executor::{
                AccessPlanStreamRequest, AccessStreamBindings, Context, KeyOrderComparator,
                load::{ExecutionOptimization, LoadExecutor},
            },
            query::plan::{AccessPath, AccessPlan},
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
        commit_memory_id = 253,
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
        let access = AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKey(id1)),
            AccessPlan::path(AccessPath::ByKey(id2)),
        ]);
        let request = AccessPlanStreamRequest {
            access: &access,
            bindings: AccessStreamBindings {
                index_prefix_specs: &[],
                index_range_specs: &[],
                index_range_anchor: None,
                direction: Direction::Asc,
            },
            key_comparator: KeyOrderComparator::from_direction(Direction::Asc),
            physical_fetch_hint: None,
            index_predicate_execution: None,
        };

        let Err(err) = LoadExecutor::<FastStreamInvariantEntity>::execute_fast_stream_request(
            &ctx,
            request,
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
