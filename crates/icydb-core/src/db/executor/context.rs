use crate::{
    db::{
        Db,
        data::{
            DataKey, DataRow, DataStore, RawDataKey, RawRow, decode_and_validate_entity_key,
            format_entity_key_for_mismatch,
        },
        executor::{ExecutorError, OrderedKeyStream},
        query::ReadConsistency,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, Path},
    types::Id,
};
use std::{collections::BTreeSet, marker::PhantomData};

// -----------------------------------------------------------------------------
// Context Subdomains
// -----------------------------------------------------------------------------
// 1) Context handle and store access.
// 2) Row reads and consistency-aware materialization.
// 3) Key/spec helper utilities and decoding invariants.

///
/// Context
///

pub(crate) struct Context<'a, E: EntityKind + EntityValue> {
    pub db: &'a Db<E::Canister>,
    _marker: PhantomData<E>,
}

impl<'a, E> Context<'a, E>
where
    E: EntityKind + EntityValue,
{
    // ------------------------------------------------------------------
    // Context setup
    // ------------------------------------------------------------------

    #[must_use]
    pub(crate) const fn new(db: &'a Db<E::Canister>) -> Self {
        Self {
            db,
            _marker: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // Store access
    // ------------------------------------------------------------------

    pub(crate) fn with_store<R>(
        &self,
        f: impl FnOnce(&DataStore) -> R,
    ) -> Result<R, InternalError> {
        self.db.with_store_registry(|reg| {
            reg.try_get_store(E::Store::PATH)
                .map(|store| store.with_data(f))
        })
    }

    // ------------------------------------------------------------------
    // Row reads
    // ------------------------------------------------------------------

    pub(crate) fn read(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw)
                .ok_or_else(|| InternalError::store_not_found(key.to_string()))
        })?
    }

    pub(crate) fn read_strict(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw).ok_or_else(|| {
                ExecutorError::store_corruption(format!("missing row: {key}")).into()
            })
        })?
    }

    // Load rows for an ordered key stream by preserving the stream order.
    pub(crate) fn rows_from_ordered_key_stream(
        &self,
        key_stream: &mut dyn OrderedKeyStream,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let keys = Self::collect_ordered_keys(key_stream)?;

        self.load_many_with_consistency(&keys, consistency)
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    pub(super) fn data_key_from_key(key: E::Key) -> Result<DataKey, InternalError>
    where
        E: EntityKind,
    {
        DataKey::try_new::<E>(key)
    }

    pub(super) fn dedup_keys(keys: Vec<E::Key>) -> Vec<E::Key> {
        let mut set = BTreeSet::new();
        set.extend(keys);
        set.into_iter().collect()
    }

    fn collect_ordered_keys(
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<Vec<DataKey>, InternalError> {
        let mut keys = Vec::new();
        while let Some(key) = key_stream.next_key()? {
            keys.push(key);
        }

        Ok(keys)
    }

    fn load_many_with_consistency(
        &self,
        keys: &[DataKey],
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let row = match consistency {
                ReadConsistency::Strict => self.read_strict(key),
                ReadConsistency::MissingOk => self.read(key),
            };

            match row {
                Ok(row) => out.push((key.clone(), row)),
                Err(err) if err.is_not_found() => {}
                Err(err) => return Err(err),
            }
        }

        Ok(out)
    }

    pub(super) fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw).map_err(|err| ExecutorError::store_corruption_from(err).into())
    }

    pub(crate) fn deserialize_rows(rows: Vec<DataRow>) -> Result<Vec<(Id<E>, E)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        rows.into_iter()
            .map(|(key, row)| {
                let expected_key = key.try_key::<E>()?;
                let entity = decode_and_validate_entity_key::<E, _, _, _, _>(
                    expected_key,
                    || row.try_decode::<E>(),
                    |err| {
                        ExecutorError::serialize_corruption(format!(
                            "failed to deserialize row: {key} ({err})"
                        ))
                        .into()
                    },
                    |expected_key, actual_key| {
                        let expected = format_entity_key_for_mismatch::<E>(expected_key);
                        let found = format_entity_key_for_mismatch::<E>(actual_key);

                        ExecutorError::store_corruption(format!(
                            "row key mismatch: expected {expected}, found {found}"
                        ))
                        .into()
                    },
                )?;

                Ok((Id::from_key(expected_key), entity))
            })
            .collect()
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
            executor::{Context, IndexStreamConstraints, StreamExecutionHints},
            query::{
                ReadConsistency,
                plan::{AccessPath, AccessPlan, Direction, IndexPrefixSpec, IndexRangeSpec},
            },
            registry::StoreRegistry,
        },
        model::{field::FieldKind, index::IndexModel},
        traits::Storable,
        types::Ulid,
        value::Value,
    };
    use icydb_derive::FieldProjection;
    use serde::{Deserialize, Serialize};
    use std::{borrow::Cow, ops::Bound};

    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "context::idx_group_rank",
        "context::InvariantStore",
        &INDEX_FIELDS,
        false,
    );
    const INDEX_MODEL_ALT: IndexModel = IndexModel::new(
        "context::idx_group_rank_alt",
        "context::InvariantStore",
        &INDEX_FIELDS,
        false,
    );

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
    struct ContextInvariantEntity {
        id: Ulid,
        group: u32,
        rank: u32,
    }

    crate::test_canister! {
        ident = ContextInvariantCanister,
    }

    crate::test_store! {
        ident = ContextInvariantStore,
        canister = ContextInvariantCanister,
    }

    crate::test_entity_schema! {
        ident = ContextInvariantEntity,
        id = Ulid,
        id_field = id,
        entity_name = "ContextInvariantEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("group", FieldKind::Uint),
            ("rank", FieldKind::Uint),
        ],
        indexes = [&INDEX_MODEL],
        store = ContextInvariantStore,
        canister = ContextInvariantCanister,
    }

    thread_local! {
        static INVARIANT_STORE_REGISTRY: StoreRegistry = StoreRegistry::new();
    }

    static INVARIANT_DB: Db<ContextInvariantCanister> = Db::new(&INVARIANT_STORE_REGISTRY);

    fn raw_index_key(byte: u8) -> crate::db::index::RawIndexKey {
        <crate::db::index::RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
    }

    fn dummy_index_range_spec() -> IndexRangeSpec {
        IndexRangeSpec::new(
            INDEX_MODEL,
            Bound::Included(raw_index_key(0x01)),
            Bound::Included(raw_index_key(0x02)),
        )
    }

    fn dummy_index_prefix_spec() -> IndexPrefixSpec {
        IndexPrefixSpec::new(
            INDEX_MODEL,
            Bound::Included(raw_index_key(0x01)),
            Bound::Included(raw_index_key(0x02)),
        )
    }

    #[test]
    fn index_range_path_requires_pre_lowered_spec() {
        let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
        let access = AccessPath::IndexRange {
            index: INDEX_MODEL,
            prefix: vec![Value::Uint(7)],
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Excluded(Value::Uint(20)),
        };

        let Err(err) = ctx.ordered_key_stream_from_access(
            &access,
            IndexStreamConstraints {
                prefix: None,
                range: None,
                anchor: None,
            },
            Direction::Asc,
            StreamExecutionHints {
                physical_fetch_hint: None,
                predicate_execution: None,
            },
        ) else {
            panic!("index-range access without lowered spec must fail")
        };

        assert!(
            err.to_string()
                .contains("index-range execution requires pre-lowered index-range spec"),
            "missing-spec error must be classified as an executor invariant"
        );
    }

    #[test]
    fn index_prefix_path_rejects_misaligned_spec_for_direct_resolution() {
        let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
        let access = AccessPath::IndexPrefix {
            index: INDEX_MODEL_ALT,
            values: vec![Value::Uint(7)],
        };
        let spec = dummy_index_prefix_spec();

        let Err(err) = ctx.ordered_key_stream_from_access(
            &access,
            IndexStreamConstraints {
                prefix: Some(&spec),
                range: None,
                anchor: None,
            },
            Direction::Asc,
            StreamExecutionHints {
                physical_fetch_hint: None,
                predicate_execution: None,
            },
        ) else {
            panic!("misaligned index-prefix spec must fail invariant checks")
        };

        assert!(
            err.to_string()
                .contains("index-prefix spec does not match access path index"),
            "misaligned prefix spec must fail fast before touching index storage"
        );
    }

    #[test]
    fn index_range_path_rejects_misaligned_spec_for_direct_resolution() {
        let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
        let access = AccessPath::IndexRange {
            index: INDEX_MODEL_ALT,
            prefix: vec![Value::Uint(7)],
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Excluded(Value::Uint(20)),
        };
        let spec = dummy_index_range_spec();

        let Err(err) = ctx.ordered_key_stream_from_access(
            &access,
            IndexStreamConstraints {
                prefix: None,
                range: Some(&spec),
                anchor: None,
            },
            Direction::Asc,
            StreamExecutionHints {
                physical_fetch_hint: None,
                predicate_execution: None,
            },
        ) else {
            panic!("misaligned index-range spec must fail invariant checks")
        };

        assert!(
            err.to_string()
                .contains("index-range spec does not match access path index"),
            "misaligned range spec must fail fast before touching index storage"
        );
    }

    #[test]
    fn access_plan_rejects_unused_index_range_specs() {
        let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
        let access = AccessPlan::path(AccessPath::ByKey(Ulid::from_u128(1)));
        let extra_prefix_spec = dummy_index_prefix_spec();
        let extra_spec = dummy_index_range_spec();

        let err = ctx
            .rows_from_access_plan(
                &access,
                &[extra_prefix_spec],
                &[extra_spec],
                ReadConsistency::MissingOk,
            )
            .expect_err("unused index-range specs must fail invariant checks");

        assert!(
            err.to_string()
                .contains("unused index-prefix executable specs after access-plan traversal"),
            "unused-spec error must be classified as an executor invariant"
        );
    }

    #[test]
    fn access_plan_rejects_misaligned_index_prefix_spec() {
        let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
        let access = AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL_ALT,
            values: vec![Value::Uint(7)],
        });
        let prefix_spec = dummy_index_prefix_spec();

        let err = ctx
            .rows_from_access_plan(&access, &[prefix_spec], &[], ReadConsistency::MissingOk)
            .expect_err("misaligned index-prefix spec must fail invariant checks");

        assert!(
            err.to_string()
                .contains("index-prefix spec does not match access path index"),
            "misaligned prefix spec must fail fast before execution"
        );
    }

    #[test]
    fn access_plan_rejects_misaligned_index_range_spec() {
        let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
        let access = AccessPlan::path(AccessPath::IndexRange {
            index: INDEX_MODEL_ALT,
            prefix: vec![Value::Uint(7)],
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Excluded(Value::Uint(20)),
        });
        let range_spec = dummy_index_range_spec();

        let err = ctx
            .rows_from_access_plan(&access, &[], &[range_spec], ReadConsistency::MissingOk)
            .expect_err("misaligned index-range spec must fail invariant checks");

        assert!(
            err.to_string()
                .contains("index-range spec does not match access path index"),
            "misaligned range spec must fail fast before execution"
        );
    }

    #[test]
    fn composite_union_rejects_misaligned_index_prefix_spec() {
        let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
        let access = AccessPlan::Union(vec![AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL_ALT,
            values: vec![Value::Uint(7)],
        })]);
        let prefix_spec = dummy_index_prefix_spec();

        let err = ctx
            .rows_from_access_plan(&access, &[prefix_spec], &[], ReadConsistency::MissingOk)
            .expect_err("misaligned composite prefix spec must fail invariant checks");

        assert!(
            err.to_string()
                .contains("index-prefix spec does not match access path index"),
            "misaligned composite prefix spec must fail fast before execution"
        );
    }

    #[test]
    fn composite_intersection_rejects_misaligned_index_range_spec() {
        let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
        let access = AccessPlan::Intersection(vec![AccessPlan::path(AccessPath::IndexRange {
            index: INDEX_MODEL_ALT,
            prefix: vec![Value::Uint(7)],
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Excluded(Value::Uint(20)),
        })]);
        let range_spec = dummy_index_range_spec();

        let err = ctx
            .rows_from_access_plan(&access, &[], &[range_spec], ReadConsistency::MissingOk)
            .expect_err("misaligned composite range spec must fail invariant checks");

        assert!(
            err.to_string()
                .contains("index-range spec does not match access path index"),
            "misaligned composite range spec must fail fast before execution"
        );
    }

    #[test]
    fn dedup_keys_returns_canonical_order_for_directional_consumers() {
        let low = Ulid::from_u128(10);
        let mid = Ulid::from_u128(11);
        let high = Ulid::from_u128(12);
        let deduped =
            Context::<ContextInvariantEntity>::dedup_keys(vec![high, low, high, mid, low]);

        assert_eq!(
            deduped,
            vec![low, mid, high],
            "dedup_keys must emit canonical ascending key order for ByKeys consumers",
        );

        let mut desc = deduped;
        desc.reverse();
        assert_eq!(
            desc,
            vec![high, mid, low],
            "reversing deduped keys must produce canonical descending order",
        );
    }
}
