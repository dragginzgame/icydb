use crate::{
    db::{
        index::{
            IndexId, IndexKey, IndexKeyKind, IndexRangeBoundEncodeError, RawIndexKey,
            raw_bounds_for_index_component_range,
        },
        query::{
            intent::QueryMode,
            plan::{
                AccessPath, AccessPlan, ContinuationSignature, CursorBoundary, ExplainPlan,
                IndexRangeCursorAnchor, LogicalPlan, PlanError, PlanFingerprint,
                continuation::{decode_typed_primary_key_cursor_slot, decode_validated_cursor},
            },
            policy,
        },
    },
    traits::{EntityKind, FieldValue},
};
use std::{marker::PhantomData, ops::Bound};

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PlannedCursor {
    boundary: Option<CursorBoundary>,
    index_range_anchor: Option<RawIndexKey>,
}

impl PlannedCursor {
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            boundary: None,
            index_range_anchor: None,
        }
    }

    #[must_use]
    const fn new(boundary: CursorBoundary, index_range_anchor: Option<RawIndexKey>) -> Self {
        Self {
            boundary: Some(boundary),
            index_range_anchor,
        }
    }

    #[must_use]
    pub(in crate::db) const fn boundary(&self) -> Option<&CursorBoundary> {
        self.boundary.as_ref()
    }

    #[must_use]
    pub(in crate::db) const fn index_range_anchor(&self) -> Option<&RawIndexKey> {
        self.index_range_anchor.as_ref()
    }
}

impl From<Option<CursorBoundary>> for PlannedCursor {
    fn from(value: Option<CursorBoundary>) -> Self {
        Self {
            boundary: value,
            index_range_anchor: None,
        }
    }
}

#[derive(Debug)]
pub struct ExecutablePlan<E: EntityKind> {
    plan: LogicalPlan<E::Key>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> ExecutablePlan<E> {
    pub(crate) const fn new(plan: LogicalPlan<E::Key>) -> Self {
        Self {
            plan,
            _marker: PhantomData,
        }
    }

    /// Explain this plan without executing it.
    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.plan.explain_with_model(E::MODEL)
    }

    /// Compute a stable fingerprint for this plan.
    #[must_use]
    pub fn fingerprint(&self) -> PlanFingerprint {
        self.plan.fingerprint()
    }

    /// Compute a stable continuation signature for cursor compatibility checks.
    ///
    /// Unlike `fingerprint()`, this excludes window state such as `limit`/`offset`.
    #[must_use]
    pub fn continuation_signature(&self) -> ContinuationSignature {
        self.plan.continuation_signature(E::PATH)
    }

    /// Validate and decode a continuation cursor into executor-ready cursor state.
    pub(in crate::db) fn plan_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, PlanError>
    where
        E::Key: FieldValue,
    {
        let Some(cursor) = cursor else {
            return Ok(PlannedCursor::none());
        };
        let order =
            policy::require_cursor_order(self.plan.order.as_ref()).map_err(PlanError::from)?;

        let decoded = decode_validated_cursor(
            cursor,
            E::PATH,
            E::MODEL,
            order,
            self.continuation_signature(),
        )?;
        self.validate_index_range_anchor(decoded.index_range_anchor(), self.plan.access.as_path())?;
        let boundary = decoded.boundary().clone();

        // Typed key decode is the final authority for PK cursor slots.
        let _pk_key = decode_typed_primary_key_cursor_slot::<E::Key>(E::MODEL, order, &boundary)?;

        let index_range_anchor = decoded
            .index_range_anchor()
            .map(|anchor| anchor.last_raw_key().clone());

        Ok(PlannedCursor::new(boundary, index_range_anchor))
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> QueryMode {
        self.plan.mode
    }

    pub(in crate::db) const fn access(&self) -> &AccessPlan<E::Key> {
        &self.plan.access
    }

    pub(in crate::db) fn into_inner(self) -> LogicalPlan<E::Key> {
        self.plan
    }

    #[expect(clippy::unused_self)]
    fn validate_index_range_anchor(
        &self,
        anchor: Option<&IndexRangeCursorAnchor>,
        access: Option<&AccessPath<E::Key>>,
    ) -> Result<(), PlanError> {
        let Some(access) = access else {
            if anchor.is_some() {
                return Err(PlanError::InvalidContinuationCursorPayload {
                    reason: "unexpected index-range continuation anchor for composite access plan"
                        .to_string(),
                });
            }

            return Ok(());
        };

        if let AccessPath::IndexRange {
            index,
            prefix,
            lower,
            upper,
        } = access
        {
            let Some(anchor) = anchor else {
                return Err(PlanError::InvalidContinuationCursorPayload {
                    reason: "index-range continuation cursor is missing a raw-key anchor"
                        .to_string(),
                });
            };

            let decoded_key = IndexKey::try_from_raw(anchor.last_raw_key()).map_err(|err| {
                PlanError::InvalidContinuationCursorPayload {
                    reason: format!("index-range continuation anchor decode failed: {err}"),
                }
            })?;
            let expected_index_id = IndexId::new::<E>(index);

            if decoded_key.index_id() != &expected_index_id {
                return Err(PlanError::InvalidContinuationCursorPayload {
                    reason: "index-range continuation anchor index id mismatch".to_string(),
                });
            }
            if decoded_key.key_kind() != IndexKeyKind::User {
                return Err(PlanError::InvalidContinuationCursorPayload {
                    reason: "index-range continuation anchor key namespace mismatch".to_string(),
                });
            }
            if decoded_key.component_count() != index.fields.len() {
                return Err(PlanError::InvalidContinuationCursorPayload {
                    reason: "index-range continuation anchor component arity mismatch".to_string(),
                });
            }
            let (range_start, range_end) = raw_bounds_for_index_component_range::<E>(
                index, prefix, lower, upper,
            )
            .map_err(|err| {
                let reason = match err {
                    IndexRangeBoundEncodeError::Prefix => {
                        "index-range continuation anchor prefix is not indexable".to_string()
                    }
                    IndexRangeBoundEncodeError::Lower => {
                        "index-range cursor lower continuation bound is not indexable".to_string()
                    }
                    IndexRangeBoundEncodeError::Upper => {
                        "index-range cursor upper continuation bound is not indexable".to_string()
                    }
                };
                PlanError::InvalidContinuationCursorPayload { reason }
            })?;

            if !raw_key_within_bounds(anchor.last_raw_key(), &range_start, &range_end) {
                return Err(PlanError::InvalidContinuationCursorPayload {
                    reason:
                        "index-range continuation anchor is outside the original range envelope"
                            .to_string(),
                });
            }
        } else if anchor.is_some() {
            return Err(PlanError::InvalidContinuationCursorPayload {
                reason:
                    "unexpected index-range continuation anchor for non-index-range access path"
                        .to_string(),
            });
        }

        Ok(())
    }
}

fn raw_key_within_bounds(
    key: &RawIndexKey,
    lower: &Bound<RawIndexKey>,
    upper: &Bound<RawIndexKey>,
) -> bool {
    let lower_ok = match lower {
        Bound::Unbounded => true,
        Bound::Included(boundary) => key >= boundary,
        Bound::Excluded(boundary) => key > boundary,
    };
    let upper_ok = match upper {
        Bound::Unbounded => true,
        Bound::Included(boundary) => key <= boundary,
        Bound::Excluded(boundary) => key < boundary,
    };

    lower_ok && upper_ok
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            ReadConsistency,
            index::{IndexId, IndexKey, encode_canonical_index_component},
            query::plan::{AccessPath, IndexRangeCursorAnchor, LogicalPlan, PlanError},
        },
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel},
            index::IndexModel,
        },
        test_fixtures::entity_model_from_static,
        traits::{
            AsView, CanisterKind, EntityIdentity, EntityKey, EntityKind, EntityPlacement,
            EntitySchema, Path, SanitizeAuto, SanitizeCustom, StoreKind, ValidateAuto,
            ValidateCustom, Visitable,
        },
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};
    use std::ops::Bound;

    use super::ExecutablePlan;

    const RANGE_INDEX_FIELDS_AB: [&str; 2] = ["a", "b"];
    const RANGE_INDEX_FIELDS_AC: [&str; 2] = ["a", "c"];
    const RANGE_INDEX_AB: IndexModel = IndexModel::new(
        "executable::idx_ab",
        "executable::RangeStoreAB",
        &RANGE_INDEX_FIELDS_AB,
        false,
    );
    const RANGE_INDEX_AC: IndexModel = IndexModel::new(
        "executable::idx_ac",
        "executable::RangeStoreAC",
        &RANGE_INDEX_FIELDS_AC,
        false,
    );

    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct ExecutableAnchorEntity;

    impl AsView for ExecutableAnchorEntity {
        type ViewType = Self;

        fn as_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for ExecutableAnchorEntity {}
    impl SanitizeCustom for ExecutableAnchorEntity {}
    impl ValidateAuto for ExecutableAnchorEntity {}
    impl ValidateCustom for ExecutableAnchorEntity {}
    impl Visitable for ExecutableAnchorEntity {}

    impl Path for ExecutableAnchorEntity {
        const PATH: &'static str = "executable::AnchorEntity";
    }

    impl EntityKey for ExecutableAnchorEntity {
        type Key = Ulid;
    }

    impl EntityIdentity for ExecutableAnchorEntity {
        const ENTITY_NAME: &'static str = "ExecutableAnchorEntity";
        const PRIMARY_KEY: &'static str = "id";
    }

    static EXECUTABLE_ANCHOR_FIELDS: [FieldModel; 4] = [
        FieldModel {
            name: "id",
            kind: FieldKind::Ulid,
        },
        FieldModel {
            name: "a",
            kind: FieldKind::Uint,
        },
        FieldModel {
            name: "b",
            kind: FieldKind::Uint,
        },
        FieldModel {
            name: "c",
            kind: FieldKind::Uint,
        },
    ];
    static EXECUTABLE_ANCHOR_FIELD_NAMES: [&str; 4] = ["id", "a", "b", "c"];
    static EXECUTABLE_ANCHOR_INDEXES: [&IndexModel; 2] = [&RANGE_INDEX_AB, &RANGE_INDEX_AC];
    static EXECUTABLE_ANCHOR_MODEL: EntityModel = entity_model_from_static(
        "executable::AnchorEntity",
        "ExecutableAnchorEntity",
        &EXECUTABLE_ANCHOR_FIELDS[0],
        &EXECUTABLE_ANCHOR_FIELDS,
        &EXECUTABLE_ANCHOR_INDEXES,
    );

    impl EntitySchema for ExecutableAnchorEntity {
        const MODEL: &'static EntityModel = &EXECUTABLE_ANCHOR_MODEL;
        const FIELDS: &'static [&'static str] = &EXECUTABLE_ANCHOR_FIELD_NAMES;
        const INDEXES: &'static [&'static IndexModel] = &EXECUTABLE_ANCHOR_INDEXES;
    }

    struct ExecutableAnchorCanister;
    struct ExecutableAnchorStore;

    impl Path for ExecutableAnchorCanister {
        const PATH: &'static str = "executable::AnchorCanister";
    }

    impl CanisterKind for ExecutableAnchorCanister {}

    impl Path for ExecutableAnchorStore {
        const PATH: &'static str = "executable::AnchorStore";
    }

    impl StoreKind for ExecutableAnchorStore {
        type Canister = ExecutableAnchorCanister;
    }

    impl EntityPlacement for ExecutableAnchorEntity {
        type Store = ExecutableAnchorStore;
        type Canister = ExecutableAnchorCanister;
    }

    impl EntityKind for ExecutableAnchorEntity {}

    fn build_executable() -> ExecutablePlan<ExecutableAnchorEntity> {
        let plan: LogicalPlan<Ulid> =
            LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
        ExecutablePlan::new(plan)
    }

    fn index_range_access() -> AccessPath<Ulid> {
        AccessPath::IndexRange {
            index: RANGE_INDEX_AB,
            prefix: vec![Value::Uint(42)],
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Included(Value::Uint(20)),
        }
    }

    fn anchor_for_value(index_id: &IndexId, second_component: u64) -> IndexRangeCursorAnchor {
        let prefix_component =
            encode_canonical_index_component(&Value::Uint(42)).expect("prefix must encode");
        let range_component = encode_canonical_index_component(&Value::Uint(second_component))
            .expect("range component must encode");
        let (start, _) = IndexKey::bounds_for_prefix_component_range(
            index_id,
            2,
            &[prefix_component],
            Bound::Included(range_component.clone()),
            Bound::Included(range_component),
        );
        let raw_key = match start {
            Bound::Included(key) | Bound::Excluded(key) => key.to_raw(),
            Bound::Unbounded => panic!("test fixture produced unbounded lower key"),
        };

        IndexRangeCursorAnchor::new(raw_key)
    }

    #[test]
    fn index_range_anchor_validation_accepts_anchor_in_range() {
        let executable = build_executable();
        let access = index_range_access();
        let expected_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AB);
        let anchor = anchor_for_value(&expected_id, 15);

        executable
            .validate_index_range_anchor(Some(&anchor), Some(&access))
            .expect("anchor inside index-range envelope should validate");
    }

    #[test]
    fn index_range_anchor_validation_rejects_mismatched_index_id() {
        let executable = build_executable();
        let access = index_range_access();
        let other_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AC);
        let anchor = anchor_for_value(&other_id, 15);

        let err = executable
            .validate_index_range_anchor(Some(&anchor), Some(&access))
            .expect_err("anchor from a different index id must fail");
        match err {
            PlanError::InvalidContinuationCursorPayload { reason } => {
                assert!(reason.contains("index id mismatch"));
            }
            _ => panic!("expected InvalidContinuationCursorPayload"),
        }
    }

    #[test]
    fn index_range_anchor_validation_rejects_out_of_envelope_anchor() {
        let executable = build_executable();
        let access = index_range_access();
        let expected_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AB);
        let anchor = anchor_for_value(&expected_id, 99);

        let err = executable
            .validate_index_range_anchor(Some(&anchor), Some(&access))
            .expect_err("anchor outside index-range envelope must fail");
        match err {
            PlanError::InvalidContinuationCursorPayload { reason } => {
                assert!(reason.contains("outside the original range envelope"));
            }
            _ => panic!("expected InvalidContinuationCursorPayload"),
        }
    }
}
