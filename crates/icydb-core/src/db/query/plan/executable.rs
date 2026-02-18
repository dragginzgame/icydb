use crate::{
    db::{
        data::StorageKey,
        index::{
            Direction, IndexId, IndexKey, IndexKeyKind, IndexRangeBoundEncodeError, RawIndexKey,
            anchor_within_envelope, raw_bounds_for_index_component_range,
        },
        query::{
            intent::QueryMode,
            plan::{
                AccessPath, AccessPlan, ContinuationSignature, CursorBoundary, ExplainPlan,
                IndexRangeCursorAnchor, LogicalPlan, PlanError, PlanFingerprint,
                continuation::{
                    decode_typed_primary_key_cursor_slot, decode_validated_cursor,
                    invalid_continuation_cursor_payload,
                },
            },
            policy,
        },
    },
    traits::{EntityKind, FieldValue},
};
use std::marker::PhantomData;

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
    direction: Direction,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> ExecutablePlan<E> {
    pub(crate) const fn new(plan: LogicalPlan<E::Key>) -> Self {
        Self {
            plan,
            direction: Direction::Asc,
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
            self.direction,
        )?;
        self.validate_index_range_anchor(
            decoded.index_range_anchor(),
            self.plan.access.as_path(),
            self.direction,
        )?;
        let boundary = decoded.boundary().clone();

        // Typed key decode is the final authority for PK cursor slots.
        let pk_key = decode_typed_primary_key_cursor_slot::<E::Key>(E::MODEL, order, &boundary)?;
        self.validate_index_range_boundary_anchor_consistency(
            decoded.index_range_anchor(),
            self.plan.access.as_path(),
            pk_key,
        )?;

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

    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    pub(in crate::db) fn into_inner(self) -> LogicalPlan<E::Key> {
        self.plan
    }

    #[expect(clippy::unused_self)]
    fn validate_index_range_anchor(
        &self,
        anchor: Option<&IndexRangeCursorAnchor>,
        access: Option<&AccessPath<E::Key>>,
        direction: Direction,
    ) -> Result<(), PlanError> {
        let Some(access) = access else {
            if anchor.is_some() {
                return Err(invalid_continuation_cursor_payload(
                    "unexpected index-range continuation anchor for composite access plan",
                ));
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
                return Err(invalid_continuation_cursor_payload(
                    "index-range continuation cursor is missing a raw-key anchor",
                ));
            };

            let decoded_key = IndexKey::try_from_raw(anchor.last_raw_key()).map_err(|err| {
                invalid_continuation_cursor_payload(format!(
                    "index-range continuation anchor decode failed: {err}"
                ))
            })?;
            let expected_index_id = IndexId::new::<E>(index);

            if decoded_key.index_id() != &expected_index_id {
                return Err(invalid_continuation_cursor_payload(
                    "index-range continuation anchor index id mismatch",
                ));
            }
            if decoded_key.key_kind() != IndexKeyKind::User {
                return Err(invalid_continuation_cursor_payload(
                    "index-range continuation anchor key namespace mismatch",
                ));
            }
            if decoded_key.component_count() != index.fields.len() {
                return Err(invalid_continuation_cursor_payload(
                    "index-range continuation anchor component arity mismatch",
                ));
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
                invalid_continuation_cursor_payload(reason)
            })?;

            if !anchor_within_envelope(direction, anchor.last_raw_key(), &range_start, &range_end) {
                return Err(invalid_continuation_cursor_payload(
                    "index-range continuation anchor is outside the original range envelope",
                ));
            }
        } else if anchor.is_some() {
            return Err(invalid_continuation_cursor_payload(
                "unexpected index-range continuation anchor for non-index-range access path",
            ));
        }

        Ok(())
    }

    #[expect(clippy::unused_self)]
    fn validate_index_range_boundary_anchor_consistency(
        &self,
        anchor: Option<&IndexRangeCursorAnchor>,
        access: Option<&AccessPath<E::Key>>,
        boundary_pk_key: E::Key,
    ) -> Result<(), PlanError> {
        let Some(anchor) = anchor else {
            return Ok(());
        };
        let Some(AccessPath::IndexRange { .. }) = access else {
            return Ok(());
        };

        let anchor_key = IndexKey::try_from_raw(anchor.last_raw_key()).map_err(|err| {
            invalid_continuation_cursor_payload(format!(
                "index-range continuation anchor decode failed: {err}"
            ))
        })?;
        let anchor_storage_key = anchor_key.primary_storage_key().map_err(|err| {
            invalid_continuation_cursor_payload(format!(
                "index-range continuation anchor primary key decode failed: {err}"
            ))
        })?;
        let boundary_storage_key = StorageKey::try_from_value(&boundary_pk_key.to_value())
            .map_err(|err| {
                invalid_continuation_cursor_payload(format!(
                    "index-range continuation boundary primary key decode failed: {err}"
                ))
            })?;

        if anchor_storage_key != boundary_storage_key {
            return Err(invalid_continuation_cursor_payload(
                "index-range continuation boundary/anchor mismatch",
            ));
        }

        Ok(())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            ReadConsistency,
            data::StorageKey,
            index::{
                Direction, IndexId, IndexKey, IndexKeyKind, RawIndexKey,
                encode_canonical_index_component,
            },
            query::plan::{
                AccessPath, ContinuationToken, CursorBoundary, CursorBoundarySlot,
                IndexRangeCursorAnchor, LogicalPlan, OrderDirection, OrderSpec, PlanError,
            },
        },
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel},
            index::IndexModel,
        },
        test_fixtures::entity_model_from_static,
        traits::{
            AsView, CanisterKind, EntityIdentity, EntityKey, EntityKind, EntityPlacement,
            EntitySchema, Path, SanitizeAuto, SanitizeCustom, Storable, StoreKind, ValidateAuto,
            ValidateCustom, Visitable,
        },
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};
    use std::borrow::Cow;
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

    fn build_index_range_cursor_executable() -> ExecutablePlan<ExecutableAnchorEntity> {
        let mut plan: LogicalPlan<Ulid> =
            LogicalPlan::new(index_range_access(), ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
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

    fn anchor_for_value_with_pk(
        index_id: &IndexId,
        second_component: u64,
        pk: Ulid,
    ) -> IndexRangeCursorAnchor {
        let mut bytes = Vec::new();
        bytes.push(IndexKeyKind::User as u8);
        bytes.extend_from_slice(&index_id.0.to_bytes());
        bytes.push(2u8);

        let prefix_component =
            encode_canonical_index_component(&Value::Uint(42)).expect("prefix must encode");
        push_segment(&mut bytes, &prefix_component);

        let range_component = encode_canonical_index_component(&Value::Uint(second_component))
            .expect("range component must encode");
        push_segment(&mut bytes, &range_component);

        let storage_key = StorageKey::try_from_value(&Value::Ulid(pk)).expect("pk must encode");
        let storage_key_bytes = storage_key
            .to_bytes()
            .expect("storage key bytes must encode");
        push_segment(&mut bytes, &storage_key_bytes);

        IndexRangeCursorAnchor::new(<RawIndexKey as Storable>::from_bytes(Cow::Owned(bytes)))
    }

    fn push_segment(bytes: &mut Vec<u8>, segment: &[u8]) {
        let len_u16 = u16::try_from(segment.len()).expect("segment length must fit u16");
        bytes.extend_from_slice(&len_u16.to_be_bytes());
        bytes.extend_from_slice(segment);
    }

    #[test]
    fn index_range_anchor_validation_accepts_anchor_in_range() {
        let executable = build_executable();
        let access = index_range_access();
        let expected_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AB);
        let anchor = anchor_for_value(&expected_id, 15);

        executable
            .validate_index_range_anchor(Some(&anchor), Some(&access), Direction::Asc)
            .expect("anchor inside index-range envelope should validate");
    }

    #[test]
    fn index_range_anchor_validation_rejects_mismatched_index_id() {
        let executable = build_executable();
        let access = index_range_access();
        let other_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AC);
        let anchor = anchor_for_value(&other_id, 15);

        let err = executable
            .validate_index_range_anchor(Some(&anchor), Some(&access), Direction::Asc)
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
            .validate_index_range_anchor(Some(&anchor), Some(&access), Direction::Asc)
            .expect_err("anchor outside index-range envelope must fail");
        match err {
            PlanError::InvalidContinuationCursorPayload { reason } => {
                assert!(reason.contains("outside the original range envelope"));
            }
            _ => panic!("expected InvalidContinuationCursorPayload"),
        }
    }

    #[test]
    fn plan_cursor_rejects_index_range_boundary_anchor_mismatch() {
        let executable = build_index_range_cursor_executable();
        let expected_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AB);
        let boundary_pk = Ulid::from_u128(10_001);
        let anchor_pk = Ulid::from_u128(10_002);
        let anchor = anchor_for_value_with_pk(&expected_id, 15, anchor_pk);
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Ulid(boundary_pk))],
        };
        let token = ContinuationToken::new_index_range_with_direction(
            executable.continuation_signature(),
            boundary,
            anchor,
            Direction::Asc,
        )
        .encode()
        .expect("cursor token should encode");

        let err = executable
            .plan_cursor(Some(token.as_slice()))
            .expect_err("boundary/anchor mismatch must fail");
        match err {
            PlanError::InvalidContinuationCursorPayload { reason } => {
                assert!(reason.contains("boundary/anchor mismatch"));
            }
            _ => panic!("expected InvalidContinuationCursorPayload"),
        }
    }
}
