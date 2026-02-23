use crate::{
    db::{
        index::{
            Direction, EncodedValue, IndexRangeNotIndexableReasonScope, RawIndexKey,
            map_index_range_not_indexable_reason, raw_keys_for_encoded_prefix,
        },
        query::{
            intent::QueryMode,
            plan::{
                AccessPath, AccessPlan, ContinuationSignature, CursorBoundary, CursorPlanError,
                ExplainPlan, LogicalPlan, OrderSpec, PlanError, PlanFingerprint,
                SlotSelectionPolicy, derive_scan_direction,
                raw_bounds_for_semantic_index_component_range, validate_planned_cursor,
                validate_planned_cursor_state,
            },
            predicate::PredicateFieldSlots,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, FieldValue},
};
use std::{marker::PhantomData, ops::Bound};

// -----------------------------------------------------------------------------
// ExecutablePlan Subdomains (Pre-Split Planning)
// -----------------------------------------------------------------------------
// 1) Lowered executor spec contracts (`IndexPrefixSpec`, `IndexRangeSpec`).
// 2) Planned cursor state and continuation revalidation.
// 3) Core executable plan API and immutable plan accessors.
// 4) Index spec lowering collectors from semantic plan to raw-key bounds.
// 5) Cursor/index-range regression tests.

const INDEX_RANGE_SPEC_INVALID: &str =
    "validated index-range plan could not be lowered to raw bounds";
const INDEX_PREFIX_SPEC_VALUE_NOT_INDEXABLE: &str = "validated index-prefix value is not indexable";
const INDEX_PREFIX_SPEC_INVALID: &str =
    "validated index-prefix plan could not be lowered to raw bounds";

///
/// IndexPrefixSpec
///
/// Executor-lowered index-prefix contract with fully materialized raw-key bounds.
/// This keeps runtime prefix traversal mechanical and free of `Value` encoding.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexPrefixSpec {
    index: IndexModel,
    lower: Bound<RawIndexKey>,
    upper: Bound<RawIndexKey>,
}

impl IndexPrefixSpec {
    #[must_use]
    pub(in crate::db) const fn new(
        index: IndexModel,
        lower: Bound<RawIndexKey>,
        upper: Bound<RawIndexKey>,
    ) -> Self {
        Self {
            index,
            lower,
            upper,
        }
    }

    #[must_use]
    pub(in crate::db) const fn index(&self) -> &IndexModel {
        &self.index
    }

    #[must_use]
    pub(in crate::db) const fn lower(&self) -> &Bound<RawIndexKey> {
        &self.lower
    }

    #[must_use]
    pub(in crate::db) const fn upper(&self) -> &Bound<RawIndexKey> {
        &self.upper
    }
}

///
/// IndexRangeSpec
///
/// Executor-lowered index-range contract with fully materialized raw-key bounds.
/// This keeps runtime traversal mechanical and free of `Value` decoding/encoding.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexRangeSpec {
    index: IndexModel,
    lower: Bound<RawIndexKey>,
    upper: Bound<RawIndexKey>,
}

impl IndexRangeSpec {
    #[must_use]
    pub(in crate::db) const fn new(
        index: IndexModel,
        lower: Bound<RawIndexKey>,
        upper: Bound<RawIndexKey>,
    ) -> Self {
        Self {
            index,
            lower,
            upper,
        }
    }

    #[must_use]
    pub(in crate::db) const fn index(&self) -> &IndexModel {
        &self.index
    }

    #[must_use]
    pub(in crate::db) const fn lower(&self) -> &Bound<RawIndexKey> {
        &self.lower
    }

    #[must_use]
    pub(in crate::db) const fn upper(&self) -> &Bound<RawIndexKey> {
        &self.upper
    }
}

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PlannedCursor {
    boundary: Option<CursorBoundary>,
    index_range_anchor: Option<RawIndexKey>,
    initial_offset: u32,
}

impl PlannedCursor {
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            boundary: None,
            index_range_anchor: None,
            initial_offset: 0,
        }
    }

    #[must_use]
    pub(in crate::db) const fn new(
        boundary: CursorBoundary,
        index_range_anchor: Option<RawIndexKey>,
        initial_offset: u32,
    ) -> Self {
        Self {
            boundary: Some(boundary),
            index_range_anchor,
            initial_offset,
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

    #[must_use]
    pub(in crate::db) const fn initial_offset(&self) -> u32 {
        self.initial_offset
    }

    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.boundary.is_none() && self.index_range_anchor.is_none() && self.initial_offset == 0
    }
}

impl From<Option<CursorBoundary>> for PlannedCursor {
    fn from(value: Option<CursorBoundary>) -> Self {
        Self {
            boundary: value,
            index_range_anchor: None,
            initial_offset: 0,
        }
    }
}

#[derive(Debug)]
pub struct ExecutablePlan<E: EntityKind> {
    plan: LogicalPlan<E::Key>,
    predicate_slots: Option<PredicateFieldSlots>,
    direction: Direction,
    index_prefix_specs: Vec<IndexPrefixSpec>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Vec<IndexRangeSpec>,
    index_range_spec_invalid: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> ExecutablePlan<E> {
    // ------------------------------------------------------------------
    // Core executable plan construction and accessors
    // ------------------------------------------------------------------

    #[cfg(test)]
    pub(crate) fn new(plan: LogicalPlan<E::Key>) -> Self {
        let predicate_slots = plan
            .predicate
            .as_ref()
            .map(PredicateFieldSlots::resolve::<E>);

        Self::new_with_compiled_predicate_slots(plan, predicate_slots)
    }

    pub(in crate::db::query) fn new_with_compiled_predicate_slots(
        plan: LogicalPlan<E::Key>,
        predicate_slots: Option<PredicateFieldSlots>,
    ) -> Self {
        let direction = Self::derive_direction(&plan);
        let (index_prefix_specs, index_prefix_spec_invalid) =
            match Self::build_index_prefix_specs(&plan) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };
        let (index_range_specs, index_range_spec_invalid) =
            match Self::build_index_range_specs(&plan) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };

        Self {
            plan,
            predicate_slots,
            direction,
            index_prefix_specs,
            index_prefix_spec_invalid,
            index_range_specs,
            index_range_spec_invalid,
            _marker: PhantomData,
        }
    }

    fn derive_direction(plan: &LogicalPlan<E::Key>) -> Direction {
        let Some(order) = plan.order.as_ref() else {
            return Direction::Asc;
        };

        derive_scan_direction(order, SlotSelectionPolicy::First)
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
        let order = self
            .validated_cursor_order_plan()
            .map_err(PlanError::from)?;

        validate_planned_cursor::<E>(
            cursor,
            self.plan.access.as_path(),
            E::PATH,
            E::MODEL,
            order,
            self.continuation_signature(),
            self.direction,
            self.plan.effective_page_offset(None),
        )
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
    pub(in crate::db) const fn as_inner(&self) -> &LogicalPlan<E::Key> {
        &self.plan
    }

    pub(in crate::db) const fn predicate_slots(&self) -> Option<&PredicateFieldSlots> {
        self.predicate_slots.as_ref()
    }

    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    pub(in crate::db) fn index_prefix_specs(&self) -> Result<&[IndexPrefixSpec], InternalError> {
        if self.index_prefix_spec_invalid {
            return Err(InternalError::query_executor_invariant(
                INDEX_PREFIX_SPEC_INVALID,
            ));
        }

        Ok(self.index_prefix_specs.as_slice())
    }

    pub(in crate::db) fn index_range_specs(&self) -> Result<&[IndexRangeSpec], InternalError> {
        if self.index_range_spec_invalid {
            return Err(InternalError::query_executor_invariant(
                INDEX_RANGE_SPEC_INVALID,
            ));
        }

        Ok(self.index_range_specs.as_slice())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) fn into_inner(self) -> LogicalPlan<E::Key> {
        self.plan
    }

    pub(in crate::db) fn into_parts(self) -> (LogicalPlan<E::Key>, Option<PredicateFieldSlots>) {
        (self.plan, self.predicate_slots)
    }

    /// Revalidate executor-provided cursor state through the canonical cursor spine.
    pub(in crate::db) fn revalidate_planned_cursor(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError>
    where
        E::Key: FieldValue,
    {
        if cursor.is_empty() {
            return Ok(PlannedCursor::none());
        }

        let order = self.validated_cursor_order_internal()?;

        validate_planned_cursor_state::<E>(
            cursor,
            self.plan.access.as_path(),
            E::MODEL,
            order,
            self.direction,
            self.plan.effective_page_offset(None),
        )
        .map_err(InternalError::from_cursor_plan_error)
    }

    // ------------------------------------------------------------------
    // Cursor ordering and validation spine
    // ------------------------------------------------------------------

    // Resolve cursor ordering for plan-surface cursor decoding.
    // Cursor readiness is owned by policy/intent validation.
    fn validated_cursor_order_plan(&self) -> Result<&OrderSpec, CursorPlanError> {
        let Some(order) = self.plan.order.as_ref() else {
            return Err(CursorPlanError::InvalidContinuationCursorPayload {
                reason: InternalError::executor_invariant_message(
                    "cursor pagination requires explicit ordering",
                ),
            });
        };
        if order.fields.is_empty() {
            return Err(CursorPlanError::InvalidContinuationCursorPayload {
                reason: InternalError::executor_invariant_message(
                    "cursor pagination requires non-empty ordering",
                ),
            });
        }

        Ok(order)
    }

    // Resolve cursor ordering for executor-provided cursor-state revalidation.
    // Missing or empty ordering at this boundary is an execution invariant violation.
    fn validated_cursor_order_internal(&self) -> Result<&OrderSpec, InternalError> {
        let Some(order) = self.plan.order.as_ref() else {
            return Err(InternalError::query_executor_invariant(
                "cursor pagination requires explicit ordering",
            ));
        };
        if order.fields.is_empty() {
            return Err(InternalError::query_executor_invariant(
                "cursor pagination requires non-empty ordering",
            ));
        }

        Ok(order)
    }

    // ------------------------------------------------------------------
    // Semantic-to-raw spec lowering
    // ------------------------------------------------------------------

    // Lower semantic index-range access into raw-key bounds once at plan materialization.
    fn build_index_prefix_specs(
        plan: &LogicalPlan<E::Key>,
    ) -> Result<Vec<IndexPrefixSpec>, InternalError> {
        let mut specs = Vec::new();
        Self::collect_index_prefix_specs(&plan.access, &mut specs)?;

        Ok(specs)
    }

    // Collect index-prefix specs in deterministic depth-first traversal order.
    fn collect_index_prefix_specs(
        access: &AccessPlan<E::Key>,
        specs: &mut Vec<IndexPrefixSpec>,
    ) -> Result<(), InternalError> {
        match access {
            AccessPlan::Path(path) => {
                if let AccessPath::IndexPrefix { index, values } = path.as_ref() {
                    let encoded_prefix = EncodedValue::try_encode_all(values).map_err(|_| {
                        InternalError::query_executor_invariant(
                            INDEX_PREFIX_SPEC_VALUE_NOT_INDEXABLE,
                        )
                    })?;
                    let (lower, upper) =
                        raw_keys_for_encoded_prefix::<E>(index, encoded_prefix.as_slice());
                    specs.push(IndexPrefixSpec::new(
                        *index,
                        Bound::Included(lower),
                        Bound::Included(upper),
                    ));
                }

                Ok(())
            }
            AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
                for child in children {
                    Self::collect_index_prefix_specs(child, specs)?;
                }

                Ok(())
            }
        }
    }

    // Lower semantic index-range access into raw-key bounds once at plan materialization.
    fn build_index_range_specs(
        plan: &LogicalPlan<E::Key>,
    ) -> Result<Vec<IndexRangeSpec>, InternalError> {
        let mut specs = Vec::new();
        Self::collect_index_range_specs(&plan.access, &mut specs)?;

        Ok(specs)
    }

    // Collect index-range specs in deterministic depth-first traversal order.
    fn collect_index_range_specs(
        access: &AccessPlan<E::Key>,
        specs: &mut Vec<IndexRangeSpec>,
    ) -> Result<(), InternalError> {
        match access {
            AccessPlan::Path(path) => {
                if let AccessPath::IndexRange {
                    index,
                    prefix,
                    lower,
                    upper,
                } = path.as_ref()
                {
                    let (lower, upper) = raw_bounds_for_semantic_index_component_range::<E>(
                        index, prefix, lower, upper,
                    )
                    .map_err(|err| {
                        InternalError::query_executor_invariant(
                            map_index_range_not_indexable_reason(
                                IndexRangeNotIndexableReasonScope::ValidatedSpec,
                                err,
                            ),
                        )
                    })?;
                    specs.push(IndexRangeSpec::new(*index, lower, upper));
                }

                Ok(())
            }
            AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
                for child in children {
                    Self::collect_index_range_specs(child, specs)?;
                }

                Ok(())
            }
        }
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
                Direction, IndexId, IndexKeyKind, RawIndexKey, continuation_advanced,
                encode_canonical_index_component,
            },
            query::plan::{
                AccessPath, ContinuationToken, CursorBoundary, CursorBoundarySlot, CursorPlanError,
                IndexRangeCursorAnchor, LogicalPlan, OrderDirection, OrderSpec, PlanError,
            },
        },
        model::{field::FieldKind, index::IndexModel},
        traits::Storable,
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

    crate::test_canister! {
        ident = ExecutableAnchorCanister,
    }

    crate::test_store! {
        ident = ExecutableAnchorStore,
        canister = ExecutableAnchorCanister,
    }

    crate::test_entity_schema! {
        ident = ExecutableAnchorEntity,
        id = Ulid,
        entity_name = "ExecutableAnchorEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("a", FieldKind::Uint),
            ("b", FieldKind::Uint),
            ("c", FieldKind::Uint),
        ],
        indexes = [&RANGE_INDEX_AB, &RANGE_INDEX_AC],
        store = ExecutableAnchorStore,
        canister = ExecutableAnchorCanister,
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

    fn encode_index_range_cursor(
        executable: &ExecutablePlan<ExecutableAnchorEntity>,
        boundary_pk: Ulid,
        anchor: IndexRangeCursorAnchor,
    ) -> Vec<u8> {
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Ulid(boundary_pk))],
        };
        ContinuationToken::new_index_range_with_direction(
            executable.continuation_signature(),
            boundary,
            anchor,
            Direction::Asc,
            0,
        )
        .encode()
        .expect("cursor token should encode")
    }

    #[test]
    fn index_range_anchor_validation_accepts_anchor_in_range() {
        let executable = build_index_range_cursor_executable();
        let expected_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AB);
        let boundary_pk = Ulid::from_u128(30_001);
        let anchor = anchor_for_value_with_pk(&expected_id, 15, boundary_pk);
        let token = encode_index_range_cursor(&executable, boundary_pk, anchor);

        executable
            .plan_cursor(Some(token.as_slice()))
            .expect("anchor inside index-range envelope should validate");
    }

    #[test]
    fn index_range_cursor_validation_layers_remain_intentionally_redundant() {
        let executable = build_index_range_cursor_executable();
        let expected_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AB);
        let boundary_pk = Ulid::from_u128(30_101);
        let anchor = anchor_for_value_with_pk(&expected_id, 15, boundary_pk);
        let token = encode_index_range_cursor(&executable, boundary_pk, anchor);

        // Layer 1 (planner): envelope + boundary/anchor compatibility.
        let planned = executable
            .plan_cursor(Some(token.as_slice()))
            .expect("planner layer should accept a compatible index-range cursor anchor");
        let anchor_raw = planned
            .index_range_anchor()
            .expect("planned cursor should carry an index-range anchor");

        // Layer 2 (store): strict advancement beyond anchor.
        assert!(
            !continuation_advanced(Direction::Asc, anchor_raw, anchor_raw),
            "store layer must still enforce strict advancement even when planner accepts the anchor"
        );
    }

    #[test]
    fn index_range_anchor_validation_rejects_mismatched_index_id() {
        let executable = build_index_range_cursor_executable();
        let other_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AC);
        let boundary_pk = Ulid::from_u128(30_002);
        let anchor = anchor_for_value_with_pk(&other_id, 15, boundary_pk);
        let token = encode_index_range_cursor(&executable, boundary_pk, anchor);

        let err = executable
            .plan_cursor(Some(token.as_slice()))
            .expect_err("anchor from a different index id must fail");
        match err {
            PlanError::Cursor(inner) => {
                let CursorPlanError::InvalidContinuationCursorPayload { reason } = inner.as_ref()
                else {
                    panic!("expected InvalidContinuationCursorPayload");
                };
                assert!(reason.contains("index id mismatch"));
            }
            _ => panic!("expected InvalidContinuationCursorPayload"),
        }
    }

    #[test]
    fn index_range_anchor_validation_rejects_out_of_envelope_anchor() {
        let executable = build_index_range_cursor_executable();
        let expected_id = IndexId::new::<ExecutableAnchorEntity>(&RANGE_INDEX_AB);
        let boundary_pk = Ulid::from_u128(30_003);
        let anchor = anchor_for_value_with_pk(&expected_id, 99, boundary_pk);
        let token = encode_index_range_cursor(&executable, boundary_pk, anchor);

        let err = executable
            .plan_cursor(Some(token.as_slice()))
            .expect_err("anchor outside index-range envelope must fail");
        match err {
            PlanError::Cursor(inner) => {
                let CursorPlanError::InvalidContinuationCursorPayload { reason } = inner.as_ref()
                else {
                    panic!("expected InvalidContinuationCursorPayload");
                };
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
        let token = encode_index_range_cursor(&executable, boundary_pk, anchor);

        let err = executable
            .plan_cursor(Some(token.as_slice()))
            .expect_err("boundary/anchor mismatch must fail");
        match err {
            PlanError::Cursor(inner) => {
                let CursorPlanError::InvalidContinuationCursorPayload { reason } = inner.as_ref()
                else {
                    panic!("expected InvalidContinuationCursorPayload");
                };
                assert!(reason.contains("boundary/anchor mismatch"));
            }
            _ => panic!("expected InvalidContinuationCursorPayload"),
        }
    }

    #[test]
    fn executable_direction_uses_desc_for_single_index_range_desc_order() {
        let mut plan: LogicalPlan<Ulid> =
            LogicalPlan::new(index_range_access(), ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });

        let executable = ExecutablePlan::<ExecutableAnchorEntity>::new(plan);

        assert_eq!(executable.direction(), Direction::Desc);
    }

    #[test]
    fn executable_direction_uses_desc_for_non_index_range_desc_order() {
        let mut plan: LogicalPlan<Ulid> =
            LogicalPlan::new(AccessPath::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });

        let executable = ExecutablePlan::<ExecutableAnchorEntity>::new(plan);

        assert_eq!(executable.direction(), Direction::Desc);
    }
}
