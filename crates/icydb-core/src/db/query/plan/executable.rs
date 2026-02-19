use crate::{
    db::{
        index::{Direction, RawIndexKey},
        query::{
            intent::QueryMode,
            plan::{
                AccessPlan, ContinuationSignature, CursorBoundary, CursorPlanError, ExplainPlan,
                LogicalPlan, OrderDirection, OrderSpec, PlanError, PlanFingerprint,
                validate_planned_cursor, validate_planned_cursor_state,
            },
        },
    },
    error::InternalError,
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
    pub(in crate::db) const fn new(
        boundary: CursorBoundary,
        index_range_anchor: Option<RawIndexKey>,
    ) -> Self {
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

    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.boundary.is_none() && self.index_range_anchor.is_none()
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
    pub(crate) fn new(plan: LogicalPlan<E::Key>) -> Self {
        let direction = Self::derive_direction(&plan);
        Self {
            plan,
            direction,
            _marker: PhantomData,
        }
    }

    fn derive_direction(plan: &LogicalPlan<E::Key>) -> Direction {
        let Some(order) = plan.order.as_ref() else {
            return Direction::Asc;
        };

        match order.fields.first().map(|(_, direction)| direction) {
            Some(OrderDirection::Desc) => Direction::Desc,
            _ => Direction::Asc,
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
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    pub(in crate::db) fn into_inner(self) -> LogicalPlan<E::Key> {
        self.plan
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
        )
        .map_err(InternalError::from_cursor_plan_error)
    }

    // Resolve cursor ordering for plan-surface cursor decoding.
    // Cursor readiness is owned by policy/intent validation.
    fn validated_cursor_order_plan(&self) -> Result<&OrderSpec, CursorPlanError> {
        let Some(order) = self.plan.order.as_ref() else {
            return Err(CursorPlanError::InvalidContinuationCursorPayload {
                reason: "executor invariant violated: cursor pagination requires explicit ordering"
                    .to_string(),
            });
        };
        if order.fields.is_empty() {
            return Err(CursorPlanError::InvalidContinuationCursorPayload {
                reason:
                    "executor invariant violated: cursor pagination requires non-empty ordering"
                        .to_string(),
            });
        }

        Ok(order)
    }

    // Resolve cursor ordering for executor-provided cursor-state revalidation.
    // Missing or empty ordering at this boundary is an execution invariant violation.
    fn validated_cursor_order_internal(&self) -> Result<&OrderSpec, InternalError> {
        let Some(order) = self.plan.order.as_ref() else {
            return Err(InternalError::query_invariant(
                "executor invariant violated: cursor pagination requires explicit ordering",
            ));
        };
        if order.fields.is_empty() {
            return Err(InternalError::query_invariant(
                "executor invariant violated: cursor pagination requires non-empty ordering",
            ));
        }

        Ok(order)
    }
}

impl InternalError {
    fn from_cursor_plan_error(err: PlanError) -> Self {
        let message = match &err {
            PlanError::Cursor(inner) => match inner.as_ref() {
                CursorPlanError::ContinuationCursorBoundaryArityMismatch { expected: 1, found } => {
                    format!(
                        "executor invariant violated: pk-ordered continuation boundary must contain exactly 1 slot, found {found}"
                    )
                }
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    value: None, ..
                } => "executor invariant violated: pk cursor slot must be present".to_string(),
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    value: Some(_),
                    ..
                } => "executor invariant violated: pk cursor slot type mismatch".to_string(),
                _ => err.to_string(),
            },
            _ => err.to_string(),
        };

        Self::query_invariant(message)
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
                Direction, IndexId, IndexKeyKind, RawIndexKey, encode_canonical_index_component,
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
