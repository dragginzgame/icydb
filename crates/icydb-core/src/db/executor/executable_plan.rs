use crate::{
    db::{
        executor::index_specs::{
            INDEX_PREFIX_SPEC_INVALID, INDEX_RANGE_SPEC_INVALID, IndexPrefixSpec, IndexRangeSpec,
            build_index_prefix_specs, build_index_range_specs,
        },
        executor::{
            PlannedCursor, plan_cursor as validate_cursor_plan,
            revalidate_planned_cursor as revalidate_cursor_plan,
        },
        index::Direction,
        query::{
            contracts::cursor::ContinuationSignature,
            explain::ExplainPlan,
            fingerprint::PlanFingerprint,
            intent::QueryMode,
            plan::{AccessPlan, AccessPlannedQuery, LogicalPlan, OrderDirection, PlanError},
        },
    },
    error::InternalError,
    traits::{EntityKind, FieldValue},
};
use std::marker::PhantomData;

fn derive_direction(plan: &LogicalPlan) -> Direction {
    let Some((_, direction)) = plan.order.as_ref().and_then(|order| order.fields.first()) else {
        return Direction::Asc;
    };

    if *direction == OrderDirection::Desc {
        Direction::Desc
    } else {
        Direction::Asc
    }
}

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///
#[derive(Debug)]
pub struct ExecutablePlan<E: EntityKind> {
    plan: AccessPlannedQuery<E::Key>,
    index_prefix_specs: Vec<IndexPrefixSpec>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Vec<IndexRangeSpec>,
    index_range_spec_invalid: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> ExecutablePlan<E> {
    #[cfg(test)]
    pub(crate) fn new(plan: AccessPlannedQuery<E::Key>) -> Self {
        Self::build(plan)
    }

    #[cfg(not(test))]
    pub(in crate::db) fn new(plan: AccessPlannedQuery<E::Key>) -> Self {
        Self::build(plan)
    }

    fn build(plan: AccessPlannedQuery<E::Key>) -> Self {
        let (index_prefix_specs, index_prefix_spec_invalid) =
            match build_index_prefix_specs::<E>(&plan) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };
        let (index_range_specs, index_range_spec_invalid) =
            match build_index_range_specs::<E>(&plan) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };

        Self {
            plan,
            index_prefix_specs,
            index_prefix_spec_invalid,
            index_range_specs,
            index_range_spec_invalid,
            _marker: PhantomData,
        }
    }

    // Initial page offset used for continuation compatibility on first-page shape.
    const fn initial_page_offset(plan: &LogicalPlan) -> u32 {
        match plan.page {
            Some(ref page) => page.offset,
            None => 0,
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
        let direction = derive_direction(&self.plan.logical);
        validate_cursor_plan::<E>(
            &self.plan,
            direction,
            self.continuation_signature(),
            Self::initial_page_offset(&self.plan),
            cursor,
        )
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> QueryMode {
        self.plan.logical.mode
    }

    pub(in crate::db) const fn access(&self) -> &AccessPlan<E::Key> {
        &self.plan.access
    }

    #[must_use]
    pub(in crate::db) const fn as_inner(&self) -> &AccessPlannedQuery<E::Key> {
        &self.plan
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
    pub(in crate::db) fn into_inner(self) -> AccessPlannedQuery<E::Key> {
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
        let direction = derive_direction(&self.plan.logical);
        revalidate_cursor_plan::<E>(
            &self.plan,
            direction,
            Self::initial_page_offset(&self.plan),
            cursor,
        )
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
            query::{
                contracts::cursor::{
                    ContinuationToken, CursorBoundary, CursorBoundarySlot, IndexRangeCursorAnchor,
                },
                plan::{
                    AccessPath, AccessPlannedQuery, CursorPlanError, OrderDirection, OrderSpec,
                    PlanError,
                },
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
        let mut plan: AccessPlannedQuery<Ulid> =
            AccessPlannedQuery::new(index_range_access(), ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        ExecutablePlan::new(plan)
    }

    fn index_range_access() -> AccessPath<Ulid> {
        AccessPath::index_range(
            RANGE_INDEX_AB,
            vec![Value::Uint(42)],
            Bound::Included(Value::Uint(10)),
            Bound::Included(Value::Uint(20)),
        )
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
            .expect("planned cursor should carry an index-range anchor")
            .last_raw_key();

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
        let mut plan: AccessPlannedQuery<Ulid> =
            AccessPlannedQuery::new(index_range_access(), ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });

        let executable = ExecutablePlan::<ExecutableAnchorEntity>::new(plan);

        assert_eq!(
            super::derive_direction(&executable.as_inner().logical),
            Direction::Desc
        );
    }

    #[test]
    fn executable_direction_uses_desc_for_non_index_range_desc_order() {
        let mut plan: AccessPlannedQuery<Ulid> =
            AccessPlannedQuery::new(AccessPath::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });

        let executable = ExecutablePlan::<ExecutableAnchorEntity>::new(plan);

        assert_eq!(
            super::derive_direction(&executable.as_inner().logical),
            Direction::Desc
        );
    }
}
