//! Module: testing::entity_tags
//! Responsibility: stable, append-only `EntityTag` assignments for crate-local tests.
//! Does not own: production entity identity assignment.
//! Boundary: keeps persisted test keyspaces deterministic across fixture edits.

use crate::types::EntityTag;

// Stable test-only entity identity registry. These values are explicit and
// append-only so fixture edits never rewrite persisted test keyspaces.
pub(crate) const DIAGNOSTICS_SINGLE_ENTITY_TAG: EntityTag = EntityTag::new(0x1000);
pub(crate) const DIAGNOSTICS_FIRST_ENTITY_TAG: EntityTag = EntityTag::new(0x1001);
pub(crate) const DIAGNOSTICS_SECOND_ENTITY_TAG: EntityTag = EntityTag::new(0x1002);
pub(crate) const DIAGNOSTICS_MINMAX_ENTITY_TAG: EntityTag = EntityTag::new(0x1003);
pub(crate) const DIAGNOSTICS_VALID_ENTITY_TAG: EntityTag = EntityTag::new(0x1004);
pub(crate) const INTEGRITY_INDEXED_ENTITY_TAG: EntityTag = EntityTag::new(0x1005);
pub(crate) const AGGREGATE_FIELD_ENTITY_TAG: EntityTag = EntityTag::new(0x1006);
pub(crate) const GROUPED_STATE_TEST_ENTITY_TAG: EntityTag = EntityTag::new(0x1007);
pub(crate) const PROJECTION_EVAL_ENTITY_TAG: EntityTag = EntityTag::new(0x1008);
pub(crate) const ROUTE_MATRIX_ENTITY_TAG: EntityTag = EntityTag::new(0x1009);
pub(crate) const FAST_STREAM_INVARIANT_ENTITY_TAG: EntityTag = EntityTag::new(0x100A);
pub(crate) const CONTEXT_INVARIANT_ENTITY_TAG: EntityTag = EntityTag::new(0x100B);
pub(crate) const PROBE_ENTITY_TAG: EntityTag = EntityTag::new(0x100C);
pub(crate) const RECOVERY_TEST_ENTITY_TAG: EntityTag = EntityTag::new(0x100D);
pub(crate) const RECOVERY_INDEXED_ENTITY_TAG: EntityTag = EntityTag::new(0x100E);
pub(crate) const RECOVERY_UNIQUE_ENTITY_TAG: EntityTag = EntityTag::new(0x100F);
pub(crate) const RECOVERY_UNIQUE_CASEFOLD_ENTITY_TAG: EntityTag = EntityTag::new(0x1010);
pub(crate) const RECOVERY_UPPER_EXPRESSION_ENTITY_TAG: EntityTag = EntityTag::new(0x1011);
pub(crate) const RECOVERY_CONDITIONAL_ENTITY_TAG: EntityTag = EntityTag::new(0x1012);
pub(crate) const RECOVERY_CONDITIONAL_UNIQUE_ENTITY_TAG: EntityTag = EntityTag::new(0x1013);
pub(crate) const RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_ENTITY_TAG: EntityTag =
    EntityTag::new(0x1014);
pub(crate) const RECOVERY_CONDITIONAL_UNIQUE_ENUM_ENTITY_TAG: EntityTag = EntityTag::new(0x1015);
pub(crate) const MIGRATION_ENTITY_TAG: EntityTag = EntityTag::new(0x1016);
pub(crate) const PLAN_ENTITY_TAG: EntityTag = EntityTag::new(0x1017);
pub(crate) const PLAN_SINGLETON_TAG: EntityTag = EntityTag::new(0x1018);
pub(crate) const PLAN_NUMERIC_ENTITY_TAG: EntityTag = EntityTag::new(0x1019);
pub(crate) const SESSION_SQL_ENTITY_TAG: EntityTag = EntityTag::new(0x101A);
pub(crate) const SIMPLE_ENTITY_TAG: EntityTag = EntityTag::new(0x101B);
pub(crate) const INDEXED_METRICS_ENTITY_TAG: EntityTag = EntityTag::new(0x101C);
pub(crate) const UNIQUE_INDEX_RANGE_ENTITY_TAG: EntityTag = EntityTag::new(0x101D);
pub(crate) const PHASE_ENTITY_TAG: EntityTag = EntityTag::new(0x101E);
pub(crate) const RELATION_TARGET_ENTITY_TAG: EntityTag = EntityTag::new(0x101F);
pub(crate) const RELATION_SOURCE_ENTITY_TAG: EntityTag = EntityTag::new(0x1020);
pub(crate) const WEAK_SINGLE_RELATION_SOURCE_ENTITY_TAG: EntityTag = EntityTag::new(0x1021);
pub(crate) const WEAK_OPTIONAL_RELATION_SOURCE_ENTITY_TAG: EntityTag = EntityTag::new(0x1022);
pub(crate) const WEAK_LIST_RELATION_SOURCE_ENTITY_TAG: EntityTag = EntityTag::new(0x1023);
pub(crate) const TEMPORAL_BOUNDARY_ENTITY_TAG: EntityTag = EntityTag::new(0x1024);
pub(crate) const PUSHDOWN_PARITY_ENTITY_TAG: EntityTag = EntityTag::new(0x1025);
pub(crate) const TEXT_PREFIX_PARITY_ENTITY_TAG: EntityTag = EntityTag::new(0x1026);
pub(crate) const EXPRESSION_CASEFOLD_PARITY_ENTITY_TAG: EntityTag = EntityTag::new(0x1027);
pub(crate) const EXPRESSION_UPPER_PARITY_ENTITY_TAG: EntityTag = EntityTag::new(0x1028);
pub(crate) const SINGLETON_UNIT_ENTITY_TAG: EntityTag = EntityTag::new(0x1029);
pub(crate) const SOURCE_ENTITY_TAG: EntityTag = EntityTag::new(0x102A);
pub(crate) const TARGET_ENTITY_TAG: EntityTag = EntityTag::new(0x102B);
pub(crate) const SOURCE_SET_ENTITY_TAG: EntityTag = EntityTag::new(0x102C);
pub(crate) const UNIQUE_EMAIL_ENTITY_TAG: EntityTag = EntityTag::new(0x102D);
pub(crate) const MISMATCHED_PK_ENTITY_TAG: EntityTag = EntityTag::new(0x102E);
pub(crate) const INVALID_RELATION_METADATA_ENTITY_TAG: EntityTag = EntityTag::new(0x102F);
pub(crate) const DECIMAL_SCALE_ENTITY_TAG: EntityTag = EntityTag::new(0x1030);
pub(crate) const SQL_LOWER_ENTITY_TAG: EntityTag = EntityTag::new(0x1031);
pub(crate) const RECOVERY_PAYLOAD_ENTITY_TAG: EntityTag = EntityTag::new(0x1032);
pub(crate) const NULLABLE_ACCOUNT_EVENT_ENTITY_TAG: EntityTag = EntityTag::new(0x1033);
pub(crate) const WEAK_SET_RELATION_SOURCE_ENTITY_TAG: EntityTag = EntityTag::new(0x1051);
pub(crate) const WRONG_TAG_RELATION_METADATA_ENTITY_TAG: EntityTag = EntityTag::new(0x1052);
pub(crate) const WRONG_STORE_RELATION_METADATA_ENTITY_TAG: EntityTag = EntityTag::new(0x1053);
pub(crate) const DIAGNOSTICS_UNKNOWN_ENTITY_TAG: EntityTag = EntityTag::new(0x1FFF);

const ALL_TEST_ENTITY_TAGS: &[EntityTag] = &[
    DIAGNOSTICS_SINGLE_ENTITY_TAG,
    DIAGNOSTICS_FIRST_ENTITY_TAG,
    DIAGNOSTICS_SECOND_ENTITY_TAG,
    DIAGNOSTICS_MINMAX_ENTITY_TAG,
    DIAGNOSTICS_VALID_ENTITY_TAG,
    INTEGRITY_INDEXED_ENTITY_TAG,
    AGGREGATE_FIELD_ENTITY_TAG,
    GROUPED_STATE_TEST_ENTITY_TAG,
    PROJECTION_EVAL_ENTITY_TAG,
    ROUTE_MATRIX_ENTITY_TAG,
    FAST_STREAM_INVARIANT_ENTITY_TAG,
    CONTEXT_INVARIANT_ENTITY_TAG,
    PROBE_ENTITY_TAG,
    RECOVERY_TEST_ENTITY_TAG,
    RECOVERY_INDEXED_ENTITY_TAG,
    RECOVERY_UNIQUE_ENTITY_TAG,
    RECOVERY_UNIQUE_CASEFOLD_ENTITY_TAG,
    RECOVERY_UPPER_EXPRESSION_ENTITY_TAG,
    RECOVERY_CONDITIONAL_ENTITY_TAG,
    RECOVERY_CONDITIONAL_UNIQUE_ENTITY_TAG,
    RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_ENTITY_TAG,
    RECOVERY_CONDITIONAL_UNIQUE_ENUM_ENTITY_TAG,
    MIGRATION_ENTITY_TAG,
    PLAN_ENTITY_TAG,
    PLAN_SINGLETON_TAG,
    PLAN_NUMERIC_ENTITY_TAG,
    SESSION_SQL_ENTITY_TAG,
    SIMPLE_ENTITY_TAG,
    INDEXED_METRICS_ENTITY_TAG,
    UNIQUE_INDEX_RANGE_ENTITY_TAG,
    PHASE_ENTITY_TAG,
    RELATION_TARGET_ENTITY_TAG,
    RELATION_SOURCE_ENTITY_TAG,
    WEAK_SINGLE_RELATION_SOURCE_ENTITY_TAG,
    WEAK_OPTIONAL_RELATION_SOURCE_ENTITY_TAG,
    WEAK_LIST_RELATION_SOURCE_ENTITY_TAG,
    TEMPORAL_BOUNDARY_ENTITY_TAG,
    PUSHDOWN_PARITY_ENTITY_TAG,
    TEXT_PREFIX_PARITY_ENTITY_TAG,
    EXPRESSION_CASEFOLD_PARITY_ENTITY_TAG,
    EXPRESSION_UPPER_PARITY_ENTITY_TAG,
    SINGLETON_UNIT_ENTITY_TAG,
    SOURCE_ENTITY_TAG,
    TARGET_ENTITY_TAG,
    SOURCE_SET_ENTITY_TAG,
    UNIQUE_EMAIL_ENTITY_TAG,
    MISMATCHED_PK_ENTITY_TAG,
    INVALID_RELATION_METADATA_ENTITY_TAG,
    DECIMAL_SCALE_ENTITY_TAG,
    SQL_LOWER_ENTITY_TAG,
    RECOVERY_PAYLOAD_ENTITY_TAG,
    NULLABLE_ACCOUNT_EVENT_ENTITY_TAG,
    WEAK_SET_RELATION_SOURCE_ENTITY_TAG,
    WRONG_TAG_RELATION_METADATA_ENTITY_TAG,
    WRONG_STORE_RELATION_METADATA_ENTITY_TAG,
    DIAGNOSTICS_UNKNOWN_ENTITY_TAG,
];

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::testing::entity_tags::ALL_TEST_ENTITY_TAGS;
    use std::collections::BTreeSet;

    #[test]
    fn test_entity_tags_are_unique() {
        let unique = ALL_TEST_ENTITY_TAGS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();

        assert_eq!(unique.len(), ALL_TEST_ENTITY_TAGS.len());
    }
}
