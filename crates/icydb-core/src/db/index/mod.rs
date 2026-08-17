//! Module: index
//! Responsibility: index key encoding, index entry modeling, and index-store access.
//! Does not own: query planning policy, commit orchestration, or relation semantics.
//! Boundary: executor/query/commit may depend on index; index depends on data primitives.

mod cardinality;
mod entry;
pub(in crate::db) mod envelope;
mod expression_contract;
mod key;
mod plan;
pub(in crate::db) mod predicate;
mod range;
mod readers;
mod scan;
mod store;

pub(in crate::db) use crate::db::key_taxonomy::IndexEntryValue;
pub(in crate::db) use cardinality::UserIndexPrefixCardinalityKey;
pub(in crate::db) use entry::IndexEntryExistenceWitness;
pub(in crate::db) use entry::IndexEntryRowWitness;
pub(in crate::db) use entry::IndexRowIdentity;
pub(in crate::db) use envelope::{envelope_is_empty, key_within_envelope};
pub(in crate::db) use envelope::{
    resume_bounds_for_continuation, validate_index_scan_continuation_advancement,
};
pub(in crate::db) use expression_contract::SemanticIndexExpression;
pub(in crate::db) use expression_contract::index_expression_supports_text_casefold_lookup;
pub(in crate::db) use key::{
    EncodedValue, IndexExpressionSourceClass, encode_accepted_index_literal_component,
};
pub(in crate::db) use key::{
    IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey, derive_index_expression_value,
};
pub(in crate::db) use plan::{
    AcceptedIndexInspectionDomain, AcceptedIndexInspectionPlan, IndexDelta, IndexDeltaGroup,
    IndexMembershipDelta, IndexMutationPlan, IndexPlanReadView,
    plan_index_mutation_for_slot_reader_structural,
};
pub(in crate::db) use predicate::{
    IndexCompareOp, IndexCompilePolicy, IndexLiteral, IndexPredicateProgram, compile_index_program,
    compile_index_program_for_targets,
};
pub(in crate::db) use range::raw_keys_for_component_prefix_with_kind;
pub(in crate::db) use range::{
    IndexBoundsSpec, IndexRangeBoundEncodeError, TextPrefixBoundMode,
    build_index_bounds_lowering_for_arity, build_index_component_range_with_encoded_prefix,
    build_index_prefix_bounds_for_encoded_components, starts_with_component_bounds,
};
pub(in crate::db) use readers::{
    IndexReadContract, StructuralIndexEntryReader, StructuralPrimaryRowReader,
};
pub use store::{IndexState, IndexStore};
pub(in crate::db) use store::{
    IndexStoreVisit, PreparedIndexPositionPublication, PreparedIndexPositionRetirement,
};
