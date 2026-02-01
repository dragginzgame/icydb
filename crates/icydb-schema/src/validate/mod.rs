//! Schema validation orchestration and shared helpers.

pub mod naming;
pub mod relation;

use crate::{
    error::ErrorTree,
    node::{Schema, VisitableNode},
    visit::ValidateVisitor,
};

/// Run full schema validation in a staged, deterministic order.
pub(crate) fn validate_schema(schema: &Schema) -> Result<(), ErrorTree> {
    // Phase 1: validate each node (structural + local invariants).
    let mut errors = validate_nodes(schema);

    // Phase 2: enforce schema-wide invariants.
    validate_global(schema, &mut errors);

    errors.result()
}

// Validate all nodes via a visitor to retain route-aware error aggregation.
fn validate_nodes(schema: &Schema) -> ErrorTree {
    let mut visitor = ValidateVisitor::new();
    schema.accept(&mut visitor);

    visitor.errors
}

// Run global validation passes that require a full schema view.
fn validate_global(schema: &Schema, errors: &mut ErrorTree) {
    naming::validate_entity_naming(schema, errors);
    relation::validate_same_canister_relations(schema, errors);
}
