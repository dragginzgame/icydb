//! Module: index::plan::metadata
//! Responsibility: prepare immutable accepted index inputs for a commit operation.
//! Does not own: row admission, membership results, uniqueness or mutation deltas.
//! Boundary: one accepted schema/row contract -> reusable per-index programs and slots.

use crate::{
    db::{
        data::StructuralRowContract,
        predicate::PredicateProgram,
        schema::{SchemaExpressionIndexKeyItemInfo, SchemaIndexFieldPathInfo, SchemaInfo},
    },
    error::InternalError,
};

/// Fixed accepted index setup bound to the commit context's row contract.
/// Keep this inside the existing synchronous context lifetime; membership
/// evaluation and after-image key admission still run for every row.
/// Private tables retain schema order and are never mutated after preparation.
pub(in crate::db) struct PreparedIndexMutationMetadata {
    pub(super) schema_info: SchemaInfo,
    pub(super) row_contract: StructuralRowContract,
    pub(super) field_path_inputs: Vec<PreparedIndexInputs>,
    pub(super) expression_inputs: Vec<PreparedIndexInputs>,
}

impl PreparedIndexMutationMetadata {
    /// Compile accepted predicates and dependency slots once for this context.
    pub(in crate::db) fn new(
        schema_info: SchemaInfo,
        row_contract: StructuralRowContract,
    ) -> Result<Self, InternalError> {
        let field_path_inputs = schema_info
            .field_path_indexes()
            .iter()
            .map(|index| {
                let program = index.predicate()?.map(|predicate| {
                    PredicateProgram::compile_with_row_contract(&row_contract, &predicate)
                });
                Ok(PreparedIndexInputs::new(
                    &row_contract,
                    program,
                    index.fields().iter().map(SchemaIndexFieldPathInfo::slot),
                ))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;
        let expression_inputs = schema_info
            .expression_indexes()
            .iter()
            .map(|index| {
                let program = index.predicate()?.map(|predicate| {
                    PredicateProgram::compile_with_row_contract(&row_contract, &predicate)
                });
                Ok(PreparedIndexInputs::new(
                    &row_contract,
                    program,
                    index.key_items().iter().map(|item| match item {
                        SchemaExpressionIndexKeyItemInfo::FieldPath(field) => field.slot(),
                        SchemaExpressionIndexKeyItemInfo::Expression(expression) => {
                            expression.source().slot()
                        }
                    }),
                ))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;
        Ok(Self {
            schema_info,
            row_contract,
            field_path_inputs,
            expression_inputs,
        })
    }
}

/// One membership program and the union of key and predicate dependencies.
pub(super) struct PreparedIndexInputs {
    pub(super) predicate_program: Option<PredicateProgram>,
    pub(super) referenced_slots: Box<[bool]>,
}

impl PreparedIndexInputs {
    fn new(
        row_contract: &StructuralRowContract,
        predicate_program: Option<PredicateProgram>,
        key_slots: impl Iterator<Item = usize>,
    ) -> Self {
        let mut referenced_slots = vec![false; row_contract.field_count()];
        for slot in key_slots {
            if let Some(referenced) = referenced_slots.get_mut(slot) {
                *referenced = true;
            }
        }
        if let Some(program) = predicate_program.as_ref() {
            program.mark_referenced_slots(&mut referenced_slots);
        }
        Self {
            predicate_program,
            referenced_slots: referenced_slots.into_boxed_slice(),
        }
    }
}
