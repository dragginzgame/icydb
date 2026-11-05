//! Module: executor::aggregate::scalar_terminals::request
//! Responsibility: structural aggregate request and output projection compile.
//! Boundary: converts structural terminal requests into grouped-row projection programs.

use crate::{
    db::{
        executor::{
            aggregate::{
                CompiledExpr, Expr, ProjectionSpec,
                scalar_terminals::terminal::{
                    StructuralAggregateTerminal, resolve_structural_aggregate_terminal,
                },
            },
            projection::{compile_grouped_projection_expr, compile_grouped_projection_plan},
        },
        query::plan::expr::GroupedCompilationError,
        resource::budget::ExecutionConstructionBudget,
        schema::SchemaInfo,
    },
    error::InternalError,
};

///
/// StructuralAggregateRequest
///
/// StructuralAggregateRequest carries the canonical aggregate execution intent
/// needed after adapter or fluent lowering has finished. The executor compiles
/// and executes these semantic expressions against a prepared scalar plan.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralAggregateRequest {
    terminals: Vec<StructuralAggregateTerminal>,
    projection: ProjectionSpec,
    having: Option<Expr>,
    schema_info: SchemaInfo,
}

impl StructuralAggregateRequest {
    /// Build one structural aggregate request from lowered aggregate inputs.
    #[must_use]
    pub(in crate::db) const fn new(
        terminals: Vec<StructuralAggregateTerminal>,
        projection: ProjectionSpec,
        having: Option<Expr>,
        schema_info: SchemaInfo,
    ) -> Self {
        Self {
            terminals,
            projection,
            having,
            schema_info,
        }
    }

    pub(super) const fn terminals(&self) -> &[StructuralAggregateTerminal] {
        self.terminals.as_slice()
    }

    pub(super) const fn schema_info(&self) -> &SchemaInfo {
        &self.schema_info
    }
}

///
/// CompiledStructuralAggregateRequest
///
/// CompiledStructuralAggregateRequest keeps post-reduction projection and
/// HAVING programs beside the aggregate identity specs needed to evaluate them
/// against the implicit single-row aggregate output.
///

pub(super) struct CompiledStructuralAggregateRequest {
    projection: Vec<CompiledExpr>,
    having: Option<CompiledExpr>,
}

impl CompiledStructuralAggregateRequest {
    pub(super) fn compile(request: &StructuralAggregateRequest) -> Result<Self, InternalError> {
        let aggregate_execution_specs = request
            .terminals
            .iter()
            .map(|terminal| resolve_structural_aggregate_terminal(terminal).into_grouped_spec())
            .collect::<Vec<_>>();

        let projection = compile_grouped_projection_plan(
            &request.projection,
            &crate::db::query::plan::GroupFieldSet::empty(),
            aggregate_execution_specs.as_slice(),
            &ExecutionConstructionBudget,
        )
        .map_err(structural_compilation_error)?;

        let having = request
            .having
            .as_ref()
            .map(|expr| {
                compile_grouped_projection_expr(
                    expr,
                    &crate::db::query::plan::GroupFieldSet::empty(),
                    aggregate_execution_specs.as_slice(),
                    &ExecutionConstructionBudget,
                )
                .map_err(structural_compilation_error)
            })
            .transpose()?;

        Ok(Self { projection, having })
    }

    pub(super) const fn projection(&self) -> &[CompiledExpr] {
        self.projection.as_slice()
    }

    pub(super) const fn having(&self) -> Option<&CompiledExpr> {
        self.having.as_ref()
    }
}

// Structural shapes are already validated; resource failures are not invariants.
fn structural_compilation_error(error: GroupedCompilationError) -> InternalError {
    match error {
        GroupedCompilationError::Budget(error) => error,
        GroupedCompilationError::Projection(_) => InternalError::query_executor_invariant(),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
