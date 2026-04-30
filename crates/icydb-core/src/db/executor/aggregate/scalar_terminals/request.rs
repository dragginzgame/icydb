//! Module: executor::aggregate::scalar_terminals::request
//! Responsibility: structural aggregate request and output projection compile.
//! Boundary: converts structural terminal requests into grouped-row projection programs.

use crate::{
    db::{
        executor::{
            aggregate::scalar_terminals::terminal::{
                StructuralAggregateTerminal, resolve_structural_aggregate_terminal,
            },
            projection::compile_grouped_projection_expr,
        },
        query::plan::{
            GroupedAggregateExecutionSpec,
            expr::{CompiledExpr, Expr, ProjectionField, ProjectionSpec},
        },
    },
    error::InternalError,
    value::Value,
};

///
/// StructuralAggregateResult
///
/// StructuralAggregateResult is the executor-owned transport wrapper for a
/// fully reduced aggregate result. It intentionally exposes only a consumptive
/// row handoff so adapter layers shape DTOs without owning aggregate execution.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db) struct StructuralAggregateResult(Vec<Vec<Value>>);

impl StructuralAggregateResult {
    /// Construct one structural aggregate result from executor-owned rows.
    #[must_use]
    pub(super) const fn new(rows: Vec<Vec<Value>>) -> Self {
        Self(rows)
    }

    /// Consume this structural wrapper into value rows for adapter shaping.
    #[must_use]
    pub(in crate::db) fn into_value_rows(self) -> Vec<Vec<Value>> {
        self.0
    }
}

///
/// StructuralAggregateRequest
///
/// StructuralAggregateRequest carries the canonical aggregate execution intent
/// needed after adapter or fluent lowering has finished. The executor compiles
/// and executes these semantic expressions against a prepared scalar plan.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StructuralAggregateRequest {
    terminals: Vec<StructuralAggregateTerminal>,
    projection: ProjectionSpec,
    having: Option<Expr>,
}

impl StructuralAggregateRequest {
    /// Build one structural aggregate request from lowered semantic parts.
    #[must_use]
    pub(in crate::db) const fn new(
        terminals: Vec<StructuralAggregateTerminal>,
        projection: ProjectionSpec,
        having: Option<Expr>,
    ) -> Self {
        Self {
            terminals,
            projection,
            having,
        }
    }

    pub(super) const fn terminals(&self) -> &[StructuralAggregateTerminal] {
        self.terminals.as_slice()
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
    aggregate_execution_specs: Vec<GroupedAggregateExecutionSpec>,
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

        let mut projection = Vec::with_capacity(request.projection.len());
        for field in request.projection.fields() {
            let ProjectionField::Scalar { expr, .. } = field;
            projection.push(
                compile_grouped_projection_expr(expr, &[], aggregate_execution_specs.as_slice())
                    .map_err(|err| {
                        InternalError::query_executor_invariant(format!(
                            "structural aggregate output projection must compile against aggregate row: {err}",
                        ))
                    })?,
            );
        }

        let having = request
            .having
            .as_ref()
            .map(|expr| {
                compile_grouped_projection_expr(expr, &[], aggregate_execution_specs.as_slice())
                    .map_err(|err| {
                        InternalError::query_executor_invariant(format!(
                            "structural aggregate HAVING must compile against aggregate row: {err}",
                        ))
                    })
            })
            .transpose()?;

        Ok(Self {
            aggregate_execution_specs,
            projection,
            having,
        })
    }

    pub(super) const fn aggregate_execution_specs(&self) -> &[GroupedAggregateExecutionSpec] {
        self.aggregate_execution_specs.as_slice()
    }

    pub(super) const fn projection(&self) -> &[CompiledExpr] {
        self.projection.as_slice()
    }

    pub(super) const fn having(&self) -> Option<&CompiledExpr> {
        self.having.as_ref()
    }
}
