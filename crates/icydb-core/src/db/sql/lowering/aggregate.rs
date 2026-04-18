use crate::db::sql::lowering::{
    LoweredBaseQueryShape, LoweredSqlCommand, LoweredSqlCommandInner, PreparedSqlStatement,
    SqlLoweringError,
    predicate::{lower_sql_where_bool_expr, lower_sql_where_expr},
};
#[cfg(test)]
use crate::{db::query::intent::Query, traits::EntityKind};
use crate::{
    db::{
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        predicate::MissingRowPolicy,
        query::{
            builder::{
                AggregateExpr,
                aggregate::{
                    avg, canonicalize_aggregate_input_expr, count, count_by, max_by, min_by, sum,
                },
                scalar_projection::render_scalar_projection_expr_sql_label,
            },
            intent::StructuralQuery,
            plan::{
                AggregateKind, FieldSlot,
                expr::{
                    Alias, BinaryOp, Expr, Function, ProjectionField, ProjectionSpec,
                    compile_scalar_projection_expr, expr_references_only_fields,
                },
                lower_global_aggregate_projection, resolve_aggregate_target_field_slot,
            },
        },
        sql::{
            lowering::expr::{SqlExprPhase, lower_sql_expr},
            lowering::select::{
                expr_contains_aggregate, lower_global_aggregate_having_expr,
                lower_select_item_expr, select_item_contains_aggregate,
            },
            parser::{
                SqlAggregateCall, SqlAggregateInputExpr, SqlAggregateKind, SqlExplainMode, SqlExpr,
                SqlProjection, SqlProjectionOperand, SqlRoundProjectionInput, SqlSelectItem,
                SqlSelectStatement, SqlStatement,
            },
        },
    },
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};

///
/// SqlGlobalAggregateTerminal
///
/// Global SQL aggregate terminals currently executable through dedicated
/// aggregate SQL entrypoints.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlGlobalAggregateTerminal {
    CountRows {
        filter_expr: Option<Expr>,
    },
    CountField {
        field: String,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    CountExpr {
        input_expr: Expr,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    SumField {
        field: String,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    SumExpr {
        input_expr: Expr,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    AvgField {
        field: String,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    AvgExpr {
        input_expr: Expr,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    MinField {
        field: String,
        filter_expr: Option<Expr>,
    },
    MinExpr {
        input_expr: Expr,
        filter_expr: Option<Expr>,
    },
    MaxField {
        field: String,
        filter_expr: Option<Expr>,
    },
    MaxExpr {
        input_expr: Expr,
        filter_expr: Option<Expr>,
    },
}

/// PreparedSqlScalarAggregateDomain
///
/// Prepared SQL scalar aggregate execution domain selected before session
/// runtime dispatch.
/// This keeps the aggregate lane explicit about which internal execution
/// family will consume the request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedSqlScalarAggregateDomain {
    ExistingRows,
    ProjectionField,
    NumericField,
    ScalarExtremaValue,
}

/// PreparedSqlScalarAggregateOrderingRequirement
///
/// Ordering sensitivity required by the selected typed SQL scalar aggregate
/// strategy. This keeps first-slice descriptor/explain consumers off local
/// kind checks when they need to know whether field order semantics matter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedSqlScalarAggregateOrderingRequirement {
    None,
    FieldOrder,
}

/// PreparedSqlScalarAggregateRowSource
///
/// Canonical row-source shape for one prepared typed SQL scalar aggregate
/// strategy. This describes what kind of row-derived data the execution family
/// ultimately consumes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedSqlScalarAggregateRowSource {
    ExistingRows,
    ProjectedField,
    NumericField,
    ExtremalWinnerField,
}

/// PreparedSqlScalarAggregateEmptySetBehavior
///
/// Canonical empty-window result behavior for one prepared typed SQL scalar
/// aggregate strategy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedSqlScalarAggregateEmptySetBehavior {
    Zero,
    Null,
}

/// PreparedSqlScalarAggregateDescriptorShape
///
/// Stable typed SQL scalar aggregate descriptor shape derived once at the SQL
/// aggregate preparation boundary and reused by runtime/EXPLAIN projections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedSqlScalarAggregateDescriptorShape {
    CountRows,
    CountField,
    SumField,
    AvgField,
    MinField,
    MaxField,
}

/// PreparedSqlScalarAggregateRuntimeDescriptor
///
/// Stable runtime-family projection for one prepared typed SQL scalar
/// aggregate strategy.
/// Session SQL aggregate execution consumes this descriptor instead of
/// rebuilding runtime boundary choice from raw SQL terminal variants or
/// parallel metadata tuple matches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedSqlScalarAggregateRuntimeDescriptor {
    CountRows,
    CountField,
    NumericField { kind: AggregateKind },
    ExtremalWinnerField { kind: AggregateKind },
}

///
/// PreparedSqlScalarAggregateDescriptorPolicy
///
/// Stable descriptor policy bundle derived from one prepared scalar aggregate
/// descriptor shape. SQL aggregate preparation uses this to keep domain,
/// ordering, row-source, and empty-set behavior on one owner-local seam.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedSqlScalarAggregateDescriptorPolicy {
    domain: PreparedSqlScalarAggregateDomain,
    ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement,
    row_source: PreparedSqlScalarAggregateRowSource,
    empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior,
}

///
/// PreparedSqlScalarAggregateStrategy
///
/// PreparedSqlScalarAggregateStrategy is the single typed SQL scalar aggregate
/// behavior source for the first `0.71` slice.
/// It resolves aggregate domain, descriptor shape, target-slot ownership, and
/// runtime behavior once so runtime and EXPLAIN do not re-derive that
/// behavior from raw SQL terminal variants.
/// Explain-visible aggregate expressions are projected on demand from this
/// prepared strategy instead of being carried as owned execution metadata.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedSqlScalarAggregateStrategy {
    target_slot: Option<FieldSlot>,
    input_expr: Option<Expr>,
    filter_expr: Option<Expr>,
    distinct_input: bool,
    domain: PreparedSqlScalarAggregateDomain,
    ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement,
    row_source: PreparedSqlScalarAggregateRowSource,
    empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior,
    descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
}

impl PreparedSqlScalarAggregateStrategy {
    // Resolve the stable descriptor-owned policy once so both typed and
    // structural aggregate preparation entrypoints stop rebuilding the same
    // domain/runtime behavior tuple by hand.
    const fn descriptor_policy(
        descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
    ) -> PreparedSqlScalarAggregateDescriptorPolicy {
        match descriptor_shape {
            PreparedSqlScalarAggregateDescriptorShape::CountRows => {
                PreparedSqlScalarAggregateDescriptorPolicy {
                    domain: PreparedSqlScalarAggregateDomain::ExistingRows,
                    ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::None,
                    row_source: PreparedSqlScalarAggregateRowSource::ExistingRows,
                    empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Zero,
                }
            }
            PreparedSqlScalarAggregateDescriptorShape::CountField => {
                PreparedSqlScalarAggregateDescriptorPolicy {
                    domain: PreparedSqlScalarAggregateDomain::ProjectionField,
                    ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::None,
                    row_source: PreparedSqlScalarAggregateRowSource::ProjectedField,
                    empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Zero,
                }
            }
            PreparedSqlScalarAggregateDescriptorShape::SumField
            | PreparedSqlScalarAggregateDescriptorShape::AvgField => {
                PreparedSqlScalarAggregateDescriptorPolicy {
                    domain: PreparedSqlScalarAggregateDomain::NumericField,
                    ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::None,
                    row_source: PreparedSqlScalarAggregateRowSource::NumericField,
                    empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Null,
                }
            }
            PreparedSqlScalarAggregateDescriptorShape::MinField
            | PreparedSqlScalarAggregateDescriptorShape::MaxField => {
                PreparedSqlScalarAggregateDescriptorPolicy {
                    domain: PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
                    ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::FieldOrder,
                    row_source: PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
                    empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Null,
                }
            }
        }
    }

    // Build one prepared aggregate strategy from the already-resolved target
    // slot and descriptor shape so higher entrypoints only own target
    // resolution, not the descriptor policy bundle.
    pub(in crate::db) const fn from_resolved_shape(
        target_slot: Option<FieldSlot>,
        input_expr: Option<Expr>,
        filter_expr: Option<Expr>,
        distinct_input: bool,
        descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
    ) -> Self {
        let policy = Self::descriptor_policy(descriptor_shape);

        Self {
            target_slot,
            input_expr,
            filter_expr,
            distinct_input,
            domain: policy.domain,
            ordering_requirement: policy.ordering_requirement,
            row_source: policy.row_source,
            empty_set_behavior: policy.empty_set_behavior,
            descriptor_shape,
        }
    }

    // Keep terminal preparation on one owner-local seam so field-target and
    // expression-input aggregate shapes cannot drift apart across parallel
    // helpers.
    #[expect(
        clippy::too_many_lines,
        reason = "aggregate terminal preparation keeps field and expression variants on one owner-local boundary"
    )]
    fn from_lowered_terminal(
        model: &'static EntityModel,
        terminal: &SqlGlobalAggregateTerminal,
    ) -> Result<Self, SqlLoweringError> {
        let resolve_target_slot = |field: &str| {
            resolve_aggregate_target_field_slot(model, field).map_err(SqlLoweringError::from)
        };
        let validate_input_expr = |input_expr: &Expr| {
            if let Some(field) = first_unknown_field_in_expr(input_expr, model) {
                return Err(SqlLoweringError::unknown_field(field));
            }
            if compile_scalar_projection_expr(model, input_expr).is_none() {
                return Err(SqlLoweringError::unsupported_aggregate_input_expressions());
            }

            Ok(())
        };

        match terminal {
            SqlGlobalAggregateTerminal::CountRows { filter_expr } => Ok(Self::from_resolved_shape(
                None,
                None,
                filter_expr.clone(),
                false,
                PreparedSqlScalarAggregateDescriptorShape::CountRows,
            )),
            SqlGlobalAggregateTerminal::CountField {
                field,
                filter_expr,
                distinct,
            } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    filter_expr.clone(),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::CountField,
                ))
            }
            SqlGlobalAggregateTerminal::CountExpr {
                input_expr,
                filter_expr,
                distinct,
            } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    filter_expr.clone(),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::CountField,
                ))
            }
            SqlGlobalAggregateTerminal::SumField {
                field,
                filter_expr,
                distinct,
            } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    filter_expr.clone(),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::SumField,
                ))
            }
            SqlGlobalAggregateTerminal::SumExpr {
                input_expr,
                filter_expr,
                distinct,
            } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    filter_expr.clone(),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::SumField,
                ))
            }
            SqlGlobalAggregateTerminal::AvgField {
                field,
                filter_expr,
                distinct,
            } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    filter_expr.clone(),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::AvgField,
                ))
            }
            SqlGlobalAggregateTerminal::AvgExpr {
                input_expr,
                filter_expr,
                distinct,
            } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    filter_expr.clone(),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::AvgField,
                ))
            }
            SqlGlobalAggregateTerminal::MinField { field, filter_expr } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    filter_expr.clone(),
                    false,
                    PreparedSqlScalarAggregateDescriptorShape::MinField,
                ))
            }
            SqlGlobalAggregateTerminal::MinExpr {
                input_expr,
                filter_expr,
            } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    filter_expr.clone(),
                    false,
                    PreparedSqlScalarAggregateDescriptorShape::MinField,
                ))
            }
            SqlGlobalAggregateTerminal::MaxField { field, filter_expr } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    filter_expr.clone(),
                    false,
                    PreparedSqlScalarAggregateDescriptorShape::MaxField,
                ))
            }
            SqlGlobalAggregateTerminal::MaxExpr {
                input_expr,
                filter_expr,
            } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    filter_expr.clone(),
                    false,
                    PreparedSqlScalarAggregateDescriptorShape::MaxField,
                ))
            }
        }
    }

    /// Borrow the resolved target slot when this prepared SQL scalar strategy is field-targeted.
    #[must_use]
    pub(crate) const fn target_slot(&self) -> Option<&FieldSlot> {
        self.target_slot.as_ref()
    }

    /// Borrow the aggregate input expression when this prepared SQL scalar strategy is expression-backed.
    #[must_use]
    pub(crate) const fn input_expr(&self) -> Option<&Expr> {
        self.input_expr.as_ref()
    }

    /// Borrow the aggregate filter expression when this prepared SQL scalar strategy is filtered.
    #[must_use]
    pub(crate) const fn filter_expr(&self) -> Option<&Expr> {
        self.filter_expr.as_ref()
    }

    /// Return whether this prepared SQL scalar aggregate deduplicates field inputs.
    #[must_use]
    pub(crate) const fn is_distinct(&self) -> bool {
        self.distinct_input
    }

    /// Return the canonical typed SQL scalar aggregate domain.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn domain(&self) -> PreparedSqlScalarAggregateDomain {
        self.domain
    }

    /// Return the stable descriptor/runtime shape label for this prepared strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn descriptor_shape(&self) -> PreparedSqlScalarAggregateDescriptorShape {
        self.descriptor_shape
    }

    /// Return the stable runtime-family projection for this prepared SQL
    /// scalar aggregate strategy.
    #[must_use]
    pub(crate) const fn runtime_descriptor(&self) -> PreparedSqlScalarAggregateRuntimeDescriptor {
        match self.descriptor_shape {
            PreparedSqlScalarAggregateDescriptorShape::CountRows => {
                PreparedSqlScalarAggregateRuntimeDescriptor::CountRows
            }
            PreparedSqlScalarAggregateDescriptorShape::CountField => {
                PreparedSqlScalarAggregateRuntimeDescriptor::CountField
            }
            PreparedSqlScalarAggregateDescriptorShape::SumField => {
                PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                    kind: AggregateKind::Sum,
                }
            }
            PreparedSqlScalarAggregateDescriptorShape::AvgField => {
                PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                    kind: AggregateKind::Avg,
                }
            }
            PreparedSqlScalarAggregateDescriptorShape::MinField => {
                PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                    kind: AggregateKind::Min,
                }
            }
            PreparedSqlScalarAggregateDescriptorShape::MaxField => {
                PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                    kind: AggregateKind::Max,
                }
            }
        }
    }

    /// Return the canonical aggregate kind for this prepared SQL scalar strategy.
    #[must_use]
    pub(crate) const fn aggregate_kind(&self) -> AggregateKind {
        match self.descriptor_shape {
            PreparedSqlScalarAggregateDescriptorShape::CountRows
            | PreparedSqlScalarAggregateDescriptorShape::CountField => AggregateKind::Count,
            PreparedSqlScalarAggregateDescriptorShape::SumField => AggregateKind::Sum,
            PreparedSqlScalarAggregateDescriptorShape::AvgField => AggregateKind::Avg,
            PreparedSqlScalarAggregateDescriptorShape::MinField => AggregateKind::Min,
            PreparedSqlScalarAggregateDescriptorShape::MaxField => AggregateKind::Max,
        }
    }

    /// Return the projected field label for descriptor/explain projection when
    /// this prepared strategy is field-targeted.
    #[must_use]
    pub(crate) fn projected_field(&self) -> Option<&str> {
        self.target_slot().map(FieldSlot::field)
    }

    /// Return field-order sensitivity for this prepared SQL scalar aggregate strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn ordering_requirement(
        &self,
    ) -> PreparedSqlScalarAggregateOrderingRequirement {
        self.ordering_requirement
    }

    /// Return the canonical row-source shape for this prepared strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn row_source(&self) -> PreparedSqlScalarAggregateRowSource {
        self.row_source
    }

    /// Return empty-window behavior for this prepared SQL scalar aggregate strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn empty_set_behavior(&self) -> PreparedSqlScalarAggregateEmptySetBehavior {
        self.empty_set_behavior
    }
}

///
/// LoweredSqlGlobalAggregateCommand
///
/// Generic-free global aggregate command shape prepared before typed query
/// binding.
/// This keeps aggregate SQL lowering shared across entities until the final
/// execution boundary converts the base query shape into `Query<E>`.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredSqlGlobalAggregateCommand {
    pub(in crate::db::sql::lowering) query: LoweredBaseQueryShape,
    pub(in crate::db::sql::lowering) terminals: Vec<SqlGlobalAggregateTerminal>,
    pub(in crate::db::sql::lowering) projection: ProjectionSpec,
    pub(in crate::db::sql::lowering) having: Option<Expr>,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db::sql::lowering) output_remap: Vec<usize>,
}

impl LoweredSqlGlobalAggregateCommand {
    /// Lower one constrained global aggregate select into the generic-free
    /// command shape shared by typed and structural aggregate binders.
    fn from_select_statement(statement: SqlSelectStatement) -> Result<Self, SqlLoweringError> {
        let SqlSelectStatement {
            projection,
            projection_aliases,
            predicate,
            distinct,
            group_by,
            having,
            order_by,
            limit,
            offset,
            entity: _,
        } = statement;

        if distinct {
            return Err(SqlLoweringError::unsupported_select_distinct());
        }
        if !group_by.is_empty() {
            return Err(SqlLoweringError::global_aggregate_does_not_support_group_by());
        }
        let projection_for_having = projection.clone();
        let order_by = strip_inert_global_aggregate_output_order_terms(
            order_by,
            &projection_for_having,
            projection_aliases.as_slice(),
        )?;

        let mut lowered_terminals =
            LoweredSqlGlobalAggregateTerminals::from_projection(projection, &projection_aliases)?;
        let having =
            lower_global_aggregate_having_expr(having, &projection_for_having, |aggregate| {
                resolve_or_insert_global_aggregate_terminal_index_from_expr(
                    &mut lowered_terminals.terminals,
                    aggregate,
                )
            })?;

        Ok(Self {
            query: LoweredBaseQueryShape {
                predicate: predicate.as_ref().map(lower_sql_where_expr).transpose()?,
                order_by,
                limit,
                offset,
            },
            terminals: lowered_terminals.terminals,
            projection: lowered_terminals.projection,
            having,
            output_remap: lowered_terminals.output_remap,
        })
    }

    /// Bind this lowered aggregate command onto one entity-owned typed query.
    #[cfg(test)]
    fn into_typed<E: EntityKind>(
        self,
        consistency: MissingRowPolicy,
    ) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
        let Self {
            query,
            terminals,
            projection,
            having,
            output_remap,
        } = self;

        let terminals = terminals
            .iter()
            .map(|terminal| {
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal(E::MODEL, terminal)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SqlGlobalAggregateCommand {
            query: Query::from_inner(crate::db::sql::lowering::apply_lowered_base_query_shape(
                StructuralQuery::new(E::MODEL, consistency),
                query,
            )),
            terminals,
            projection,
            having,
            output_remap,
        })
    }

    /// Bind this lowered aggregate command onto the structural query surface
    /// used by aggregate explain and dynamic SQL execution.
    fn into_structural(
        self,
        model: &'static EntityModel,
        consistency: MissingRowPolicy,
    ) -> Result<SqlGlobalAggregateCommandCore, SqlLoweringError> {
        let Self {
            query,
            terminals,
            projection,
            having,
            output_remap: _,
        } = self;

        let strategies = terminals
            .iter()
            .map(|terminal| {
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal(model, terminal)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SqlGlobalAggregateCommandCore {
            query: crate::db::sql::lowering::apply_lowered_base_query_shape(
                StructuralQuery::new(model, consistency),
                query,
            ),
            strategies,
            projection,
            having,
        })
    }
}

// Drop singleton-result ORDER BY terms that target the global aggregate output
// row itself, while preserving base-row ordering used to shape the aggregate input window.
fn strip_inert_global_aggregate_output_order_terms(
    order_by: Vec<crate::db::sql::parser::SqlOrderTerm>,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Result<Vec<crate::db::sql::parser::SqlOrderTerm>, SqlLoweringError> {
    let inert_targets =
        collect_global_aggregate_output_order_targets(projection, projection_aliases)?;

    Ok(order_by
        .into_iter()
        .filter(|term| !inert_targets.iter().any(|target| target == &term.field))
        .collect())
}

// Collect the canonical ORDER BY spellings that refer to the singleton global
// aggregate output row so the dedicated aggregate lane can ignore them instead
// of re-deriving them as base-row ordering.
fn collect_global_aggregate_output_order_targets(
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Result<Vec<String>, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(Vec::new());
    };

    let mut targets = Vec::with_capacity(items.len());
    for (item, alias) in items.iter().zip(projection_aliases.iter()) {
        let expr = lower_select_item_expr(item, SqlExprPhase::PostAggregate)?;
        if !expr_contains_aggregate(&expr) || expr_references_global_direct_fields(&expr) {
            continue;
        }

        targets.push(render_scalar_projection_expr_sql_label(&expr));
        if let Some(alias) = alias {
            targets.push(alias.clone());
        }
    }

    Ok(targets)
}

///
/// LoweredSqlAggregateShape
///
/// Locally validated aggregate-call shape used by SQL lowering to avoid
/// duplicating `(SqlAggregateKind, field)` validation across lowering lanes.
///
enum LoweredSqlAggregateShape {
    CountRows {
        filter_expr: Option<Expr>,
    },
    CountField {
        field: String,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    FieldTarget {
        kind: SqlAggregateKind,
        field: String,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    ExpressionInput {
        kind: SqlAggregateKind,
        input_expr: Expr,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
}

///
/// SqlGlobalAggregateCommand
///
/// Lowered global SQL aggregate command carrying base query shape plus terminal.
///
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct SqlGlobalAggregateCommand<E: EntityKind> {
    query: Query<E>,
    terminals: Vec<PreparedSqlScalarAggregateStrategy>,
    projection: ProjectionSpec,
    having: Option<Expr>,
    output_remap: Vec<usize>,
}

#[cfg(test)]
impl<E: EntityKind> SqlGlobalAggregateCommand<E> {
    /// Borrow the lowered base query shape for aggregate execution.
    #[must_use]
    pub(crate) const fn query(&self) -> &Query<E> {
        &self.query
    }

    /// Borrow the lowered aggregate terminals.
    #[must_use]
    pub(crate) fn terminals(&self) -> &[PreparedSqlScalarAggregateStrategy] {
        self.terminals.as_slice()
    }

    /// Borrow the canonical output projection contract for this global aggregate command.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    /// Borrow the optional global aggregate HAVING expression.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn having(&self) -> Option<&Expr> {
        self.having.as_ref()
    }

    /// Borrow the output-to-unique-terminal remap preserved from original SQL projection order.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn output_remap(&self) -> &[usize] {
        self.output_remap.as_slice()
    }

    /// Borrow the first lowered aggregate terminal for single-terminal callers.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn terminal(&self) -> &PreparedSqlScalarAggregateStrategy {
        self.terminals
            .first()
            .expect("global aggregate command must contain at least one terminal")
    }
}

///
/// SqlGlobalAggregateCommandCore
///
/// Generic-free lowered global aggregate command bound onto the structural
/// query surface.
/// This keeps global aggregate EXPLAIN on the shared query/explain path until
/// a typed boundary is strictly required.
///
#[derive(Clone, Debug)]
pub(crate) struct SqlGlobalAggregateCommandCore {
    query: StructuralQuery,
    strategies: Vec<PreparedSqlScalarAggregateStrategy>,
    projection: ProjectionSpec,
    having: Option<Expr>,
}

impl SqlGlobalAggregateCommandCore {
    /// Borrow the structural query payload for aggregate explain/execution.
    #[must_use]
    pub(in crate::db) const fn query(&self) -> &StructuralQuery {
        &self.query
    }

    /// Borrow the canonical output projection contract for aggregate-result materialization.
    #[must_use]
    pub(in crate::db) const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    /// Borrow the optional global aggregate HAVING expression.
    #[must_use]
    pub(in crate::db) const fn having(&self) -> Option<&Expr> {
        self.having.as_ref()
    }

    /// Borrow prepared structural SQL scalar aggregate strategies.
    #[must_use]
    pub(in crate::db) const fn strategies(&self) -> &[PreparedSqlScalarAggregateStrategy] {
        self.strategies.as_slice()
    }
}

/// Return whether one parsed SQL statement is an executable constrained global
/// aggregate shape owned by the dedicated aggregate lane.
pub(in crate::db) fn is_sql_global_aggregate_statement(statement: &SqlStatement) -> bool {
    let SqlStatement::Select(statement) = statement else {
        return false;
    };

    is_sql_global_aggregate_select(statement)
}

// Detect one constrained global aggregate select shape without widening any
// non-aggregate SQL surface onto the dedicated aggregate execution lane.
fn is_sql_global_aggregate_select(statement: &SqlSelectStatement) -> bool {
    if statement.distinct || !statement.group_by.is_empty() {
        return false;
    }

    // Skip the heavier global-aggregate shape lowering when one plain scalar
    // SELECT cannot possibly route onto the dedicated aggregate lane.
    if !sql_select_might_require_global_aggregate_lane(statement) {
        return false;
    }

    LoweredSqlGlobalAggregateCommand::from_select_statement(statement.clone()).is_ok()
}

// Use one cheap parsed-shape screen before the dedicated aggregate lane opens
// the full lowering path. Plain scalar selects with no HAVING and no aggregate
// projection items can never become executable global aggregates.
fn sql_select_might_require_global_aggregate_lane(statement: &SqlSelectStatement) -> bool {
    if !statement.having.is_empty() {
        return true;
    }

    match &statement.projection {
        SqlProjection::Items(items) => items.iter().any(select_item_contains_aggregate),
        SqlProjection::All => false,
    }
}

/// Bind one lowered global aggregate EXPLAIN shape onto the structural query
/// surface when the explain command carries that specialized form.
pub(crate) fn bind_lowered_sql_explain_global_aggregate_structural(
    lowered: &LoweredSqlCommand,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
) -> Result<Option<(SqlExplainMode, SqlGlobalAggregateCommandCore)>, SqlLoweringError> {
    let LoweredSqlCommandInner::ExplainGlobalAggregate { mode, command } = &lowered.0 else {
        return Ok(None);
    };

    Ok(Some((
        *mode,
        bind_lowered_sql_global_aggregate_command_structural(model, command.clone(), consistency)?,
    )))
}

/// Parse and lower one SQL statement into global aggregate execution command for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_global_aggregate_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let statement = crate::db::sql::parser::parse_sql(sql)?;
    let prepared = crate::db::sql::lowering::prepare_sql_statement(statement, E::MODEL.name())?;

    compile_sql_global_aggregate_command_from_prepared::<E>(prepared, consistency)
}

// Lower one already-prepared SQL statement into the constrained global
// aggregate command envelope so callers that already parsed and routed the
// statement do not pay the parser again.
#[cfg(test)]
pub(crate) fn compile_sql_global_aggregate_command_from_prepared<E: EntityKind>(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    bind_lowered_sql_global_aggregate_command::<E>(
        lower_global_aggregate_select_shape(statement)?,
        consistency,
    )
}

// Lower one already-prepared SQL statement into the generic-free global
// aggregate command envelope so dynamic SQL surfaces can share the same
// aggregate-shape authority before choosing their outward payload contract.
pub(in crate::db) fn compile_sql_global_aggregate_command_core_from_prepared(
    prepared: PreparedSqlStatement,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommandCore, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    bind_lowered_sql_global_aggregate_command_structural(
        model,
        lower_global_aggregate_select_shape(statement)?,
        consistency,
    )
}

pub(in crate::db::sql::lowering) fn lower_global_aggregate_select_shape(
    statement: SqlSelectStatement,
) -> Result<LoweredSqlGlobalAggregateCommand, SqlLoweringError> {
    LoweredSqlGlobalAggregateCommand::from_select_statement(statement)
}

#[cfg(test)]
pub(in crate::db::sql::lowering) fn bind_lowered_sql_global_aggregate_command<E: EntityKind>(
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    lowered.into_typed::<E>(consistency)
}

fn bind_lowered_sql_global_aggregate_command_structural(
    model: &'static EntityModel,
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommandCore, SqlLoweringError> {
    lowered.into_structural(model, consistency)
}

fn lower_global_aggregate_terminal(
    aggregate_expr: &AggregateExpr,
) -> Result<SqlGlobalAggregateTerminal, SqlLoweringError> {
    let distinct = aggregate_expr.is_distinct();
    let filter_expr = aggregate_expr.filter_expr().cloned();

    match (
        aggregate_expr.kind(),
        aggregate_expr.target_field().map(str::to_string),
        aggregate_expr.input_expr().cloned(),
    ) {
        (AggregateKind::Count, None, None) => {
            Ok(SqlGlobalAggregateTerminal::CountRows { filter_expr })
        }
        (AggregateKind::Count, Some(field), _) => Ok(SqlGlobalAggregateTerminal::CountField {
            field,
            filter_expr,
            distinct,
        }),
        (AggregateKind::Count, None, Some(input_expr)) => {
            Ok(SqlGlobalAggregateTerminal::CountExpr {
                input_expr,
                filter_expr,
                distinct,
            })
        }
        (AggregateKind::Sum, Some(field), _) => Ok(SqlGlobalAggregateTerminal::SumField {
            field,
            filter_expr,
            distinct,
        }),
        (AggregateKind::Sum, None, Some(input_expr)) => Ok(SqlGlobalAggregateTerminal::SumExpr {
            input_expr,
            filter_expr,
            distinct,
        }),
        (AggregateKind::Avg, Some(field), _) => Ok(SqlGlobalAggregateTerminal::AvgField {
            field,
            filter_expr,
            distinct,
        }),
        (AggregateKind::Avg, None, Some(input_expr)) => Ok(SqlGlobalAggregateTerminal::AvgExpr {
            input_expr,
            filter_expr,
            distinct,
        }),
        (AggregateKind::Min, Some(field), _) => {
            Ok(SqlGlobalAggregateTerminal::MinField { field, filter_expr })
        }
        (AggregateKind::Min, None, Some(input_expr)) => Ok(SqlGlobalAggregateTerminal::MinExpr {
            input_expr,
            filter_expr,
        }),
        (AggregateKind::Max, Some(field), _) => {
            Ok(SqlGlobalAggregateTerminal::MaxField { field, filter_expr })
        }
        (AggregateKind::Max, None, Some(input_expr)) => Ok(SqlGlobalAggregateTerminal::MaxExpr {
            input_expr,
            filter_expr,
        }),
        (AggregateKind::Exists | AggregateKind::First | AggregateKind::Last, _, _)
        | (_, None, None) => Err(SqlLoweringError::unsupported_global_aggregate_projection()),
    }
}

fn resolve_or_insert_global_aggregate_terminal_index_from_expr(
    terminals: &mut Vec<SqlGlobalAggregateTerminal>,
    aggregate_expr: &AggregateExpr,
) -> Result<usize, SqlLoweringError> {
    let terminal = lower_global_aggregate_terminal(aggregate_expr)?;

    Ok(terminals
        .iter()
        .position(|current| current == &terminal)
        .unwrap_or_else(|| {
            let index = terminals.len();
            terminals.push(terminal);
            index
        }))
}

pub(in crate::db::sql::lowering) fn resolve_having_aggregate_expr_index(
    target: &AggregateExpr,
    grouped_projection_aggregates: &[SqlAggregateCall],
) -> Result<usize, SqlLoweringError> {
    let mut matched =
        grouped_projection_aggregates
            .iter()
            .enumerate()
            .filter_map(|(index, aggregate)| {
                lower_aggregate_call(aggregate.clone())
                    .ok()
                    .filter(|current| current == target)
                    .map(|_| index)
            });
    let Some(index) = matched.next() else {
        return Err(SqlLoweringError::unsupported_select_having());
    };
    if matched.next().is_some() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    Ok(index)
}

///
/// LoweredSqlGlobalAggregateTerminals
///
/// Canonical global aggregate lowering result that keeps only unique
/// executable terminals plus one remap back to original SQL projection order.
///
struct LoweredSqlGlobalAggregateTerminals {
    terminals: Vec<SqlGlobalAggregateTerminal>,
    projection: ProjectionSpec,
    output_remap: Vec<usize>,
}

impl LoweredSqlGlobalAggregateTerminals {
    /// Lower one SQL projection into unique executable aggregate terminals plus
    /// the output remap needed to preserve original projection order.
    fn from_projection(
        projection: SqlProjection,
        projection_aliases: &[Option<String>],
    ) -> Result<Self, SqlLoweringError> {
        let SqlProjection::Items(items) = projection else {
            return Err(SqlLoweringError::unsupported_global_aggregate_projection());
        };
        if items.is_empty() {
            return Err(SqlLoweringError::unsupported_global_aggregate_projection());
        }

        let mut terminals = Vec::<SqlGlobalAggregateTerminal>::with_capacity(items.len());
        let mut output_remap = Vec::<usize>::with_capacity(items.len());
        let mut fields = Vec::<ProjectionField>::with_capacity(items.len());
        let mut saw_wrapped_projection = false;

        for (index, item) in items.into_iter().enumerate() {
            let expr = lower_select_item_expr(&item, SqlExprPhase::PostAggregate)?;
            if !expr_contains_aggregate(&expr) || expr_references_global_direct_fields(&expr) {
                return Err(SqlLoweringError::unsupported_global_aggregate_projection());
            }

            let direct_terminal_index =
                collect_unique_global_aggregate_terminals_from_expr(&expr, &mut terminals)?;
            match direct_terminal_index {
                Some(unique_index) => output_remap.push(unique_index),
                None => {
                    saw_wrapped_projection = true;
                }
            }

            fields.push(ProjectionField::Scalar {
                expr,
                alias: projection_aliases
                    .get(index)
                    .and_then(Option::as_deref)
                    .map(Alias::new),
            });
        }

        Ok(Self {
            terminals,
            projection: lower_global_aggregate_projection(fields),
            output_remap: if saw_wrapped_projection {
                Vec::new()
            } else {
                output_remap
            },
        })
    }
}

// Global post-aggregate projection expressions may compose aggregate leaves
// with literals/functions/arithmetic, but they may not reopen direct field
// access outside aggregate inputs.
pub(in crate::db::sql::lowering) fn expr_references_global_direct_fields(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) => true,
        Expr::Aggregate(_) | Expr::Literal(_) => false,
        Expr::FunctionCall { args, .. } => args.iter().any(expr_references_global_direct_fields),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().any(|arm| {
                expr_references_global_direct_fields(arm.condition())
                    || expr_references_global_direct_fields(arm.result())
            }) || expr_references_global_direct_fields(else_expr.as_ref())
        }
        Expr::Binary { left, right, .. } => {
            expr_references_global_direct_fields(left.as_ref())
                || expr_references_global_direct_fields(right.as_ref())
        }
        Expr::Unary { expr, .. } => expr_references_global_direct_fields(expr.as_ref()),
        #[cfg(test)]
        Expr::Alias { expr, .. } => expr_references_global_direct_fields(expr.as_ref()),
    }
}

// Visit aggregate leaves in one planner-owned expression tree while keeping
// recursive tree ownership on one shared lowering helper.
pub(in crate::db::sql::lowering) fn try_for_each_expr_aggregate<F>(
    expr: &Expr,
    visit: &mut F,
) -> Result<(), SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<(), SqlLoweringError>,
{
    match expr {
        Expr::Field(_) | Expr::Literal(_) => Ok(()),
        Expr::Aggregate(aggregate) => visit(aggregate),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                try_for_each_expr_aggregate(arg, visit)?;
            }

            Ok(())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                try_for_each_expr_aggregate(arm.condition(), visit)?;
                try_for_each_expr_aggregate(arm.result(), visit)?;
            }
            try_for_each_expr_aggregate(else_expr.as_ref(), visit)
        }
        Expr::Binary { left, right, .. } => {
            try_for_each_expr_aggregate(left.as_ref(), visit)?;
            try_for_each_expr_aggregate(right.as_ref(), visit)
        }
        Expr::Unary { expr, .. } => try_for_each_expr_aggregate(expr.as_ref(), visit),
        #[cfg(test)]
        Expr::Alias { expr, .. } => try_for_each_expr_aggregate(expr.as_ref(), visit),
    }
}

// Collect every aggregate leaf referenced by one global post-aggregate output
// expression while deduplicating onto the canonical executable terminal list.
// Direct aggregate terminals still report the first-seen terminal remap so the
// legacy terminal-remap tests keep their existing contract.
fn collect_unique_global_aggregate_terminals_from_expr(
    expr: &Expr,
    terminals: &mut Vec<SqlGlobalAggregateTerminal>,
) -> Result<Option<usize>, SqlLoweringError> {
    let mut direct_terminal_index = None;
    try_for_each_expr_aggregate(expr, &mut |aggregate_expr| {
        let terminal = lower_global_aggregate_terminal(aggregate_expr)?;
        let unique_index = terminals
            .iter()
            .position(|current| current == &terminal)
            .unwrap_or_else(|| {
                let index = terminals.len();
                terminals.push(terminal);
                index
            });
        if direct_terminal_index.is_none() && matches!(expr, Expr::Aggregate(_)) {
            direct_terminal_index = Some(unique_index);
        }

        Ok(())
    })?;

    Ok(direct_terminal_index)
}

fn lower_sql_aggregate_shape(
    call: SqlAggregateCall,
) -> Result<LoweredSqlAggregateShape, SqlLoweringError> {
    let SqlAggregateCall {
        kind,
        input,
        filter_expr,
        distinct,
    } = call;
    let filter_expr = filter_expr
        .map(|expr| lower_sql_where_bool_expr(expr.as_ref()))
        .transpose()?;

    if distinct && filter_expr.is_some() {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    match (kind, input.map(|input| *input), distinct) {
        (SqlAggregateKind::Count, None, false) => {
            Ok(LoweredSqlAggregateShape::CountRows { filter_expr })
        }
        (SqlAggregateKind::Count, Some(SqlAggregateInputExpr::Field(field)), distinct) => {
            Ok(LoweredSqlAggregateShape::CountField {
                field,
                filter_expr,
                distinct,
            })
        }
        (
            kind @ (SqlAggregateKind::Sum
            | SqlAggregateKind::Avg
            | SqlAggregateKind::Min
            | SqlAggregateKind::Max),
            Some(SqlAggregateInputExpr::Field(field)),
            distinct,
        ) => Ok(LoweredSqlAggregateShape::FieldTarget {
            kind,
            field,
            filter_expr,
            distinct,
        }),
        (
            kind @ (SqlAggregateKind::Count
            | SqlAggregateKind::Sum
            | SqlAggregateKind::Avg
            | SqlAggregateKind::Min
            | SqlAggregateKind::Max),
            Some(input),
            distinct,
        ) => Ok(LoweredSqlAggregateShape::ExpressionInput {
            kind,
            input_expr: canonicalize_aggregate_input_expr(
                match kind {
                    SqlAggregateKind::Count => AggregateKind::Count,
                    SqlAggregateKind::Sum => AggregateKind::Sum,
                    SqlAggregateKind::Avg => AggregateKind::Avg,
                    SqlAggregateKind::Min => AggregateKind::Min,
                    SqlAggregateKind::Max => AggregateKind::Max,
                },
                lower_sql_aggregate_input_expr(input)?,
            ),
            filter_expr,
            distinct,
        }),
        _ => Err(SqlLoweringError::unsupported_select_projection()),
    }
}

pub(in crate::db::sql::lowering) fn grouped_projection_aggregate_calls(
    projection: &SqlProjection,
    group_by_fields: &[String],
    model: &'static EntityModel,
) -> Result<Vec<SqlAggregateCall>, SqlLoweringError> {
    if group_by_fields.is_empty() {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::grouped_projection_requires_explicit_list());
    };

    GroupedProjectionAggregateCollector::new(group_by_fields, model)?.collect_from_items(items)
}

// Extend one unique aggregate-call list from one SQL expression while keeping
// first-seen SQL order stable for grouped reducer slot assignment.
pub(in crate::db::sql::lowering) fn extend_unique_sql_expr_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    expr: &SqlExpr,
) {
    match expr {
        SqlExpr::Field(_) | SqlExpr::Literal(_) | SqlExpr::TextFunction(_) => {}
        SqlExpr::Aggregate(aggregate) => {
            push_unique_sql_aggregate_call(aggregate_calls, aggregate.clone());
        }
        SqlExpr::Membership { expr, .. }
        | SqlExpr::NullTest { expr, .. }
        | SqlExpr::Unary { expr, .. } => {
            extend_unique_sql_expr_aggregate_calls(aggregate_calls, expr);
        }
        SqlExpr::FunctionCall { args, .. } => {
            for arg in args {
                extend_unique_sql_expr_aggregate_calls(aggregate_calls, arg);
            }
        }
        SqlExpr::Round(call) => {
            extend_unique_round_input_aggregate_calls(aggregate_calls, &call.input);
        }
        SqlExpr::Binary { left, right, .. } => {
            extend_unique_sql_expr_aggregate_calls(aggregate_calls, left);
            extend_unique_sql_expr_aggregate_calls(aggregate_calls, right);
        }
        SqlExpr::Case { arms, else_expr } => {
            for arm in arms {
                extend_unique_sql_expr_aggregate_calls(aggregate_calls, &arm.condition);
                extend_unique_sql_expr_aggregate_calls(aggregate_calls, &arm.result);
            }
            if let Some(else_expr) = else_expr {
                extend_unique_sql_expr_aggregate_calls(aggregate_calls, else_expr);
            }
        }
    }
}

// Extend one unique aggregate-call list from one SQL select item while keeping
// SQL item-order ownership local to shared aggregate collection helpers.
pub(in crate::db::sql::lowering) fn extend_unique_sql_select_item_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    item: &SqlSelectItem,
) {
    match item {
        SqlSelectItem::Field(_) | SqlSelectItem::TextFunction(_) => {}
        SqlSelectItem::Aggregate(aggregate) => {
            push_unique_sql_aggregate_call(aggregate_calls, aggregate.clone());
        }
        SqlSelectItem::Arithmetic(call) => {
            extend_unique_projection_operand_aggregate_calls(aggregate_calls, &call.left);
            extend_unique_projection_operand_aggregate_calls(aggregate_calls, &call.right);
        }
        SqlSelectItem::Round(call) => {
            extend_unique_round_input_aggregate_calls(aggregate_calls, &call.input);
        }
        SqlSelectItem::Expr(expr) => {
            extend_unique_sql_expr_aggregate_calls(aggregate_calls, expr);
        }
    }
}

///
/// GroupedProjectionAggregateCollector
///
/// Local grouped-projection aggregate extraction owner. It validates grouped
/// field authority, preserves the first aggregate ordering rule, and keeps one
/// stable unique aggregate list so grouped reducer slots are derived once.
///

struct GroupedProjectionAggregateCollector<'a> {
    grouped_field_names: Vec<&'a str>,
    model: &'static EntityModel,
    aggregate_calls: Vec<SqlAggregateCall>,
    seen_aggregate: bool,
}

impl<'a> GroupedProjectionAggregateCollector<'a> {
    // Build the grouped projection collector once so field-authority and
    // aggregate-ordering policy stay on one local owner.
    fn new(
        group_by_fields: &'a [String],
        model: &'static EntityModel,
    ) -> Result<Self, SqlLoweringError> {
        if group_by_fields.is_empty() {
            return Err(SqlLoweringError::unsupported_select_group_by());
        }

        Ok(Self {
            grouped_field_names: group_by_fields.iter().map(String::as_str).collect(),
            model,
            aggregate_calls: Vec::new(),
            seen_aggregate: false,
        })
    }

    // Walk grouped projection items in SQL order so first-seen aggregate leaves
    // map onto one stable grouped reducer slot ordering.
    fn collect_from_items(
        mut self,
        items: &[SqlSelectItem],
    ) -> Result<Vec<SqlAggregateCall>, SqlLoweringError> {
        for (index, item) in items.iter().enumerate() {
            self.collect_item(index, item)?;
        }

        if self.aggregate_calls.is_empty() {
            return Err(SqlLoweringError::grouped_projection_requires_aggregate());
        }

        Ok(self.aggregate_calls)
    }

    // Validate one grouped projection item before collecting any aggregate
    // leaves so field-resolution and grouped-key diagnostics stay precise.
    fn collect_item(&mut self, index: usize, item: &SqlSelectItem) -> Result<(), SqlLoweringError> {
        let expr = crate::db::sql::lowering::select::lower_select_item_expr(
            item,
            SqlExprPhase::PostAggregate,
        )?;
        let contains_aggregate = expr_contains_aggregate(&expr);
        if self.seen_aggregate && !contains_aggregate {
            return Err(SqlLoweringError::grouped_projection_scalar_after_aggregate(
                index,
            ));
        }
        if let Some(field) = first_unknown_field_in_expr(&expr, self.model) {
            return Err(SqlLoweringError::unknown_field(field));
        }
        if !expr_references_only_fields(&expr, self.grouped_field_names.as_slice()) {
            return Err(SqlLoweringError::grouped_projection_references_non_group_field(index));
        }
        if contains_aggregate {
            self.seen_aggregate = true;
            extend_unique_sql_select_item_aggregate_calls(&mut self.aggregate_calls, item);
        }

        Ok(())
    }
}

// Only aggregate operands contribute grouped reducer slots, so the operand
// walk stays intentionally narrow on one shared helper.
fn extend_unique_projection_operand_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    operand: &SqlProjectionOperand,
) {
    match operand {
        SqlProjectionOperand::Field(_) | SqlProjectionOperand::Literal(_) => {}
        SqlProjectionOperand::Aggregate(aggregate) => {
            push_unique_sql_aggregate_call(aggregate_calls, aggregate.clone());
        }
        SqlProjectionOperand::Arithmetic(call) => {
            extend_unique_projection_operand_aggregate_calls(aggregate_calls, &call.left);
            extend_unique_projection_operand_aggregate_calls(aggregate_calls, &call.right);
        }
    }
}

// Extend one unique aggregate-call list from one ROUND input shape while
// keeping ROUND-specific SQL structure local to the shared collector helpers.
fn extend_unique_round_input_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    input: &SqlRoundProjectionInput,
) {
    match input {
        SqlRoundProjectionInput::Operand(operand) => {
            extend_unique_projection_operand_aggregate_calls(aggregate_calls, operand);
        }
        SqlRoundProjectionInput::Arithmetic(call) => {
            extend_unique_projection_operand_aggregate_calls(aggregate_calls, &call.left);
            extend_unique_projection_operand_aggregate_calls(aggregate_calls, &call.right);
        }
    }
}

// Keep aggregate extraction on one stable first-seen unique terminal order so
// repeated SQL aggregate leaves reuse the same reducer slot.
fn push_unique_sql_aggregate_call(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    aggregate: SqlAggregateCall,
) {
    if aggregate_calls.iter().all(|current| current != &aggregate) {
        aggregate_calls.push(aggregate);
    }
}

// Preserve field-resolution diagnostics during grouped aggregate extraction so
// grouped projection typos do not collapse into the generic unsupported shape.
fn first_unknown_field_in_expr(expr: &Expr, model: &EntityModel) -> Option<String> {
    match expr {
        Expr::Field(field) => (resolve_field_slot(model, field.as_str()).is_none())
            .then(|| field.as_str().to_string()),
        Expr::Literal(_) | Expr::Aggregate(_) => None,
        Expr::FunctionCall { args, .. } => args
            .iter()
            .find_map(|arg| first_unknown_field_in_expr(arg, model)),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => when_then_arms
            .iter()
            .find_map(|arm| {
                first_unknown_field_in_expr(arm.condition(), model)
                    .or_else(|| first_unknown_field_in_expr(arm.result(), model))
            })
            .or_else(|| first_unknown_field_in_expr(else_expr, model)),
        Expr::Binary { left, right, .. } => first_unknown_field_in_expr(left, model)
            .or_else(|| first_unknown_field_in_expr(right, model)),
        Expr::Unary { expr, .. } => first_unknown_field_in_expr(expr, model),
        #[cfg(test)]
        Expr::Alias { expr, .. } => first_unknown_field_in_expr(expr, model),
    }
}

pub(in crate::db::sql::lowering) fn lower_aggregate_call(
    call: SqlAggregateCall,
) -> Result<crate::db::query::builder::AggregateExpr, SqlLoweringError> {
    match lower_sql_aggregate_shape(call)? {
        LoweredSqlAggregateShape::CountRows { filter_expr } => {
            Ok(apply_aggregate_filter_expr(count(), filter_expr))
        }
        LoweredSqlAggregateShape::CountField {
            field,
            filter_expr,
            distinct: false,
        } => Ok(apply_aggregate_filter_expr(count_by(field), filter_expr)),
        LoweredSqlAggregateShape::CountField {
            field,
            filter_expr,
            distinct: true,
        } => Ok(apply_aggregate_filter_expr(
            count_by(field).distinct(),
            filter_expr,
        )),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Sum,
            field,
            filter_expr,
            distinct: false,
        } => Ok(apply_aggregate_filter_expr(sum(field), filter_expr)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Sum,
            field,
            filter_expr,
            distinct: true,
        } => Ok(apply_aggregate_filter_expr(
            sum(field).distinct(),
            filter_expr,
        )),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Avg,
            field,
            filter_expr,
            distinct: false,
        } => Ok(apply_aggregate_filter_expr(avg(field), filter_expr)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Avg,
            field,
            filter_expr,
            distinct: true,
        } => Ok(apply_aggregate_filter_expr(
            avg(field).distinct(),
            filter_expr,
        )),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Min,
            field,
            filter_expr,
            distinct: _,
        } => Ok(apply_aggregate_filter_expr(min_by(field), filter_expr)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Max,
            field,
            filter_expr,
            distinct: _,
        } => Ok(apply_aggregate_filter_expr(max_by(field), filter_expr)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Count,
            ..
        } => Err(SqlLoweringError::unsupported_select_projection()),
        LoweredSqlAggregateShape::ExpressionInput {
            kind,
            input_expr,
            filter_expr,
            distinct,
        } => Ok(apply_aggregate_filter_expr(
            lower_expression_owned_aggregate_call(kind, input_expr, distinct),
            filter_expr,
        )),
    }
}

// Attach one optional normalized planner-owned filter expression to an
// aggregate expression so parser/lowering support can stay on the aggregate
// identity boundary without reopening aggregate construction at callsites.
fn apply_aggregate_filter_expr(
    aggregate: AggregateExpr,
    filter_expr: Option<Expr>,
) -> AggregateExpr {
    match filter_expr {
        Some(filter_expr) => aggregate.with_filter_expr(filter_expr),
        None => aggregate,
    }
}

fn lower_expression_owned_aggregate_call(
    kind: SqlAggregateKind,
    input_expr: Expr,
    distinct: bool,
) -> AggregateExpr {
    let aggregate_kind = match kind {
        SqlAggregateKind::Count => AggregateKind::Count,
        SqlAggregateKind::Sum => AggregateKind::Sum,
        SqlAggregateKind::Avg => AggregateKind::Avg,
        SqlAggregateKind::Min => AggregateKind::Min,
        SqlAggregateKind::Max => AggregateKind::Max,
    };
    let aggregate = AggregateExpr::from_expression_input(aggregate_kind, input_expr);

    if distinct {
        aggregate.distinct()
    } else {
        aggregate
    }
}

fn lower_sql_aggregate_input_expr(expr: SqlAggregateInputExpr) -> Result<Expr, SqlLoweringError> {
    let lowered = lower_sql_expr(
        &SqlExpr::from_aggregate_input_expr(&expr),
        SqlExprPhase::PreAggregate,
    )?;

    Ok(fold_sql_aggregate_input_constant_expr(lowered))
}

// Fold one aggregate-input expression when it is fully constant under the
// bounded aggregate-input surface. This keeps aggregate terminal identity on
// one planner-owned canonical shape before dedupe and execution wiring.
fn fold_sql_aggregate_input_constant_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Field(_) | Expr::Literal(_) | Expr::Aggregate(_) => expr,
        Expr::FunctionCall { function, args } => {
            let args = args
                .into_iter()
                .map(fold_sql_aggregate_input_constant_expr)
                .collect::<Vec<_>>();

            fold_sql_aggregate_input_constant_function(function, args.as_slice())
                .unwrap_or(Expr::FunctionCall { function, args })
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    crate::db::query::plan::expr::CaseWhenArm::new(
                        fold_sql_aggregate_input_constant_expr(arm.condition().clone()),
                        fold_sql_aggregate_input_constant_expr(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(fold_sql_aggregate_input_constant_expr(*else_expr)),
        },
        Expr::Binary { op, left, right } => {
            let left = fold_sql_aggregate_input_constant_expr(*left);
            let right = fold_sql_aggregate_input_constant_expr(*right);

            fold_sql_aggregate_input_constant_binary(op, &left, &right).unwrap_or_else(|| {
                Expr::Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            })
        }
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(fold_sql_aggregate_input_constant_expr(*expr)),
            name,
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(fold_sql_aggregate_input_constant_expr(*expr)),
        },
    }
}

// Fold one literal-only aggregate-input binary expression so semantic
// aggregate dedupe can treat `SUM(2 * 3)` and `SUM(6)` as the same input.
fn fold_sql_aggregate_input_constant_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
) -> Option<Expr> {
    let (Expr::Literal(left), Expr::Literal(right)) = (left, right) else {
        return None;
    };
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Some(Expr::Literal(Value::Null));
    }

    let arithmetic_op = match op {
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => return None,
        BinaryOp::Add => NumericArithmeticOp::Add,
        BinaryOp::Sub => NumericArithmeticOp::Sub,
        BinaryOp::Mul => NumericArithmeticOp::Mul,
        BinaryOp::Div => NumericArithmeticOp::Div,
    };
    let result = apply_numeric_arithmetic(arithmetic_op, left, right)?;

    Some(Expr::Literal(Value::Decimal(result)))
}

// Fold one literal-only aggregate-input function call when the admitted
// aggregate-input family defines a deterministic literal result.
fn fold_sql_aggregate_input_constant_function(function: Function, args: &[Expr]) -> Option<Expr> {
    match function {
        Function::Round => fold_sql_aggregate_input_round(args),
        Function::IsNull
        | Function::IsNotNull
        | Function::Trim
        | Function::Ltrim
        | Function::Rtrim
        | Function::Lower
        | Function::Upper
        | Function::Length
        | Function::Left
        | Function::Right
        | Function::StartsWith
        | Function::EndsWith
        | Function::Contains
        | Function::Position
        | Function::Replace
        | Function::Substring => None,
    }
}

fn fold_sql_aggregate_input_round(args: &[Expr]) -> Option<Expr> {
    let [Expr::Literal(input), Expr::Literal(scale)] = args else {
        return None;
    };
    if matches!(input, Value::Null) || matches!(scale, Value::Null) {
        return Some(Expr::Literal(Value::Null));
    }

    let scale = match scale {
        Value::Int(value) => u32::try_from(*value).ok()?,
        Value::Uint(value) => u32::try_from(*value).ok()?,
        _ => return None,
    };
    let decimal = input.to_numeric_decimal()?;

    Some(Expr::Literal(Value::Decimal(decimal.round_dp(scale))))
}
