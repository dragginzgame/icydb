use crate::db::sql::lowering::{
    LoweredBaseQueryShape, LoweredSqlCommand, LoweredSqlCommandInner, PreparedSqlStatement,
    SqlLoweringError,
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
                aggregate::{avg, count, count_by, max_by, min_by, sum},
            },
            intent::StructuralQuery,
            plan::{
                AggregateKind, FieldSlot,
                expr::{
                    BinaryOp, Expr, FieldId, Function, compile_scalar_projection_expr,
                    expr_references_only_fields,
                },
                resolve_aggregate_target_field_slot,
            },
        },
        sql::parser::{
            SqlAggregateCall, SqlAggregateInputExpr, SqlAggregateKind, SqlExplainMode,
            SqlProjection, SqlProjectionOperand, SqlRoundProjectionInput, SqlSelectItem,
            SqlSelectStatement, SqlStatement,
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
    CountRows,
    CountField { field: String, distinct: bool },
    CountExpr { input_expr: Expr, distinct: bool },
    SumField { field: String, distinct: bool },
    SumExpr { input_expr: Expr, distinct: bool },
    AvgField { field: String, distinct: bool },
    AvgExpr { input_expr: Expr, distinct: bool },
    MinField(String),
    MinExpr { input_expr: Expr },
    MaxField(String),
    MaxExpr { input_expr: Expr },
}

///
/// TypedSqlGlobalAggregateTerminal
///
/// TypedSqlGlobalAggregateTerminal is the typed global aggregate contract used
/// after entity binding resolves one concrete model.
/// Field-target variants carry a resolved planner field slot so typed SQL
/// aggregate execution does not re-resolve the same field name before dispatch.
///
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TypedSqlGlobalAggregateTerminal {
    CountRows,
    CountField {
        target_slot: FieldSlot,
        distinct: bool,
    },
    CountExpr {
        input_expr: Expr,
        distinct: bool,
    },
    SumField {
        target_slot: FieldSlot,
        distinct: bool,
    },
    SumExpr {
        input_expr: Expr,
        distinct: bool,
    },
    AvgField {
        target_slot: FieldSlot,
        distinct: bool,
    },
    AvgExpr {
        input_expr: Expr,
        distinct: bool,
    },
    MinField(FieldSlot),
    MinExpr {
        input_expr: Expr,
    },
    MaxField(FieldSlot),
    MaxExpr {
        input_expr: Expr,
    },
}

/// PreparedSqlScalarAggregateDomain
///
/// Typed SQL scalar aggregate execution domain selected before session runtime
/// dispatch. This keeps the typed aggregate lane explicit about which internal
/// execution family will consume the request.
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
        distinct_input: bool,
        descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
    ) -> Self {
        let policy = Self::descriptor_policy(descriptor_shape);

        Self {
            target_slot,
            input_expr,
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
            SqlGlobalAggregateTerminal::CountRows => Ok(Self::from_resolved_shape(
                None,
                None,
                false,
                PreparedSqlScalarAggregateDescriptorShape::CountRows,
            )),
            SqlGlobalAggregateTerminal::CountField { field, distinct } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::CountField,
                ))
            }
            SqlGlobalAggregateTerminal::CountExpr {
                input_expr,
                distinct,
            } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::CountField,
                ))
            }
            SqlGlobalAggregateTerminal::SumField { field, distinct } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::SumField,
                ))
            }
            SqlGlobalAggregateTerminal::SumExpr {
                input_expr,
                distinct,
            } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::SumField,
                ))
            }
            SqlGlobalAggregateTerminal::AvgField { field, distinct } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::AvgField,
                ))
            }
            SqlGlobalAggregateTerminal::AvgExpr {
                input_expr,
                distinct,
            } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    *distinct,
                    PreparedSqlScalarAggregateDescriptorShape::AvgField,
                ))
            }
            SqlGlobalAggregateTerminal::MinField(field) => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    false,
                    PreparedSqlScalarAggregateDescriptorShape::MinField,
                ))
            }
            SqlGlobalAggregateTerminal::MinExpr { input_expr } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
                    false,
                    PreparedSqlScalarAggregateDescriptorShape::MinField,
                ))
            }
            SqlGlobalAggregateTerminal::MaxField(field) => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::from_resolved_shape(
                    Some(target_slot),
                    None,
                    false,
                    PreparedSqlScalarAggregateDescriptorShape::MaxField,
                ))
            }
            SqlGlobalAggregateTerminal::MaxExpr { input_expr } => {
                validate_input_expr(input_expr)?;

                Ok(Self::from_resolved_shape(
                    None,
                    Some(input_expr.clone()),
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
    pub(in crate::db::sql::lowering) output_remap: Vec<usize>,
}

impl LoweredSqlGlobalAggregateCommand {
    /// Lower one constrained global aggregate select into the generic-free
    /// command shape shared by typed and structural aggregate binders.
    fn from_select_statement(statement: SqlSelectStatement) -> Result<Self, SqlLoweringError> {
        let SqlSelectStatement {
            projection,
            projection_aliases: _,
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
        if !having.is_empty() {
            return Err(SqlLoweringError::having_requires_group_by());
        }

        let lowered_terminals = LoweredSqlGlobalAggregateTerminals::from_projection(projection)?;

        Ok(Self {
            query: LoweredBaseQueryShape {
                predicate,
                order_by,
                limit,
                offset,
            },
            terminals: lowered_terminals.terminals,
            output_remap: lowered_terminals.output_remap,
        })
    }

    /// Bind this lowered aggregate command onto one entity-owned typed query.
    #[cfg(test)]
    fn into_typed<E: EntityKind>(
        self,
        consistency: MissingRowPolicy,
    ) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
        let terminals = self
            .terminals
            .into_iter()
            .map(bind_lowered_sql_global_aggregate_terminal::<E>)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SqlGlobalAggregateCommand {
            query: Query::from_inner(crate::db::sql::lowering::apply_lowered_base_query_shape(
                StructuralQuery::new(E::MODEL, consistency),
                self.query,
            )),
            terminals,
            output_remap: self.output_remap,
        })
    }

    /// Bind this lowered aggregate command onto the structural query surface
    /// used by aggregate explain and dynamic SQL execution.
    fn into_structural(
        self,
        model: &'static EntityModel,
        consistency: MissingRowPolicy,
    ) -> SqlGlobalAggregateCommandCore {
        SqlGlobalAggregateCommandCore {
            query: crate::db::sql::lowering::apply_lowered_base_query_shape(
                StructuralQuery::new(model, consistency),
                self.query,
            ),
            terminals: self.terminals,
            output_remap: self.output_remap,
        }
    }
}

///
/// LoweredSqlAggregateShape
///
/// Locally validated aggregate-call shape used by SQL lowering to avoid
/// duplicating `(SqlAggregateKind, field)` validation across lowering lanes.
///
enum LoweredSqlAggregateShape {
    CountRows,
    CountField {
        field: String,
        distinct: bool,
    },
    FieldTarget {
        kind: SqlAggregateKind,
        field: String,
        distinct: bool,
    },
    ExpressionInput {
        kind: SqlAggregateKind,
        input_expr: Expr,
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
    terminals: Vec<TypedSqlGlobalAggregateTerminal>,
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
    pub(crate) fn terminals(&self) -> &[TypedSqlGlobalAggregateTerminal] {
        self.terminals.as_slice()
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
    pub(crate) fn terminal(&self) -> &TypedSqlGlobalAggregateTerminal {
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
    terminals: Vec<SqlGlobalAggregateTerminal>,
    output_remap: Vec<usize>,
}

impl SqlGlobalAggregateCommandCore {
    /// Borrow the structural query payload for aggregate explain/execution.
    #[must_use]
    pub(in crate::db) const fn query(&self) -> &StructuralQuery {
        &self.query
    }

    /// Borrow the output remap used to fan unique aggregate terminal results back out.
    #[must_use]
    pub(in crate::db) const fn output_remap(&self) -> &[usize] {
        self.output_remap.as_slice()
    }

    /// Prepare structural SQL scalar aggregate strategies using one concrete model.
    pub(in crate::db) fn prepared_scalar_strategies(
        &self,
        model: &'static EntityModel,
    ) -> Result<Vec<PreparedSqlScalarAggregateStrategy>, SqlLoweringError> {
        self.terminals
            .iter()
            .map(|terminal| {
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal(model, terminal)
            })
            .collect()
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
    if statement.distinct || !statement.group_by.is_empty() || !statement.having.is_empty() {
        return false;
    }

    LoweredSqlGlobalAggregateTerminals::from_projection(statement.projection.clone()).is_ok()
}

/// Bind one lowered global aggregate EXPLAIN shape onto the structural query
/// surface when the explain command carries that specialized form.
pub(crate) fn bind_lowered_sql_explain_global_aggregate_structural(
    lowered: &LoweredSqlCommand,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
) -> Option<(SqlExplainMode, SqlGlobalAggregateCommandCore)> {
    let LoweredSqlCommandInner::ExplainGlobalAggregate { mode, command } = &lowered.0 else {
        return None;
    };

    Some((
        *mode,
        bind_lowered_sql_global_aggregate_command_structural(model, command.clone(), consistency),
    ))
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

    Ok(bind_lowered_sql_global_aggregate_command_structural(
        model,
        lower_global_aggregate_select_shape(statement)?,
        consistency,
    ))
}

#[cfg(test)]
fn bind_lowered_sql_global_aggregate_terminal<E: EntityKind>(
    terminal: SqlGlobalAggregateTerminal,
) -> Result<TypedSqlGlobalAggregateTerminal, SqlLoweringError> {
    let resolve_target_slot = |field: &str| {
        resolve_aggregate_target_field_slot(E::MODEL, field).map_err(SqlLoweringError::from)
    };

    match terminal {
        SqlGlobalAggregateTerminal::CountRows => Ok(TypedSqlGlobalAggregateTerminal::CountRows),
        SqlGlobalAggregateTerminal::CountField { field, distinct } => {
            Ok(TypedSqlGlobalAggregateTerminal::CountField {
                target_slot: resolve_target_slot(field.as_str())?,
                distinct,
            })
        }
        SqlGlobalAggregateTerminal::CountExpr {
            input_expr,
            distinct,
        } => {
            if let Some(field) = first_unknown_field_in_expr(&input_expr, E::MODEL) {
                return Err(SqlLoweringError::unknown_field(field));
            }
            if compile_scalar_projection_expr(E::MODEL, &input_expr).is_none() {
                return Err(SqlLoweringError::unsupported_aggregate_input_expressions());
            }

            Ok(TypedSqlGlobalAggregateTerminal::CountExpr {
                input_expr,
                distinct,
            })
        }
        SqlGlobalAggregateTerminal::SumField { field, distinct } => {
            Ok(TypedSqlGlobalAggregateTerminal::SumField {
                target_slot: resolve_target_slot(field.as_str())?,
                distinct,
            })
        }
        SqlGlobalAggregateTerminal::SumExpr {
            input_expr,
            distinct,
        } => {
            if let Some(field) = first_unknown_field_in_expr(&input_expr, E::MODEL) {
                return Err(SqlLoweringError::unknown_field(field));
            }
            if compile_scalar_projection_expr(E::MODEL, &input_expr).is_none() {
                return Err(SqlLoweringError::unsupported_aggregate_input_expressions());
            }

            Ok(TypedSqlGlobalAggregateTerminal::SumExpr {
                input_expr,
                distinct,
            })
        }
        SqlGlobalAggregateTerminal::AvgField { field, distinct } => {
            Ok(TypedSqlGlobalAggregateTerminal::AvgField {
                target_slot: resolve_target_slot(field.as_str())?,
                distinct,
            })
        }
        SqlGlobalAggregateTerminal::AvgExpr {
            input_expr,
            distinct,
        } => {
            if let Some(field) = first_unknown_field_in_expr(&input_expr, E::MODEL) {
                return Err(SqlLoweringError::unknown_field(field));
            }
            if compile_scalar_projection_expr(E::MODEL, &input_expr).is_none() {
                return Err(SqlLoweringError::unsupported_aggregate_input_expressions());
            }

            Ok(TypedSqlGlobalAggregateTerminal::AvgExpr {
                input_expr,
                distinct,
            })
        }
        SqlGlobalAggregateTerminal::MinField(field) => Ok(
            TypedSqlGlobalAggregateTerminal::MinField(resolve_target_slot(field.as_str())?),
        ),
        SqlGlobalAggregateTerminal::MinExpr { input_expr } => {
            if let Some(field) = first_unknown_field_in_expr(&input_expr, E::MODEL) {
                return Err(SqlLoweringError::unknown_field(field));
            }
            if compile_scalar_projection_expr(E::MODEL, &input_expr).is_none() {
                return Err(SqlLoweringError::unsupported_aggregate_input_expressions());
            }

            Ok(TypedSqlGlobalAggregateTerminal::MinExpr { input_expr })
        }
        SqlGlobalAggregateTerminal::MaxField(field) => Ok(
            TypedSqlGlobalAggregateTerminal::MaxField(resolve_target_slot(field.as_str())?),
        ),
        SqlGlobalAggregateTerminal::MaxExpr { input_expr } => {
            if let Some(field) = first_unknown_field_in_expr(&input_expr, E::MODEL) {
                return Err(SqlLoweringError::unknown_field(field));
            }
            if compile_scalar_projection_expr(E::MODEL, &input_expr).is_none() {
                return Err(SqlLoweringError::unsupported_aggregate_input_expressions());
            }

            Ok(TypedSqlGlobalAggregateTerminal::MaxExpr { input_expr })
        }
    }
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
) -> SqlGlobalAggregateCommandCore {
    lowered.into_structural(model, consistency)
}

fn lower_global_aggregate_terminal(
    item: SqlSelectItem,
) -> Result<SqlGlobalAggregateTerminal, SqlLoweringError> {
    let SqlSelectItem::Aggregate(aggregate) = item else {
        return Err(SqlLoweringError::unsupported_global_aggregate_projection());
    };

    match lower_sql_aggregate_shape(aggregate)? {
        LoweredSqlAggregateShape::CountRows => Ok(SqlGlobalAggregateTerminal::CountRows),
        LoweredSqlAggregateShape::CountField { field, distinct } => {
            Ok(SqlGlobalAggregateTerminal::CountField { field, distinct })
        }
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Sum,
            field,
            distinct,
        } => Ok(SqlGlobalAggregateTerminal::SumField { field, distinct }),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Avg,
            field,
            distinct,
        } => Ok(SqlGlobalAggregateTerminal::AvgField { field, distinct }),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Min,
            field,
            ..
        } => Ok(SqlGlobalAggregateTerminal::MinField(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Max,
            field,
            ..
        } => Ok(SqlGlobalAggregateTerminal::MaxField(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Count,
            ..
        } => Err(SqlLoweringError::unsupported_global_aggregate_projection()),
        LoweredSqlAggregateShape::ExpressionInput {
            kind: SqlAggregateKind::Count,
            input_expr,
            distinct,
        } => Ok(SqlGlobalAggregateTerminal::CountExpr {
            input_expr,
            distinct,
        }),
        LoweredSqlAggregateShape::ExpressionInput {
            kind: SqlAggregateKind::Sum,
            input_expr,
            distinct,
        } => Ok(SqlGlobalAggregateTerminal::SumExpr {
            input_expr,
            distinct,
        }),
        LoweredSqlAggregateShape::ExpressionInput {
            kind: SqlAggregateKind::Avg,
            input_expr,
            distinct,
        } => Ok(SqlGlobalAggregateTerminal::AvgExpr {
            input_expr,
            distinct,
        }),
        LoweredSqlAggregateShape::ExpressionInput {
            kind: SqlAggregateKind::Min,
            input_expr,
            ..
        } => Ok(SqlGlobalAggregateTerminal::MinExpr { input_expr }),
        LoweredSqlAggregateShape::ExpressionInput {
            kind: SqlAggregateKind::Max,
            input_expr,
            ..
        } => Ok(SqlGlobalAggregateTerminal::MaxExpr { input_expr }),
    }
}

///
/// LoweredSqlGlobalAggregateTerminals
///
/// Canonical global aggregate lowering result that keeps only unique
/// executable terminals plus one remap back to original SQL projection order.
///
struct LoweredSqlGlobalAggregateTerminals {
    terminals: Vec<SqlGlobalAggregateTerminal>,
    output_remap: Vec<usize>,
}

impl LoweredSqlGlobalAggregateTerminals {
    /// Lower one SQL projection into unique executable aggregate terminals plus
    /// the output remap needed to preserve original projection order.
    fn from_projection(projection: SqlProjection) -> Result<Self, SqlLoweringError> {
        let SqlProjection::Items(items) = projection else {
            return Err(SqlLoweringError::unsupported_global_aggregate_projection());
        };
        if items.is_empty() {
            return Err(SqlLoweringError::unsupported_global_aggregate_projection());
        }

        let mut terminals = Vec::<SqlGlobalAggregateTerminal>::with_capacity(items.len());
        let mut output_remap = Vec::<usize>::with_capacity(items.len());

        for item in items {
            let terminal = lower_global_aggregate_terminal(item)?;
            let unique_index = terminals
                .iter()
                .position(|current| current == &terminal)
                .unwrap_or_else(|| {
                    let index = terminals.len();
                    terminals.push(terminal);
                    index
                });
            output_remap.push(unique_index);
        }

        Ok(Self {
            terminals,
            output_remap,
        })
    }
}

fn lower_sql_aggregate_shape(
    call: SqlAggregateCall,
) -> Result<LoweredSqlAggregateShape, SqlLoweringError> {
    match (call.kind, call.input.map(|input| *input), call.distinct) {
        (SqlAggregateKind::Count, None, false) => Ok(LoweredSqlAggregateShape::CountRows),
        (SqlAggregateKind::Count, Some(SqlAggregateInputExpr::Field(field)), distinct) => {
            Ok(LoweredSqlAggregateShape::CountField { field, distinct })
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
            input_expr: canonicalize_sql_aggregate_input_expr(
                kind,
                lower_sql_aggregate_input_expr(input)?,
            ),
            distinct,
        }),
        _ => Err(SqlLoweringError::unsupported_select_projection()),
    }
}

// Normalize aggregate-input literal shape only where aggregate semantics
// already collapse numeric input types onto one decimal accumulator contract.
fn canonicalize_sql_aggregate_input_expr(kind: SqlAggregateKind, expr: Expr) -> Expr {
    match kind {
        SqlAggregateKind::Sum | SqlAggregateKind::Avg => {
            let Expr::Literal(value) = expr else {
                return expr;
            };

            value
                .to_numeric_decimal()
                .map_or(Expr::Literal(value), |decimal| {
                    Expr::Literal(Value::Decimal(decimal.normalize()))
                })
        }
        SqlAggregateKind::Count | SqlAggregateKind::Min | SqlAggregateKind::Max => expr,
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
        let expr = crate::db::sql::lowering::select::lower_select_item_expr(item)?;
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
            self.collect_item_aggregates(item);
        }

        Ok(())
    }

    // Gather aggregate leaves from one projection item while preserving the
    // original first-seen order used by grouped reducer slot assignment.
    fn collect_item_aggregates(&mut self, item: &SqlSelectItem) {
        match item {
            SqlSelectItem::Field(_) | SqlSelectItem::TextFunction(_) => {}
            SqlSelectItem::Aggregate(aggregate) => {
                self.push_unique_aggregate(aggregate.clone());
            }
            SqlSelectItem::Arithmetic(call) => {
                self.collect_operand_aggregates(&call.left);
                self.collect_operand_aggregates(&call.right);
            }
            SqlSelectItem::Round(call) => match &call.input {
                SqlRoundProjectionInput::Operand(operand) => {
                    self.collect_operand_aggregates(operand);
                }
                SqlRoundProjectionInput::Arithmetic(call) => {
                    self.collect_operand_aggregates(&call.left);
                    self.collect_operand_aggregates(&call.right);
                }
            },
        }
    }

    // Only aggregate operands contribute grouped reducer slots, so the operand
    // walk can stay intentionally narrow.
    fn collect_operand_aggregates(&mut self, operand: &SqlProjectionOperand) {
        if let SqlProjectionOperand::Aggregate(aggregate) = operand {
            self.push_unique_aggregate(aggregate.clone());
        }
    }

    // Keep grouped aggregate extraction on one stable first-seen unique
    // terminal order so repeated aggregate leaves reuse the same reducer slot.
    fn push_unique_aggregate(&mut self, aggregate: SqlAggregateCall) {
        if self
            .aggregate_calls
            .iter()
            .all(|current| current != &aggregate)
        {
            self.aggregate_calls.push(aggregate);
        }
    }
}

// Keep grouped aggregate extraction narrow: grouped projection expressions may
// include aggregate leaves, but field references must still stay inside the
// declared grouped-key authority.
fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_) => true,
        Expr::Field(_) | Expr::Literal(_) => false,
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_aggregate),
        Expr::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        #[cfg(test)]
        Expr::Unary { expr, .. } | Expr::Alias { expr, .. } => expr_contains_aggregate(expr),
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
        Expr::Binary { left, right, .. } => first_unknown_field_in_expr(left, model)
            .or_else(|| first_unknown_field_in_expr(right, model)),
        #[cfg(test)]
        Expr::Unary { expr, .. } | Expr::Alias { expr, .. } => {
            first_unknown_field_in_expr(expr, model)
        }
    }
}

pub(in crate::db::sql::lowering) fn lower_aggregate_call(
    call: SqlAggregateCall,
) -> Result<crate::db::query::builder::AggregateExpr, SqlLoweringError> {
    match lower_sql_aggregate_shape(call)? {
        LoweredSqlAggregateShape::CountRows => Ok(count()),
        LoweredSqlAggregateShape::CountField {
            field,
            distinct: false,
        } => Ok(count_by(field)),
        LoweredSqlAggregateShape::CountField {
            field,
            distinct: true,
        } => Ok(count_by(field).distinct()),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Sum,
            field,
            distinct: false,
        } => Ok(sum(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Sum,
            field,
            distinct: true,
        } => Ok(sum(field).distinct()),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Avg,
            field,
            distinct: false,
        } => Ok(avg(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Avg,
            field,
            distinct: true,
        } => Ok(avg(field).distinct()),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Min,
            field,
            ..
        } => Ok(min_by(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Max,
            field,
            ..
        } => Ok(max_by(field)),
        LoweredSqlAggregateShape::FieldTarget {
            kind: SqlAggregateKind::Count,
            ..
        } => Err(SqlLoweringError::unsupported_select_projection()),
        LoweredSqlAggregateShape::ExpressionInput {
            kind,
            input_expr,
            distinct,
        } => Ok(lower_expression_owned_aggregate_call(
            kind, input_expr, distinct,
        )),
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
    let lowered = match expr {
        SqlAggregateInputExpr::Field(field) => Expr::Field(FieldId::new(field)),
        SqlAggregateInputExpr::Literal(literal) => Expr::Literal(literal),
        SqlAggregateInputExpr::Arithmetic(call) => Expr::Binary {
            op: lower_sql_aggregate_binary_op(call.op),
            left: Box::new(lower_sql_aggregate_operand_expr(call.left)?),
            right: Box::new(lower_sql_aggregate_operand_expr(call.right)?),
        },
        SqlAggregateInputExpr::Round(call) => Expr::FunctionCall {
            function: Function::Round,
            args: lower_sql_aggregate_round_args(call)?,
        },
    };

    Ok(fold_sql_aggregate_input_constant_expr(lowered))
}

fn lower_sql_aggregate_round_args(
    call: crate::db::sql::parser::SqlRoundProjectionCall,
) -> Result<Vec<Expr>, SqlLoweringError> {
    let value_expr = match call.input {
        SqlRoundProjectionInput::Operand(operand) => lower_sql_aggregate_operand_expr(operand)?,
        SqlRoundProjectionInput::Arithmetic(call) => Expr::Binary {
            op: lower_sql_aggregate_binary_op(call.op),
            left: Box::new(lower_sql_aggregate_operand_expr(call.left)?),
            right: Box::new(lower_sql_aggregate_operand_expr(call.right)?),
        },
    };

    Ok(vec![value_expr, Expr::Literal(call.scale)])
}

fn lower_sql_aggregate_operand_expr(
    operand: SqlProjectionOperand,
) -> Result<Expr, SqlLoweringError> {
    match operand {
        SqlProjectionOperand::Field(field) => Ok(Expr::Field(FieldId::new(field))),
        SqlProjectionOperand::Literal(literal) => Ok(Expr::Literal(literal)),
        SqlProjectionOperand::Aggregate(_) => {
            Err(SqlLoweringError::unsupported_select_projection())
        }
    }
}

const fn lower_sql_aggregate_binary_op(
    op: crate::db::sql::parser::SqlArithmeticProjectionOp,
) -> BinaryOp {
    match op {
        crate::db::sql::parser::SqlArithmeticProjectionOp::Add => BinaryOp::Add,
        crate::db::sql::parser::SqlArithmeticProjectionOp::Sub => BinaryOp::Sub,
        crate::db::sql::parser::SqlArithmeticProjectionOp::Mul => BinaryOp::Mul,
        crate::db::sql::parser::SqlArithmeticProjectionOp::Div => BinaryOp::Div,
    }
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
        #[cfg(test)]
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
        BinaryOp::Add => NumericArithmeticOp::Add,
        BinaryOp::Sub => NumericArithmeticOp::Sub,
        BinaryOp::Mul => NumericArithmeticOp::Mul,
        BinaryOp::Div => NumericArithmeticOp::Div,
        #[cfg(test)]
        BinaryOp::And | BinaryOp::Eq => return None,
    };
    let result = apply_numeric_arithmetic(arithmetic_op, left, right)?;

    Some(Expr::Literal(Value::Decimal(result)))
}

// Fold one literal-only aggregate-input function call when the admitted
// aggregate-input family defines a deterministic literal result.
fn fold_sql_aggregate_input_constant_function(function: Function, args: &[Expr]) -> Option<Expr> {
    match function {
        Function::Round => fold_sql_aggregate_input_round(args),
        Function::Trim
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

pub(in crate::db::sql::lowering) fn resolve_having_aggregate_index(
    target: &SqlAggregateCall,
    grouped_projection_aggregates: &[SqlAggregateCall],
) -> Result<usize, SqlLoweringError> {
    let mut matched = grouped_projection_aggregates
        .iter()
        .enumerate()
        .filter_map(|(index, aggregate)| (aggregate == target).then_some(index));
    let Some(index) = matched.next() else {
        return Err(SqlLoweringError::unsupported_select_having());
    };
    if matched.next().is_some() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    Ok(index)
}
