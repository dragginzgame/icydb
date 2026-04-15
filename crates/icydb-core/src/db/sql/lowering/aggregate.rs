use crate::db::sql::lowering::{
    LoweredBaseQueryShape, LoweredSqlCommand, LoweredSqlCommandInner, PreparedSqlStatement,
    SqlLoweringError,
};
#[cfg(test)]
use crate::{db::query::intent::Query, traits::EntityKind};
use crate::{
    db::{
        predicate::MissingRowPolicy,
        query::{
            builder::aggregate::{avg, count, count_by, max_by, min_by, sum},
            intent::StructuralQuery,
            plan::{
                AggregateKind, FieldSlot,
                expr::{Expr, expr_references_only_fields},
                resolve_aggregate_target_field_slot,
            },
        },
        sql::parser::{
            SqlAggregateCall, SqlAggregateKind, SqlExplainMode, SqlProjection, SqlSelectItem,
            SqlSelectStatement, SqlStatement,
        },
    },
    model::entity::EntityModel,
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
    SumField { field: String, distinct: bool },
    AvgField { field: String, distinct: bool },
    MinField(String),
    MaxField(String),
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
    SumField {
        target_slot: FieldSlot,
        distinct: bool,
    },
    AvgField {
        target_slot: FieldSlot,
        distinct: bool,
    },
    MinField(FieldSlot),
    MaxField(FieldSlot),
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
    distinct_input: bool,
    domain: PreparedSqlScalarAggregateDomain,
    ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement,
    row_source: PreparedSqlScalarAggregateRowSource,
    empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior,
    descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
}

impl PreparedSqlScalarAggregateStrategy {
    const fn new(
        target_slot: Option<FieldSlot>,
        distinct_input: bool,
        domain: PreparedSqlScalarAggregateDomain,
        ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement,
        row_source: PreparedSqlScalarAggregateRowSource,
        empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior,
        descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
    ) -> Self {
        Self {
            target_slot,
            distinct_input,
            domain,
            ordering_requirement,
            row_source,
            empty_set_behavior,
            descriptor_shape,
        }
    }

    #[cfg(test)]
    pub(in crate::db::sql::lowering) fn from_typed_terminal(
        terminal: &TypedSqlGlobalAggregateTerminal,
    ) -> Self {
        match terminal {
            TypedSqlGlobalAggregateTerminal::CountRows => Self::new(
                None,
                false,
                PreparedSqlScalarAggregateDomain::ExistingRows,
                PreparedSqlScalarAggregateOrderingRequirement::None,
                PreparedSqlScalarAggregateRowSource::ExistingRows,
                PreparedSqlScalarAggregateEmptySetBehavior::Zero,
                PreparedSqlScalarAggregateDescriptorShape::CountRows,
            ),
            TypedSqlGlobalAggregateTerminal::CountField {
                target_slot,
                distinct,
            } => Self::new(
                Some(target_slot.clone()),
                *distinct,
                PreparedSqlScalarAggregateDomain::ProjectionField,
                PreparedSqlScalarAggregateOrderingRequirement::None,
                PreparedSqlScalarAggregateRowSource::ProjectedField,
                PreparedSqlScalarAggregateEmptySetBehavior::Zero,
                PreparedSqlScalarAggregateDescriptorShape::CountField,
            ),
            TypedSqlGlobalAggregateTerminal::SumField {
                target_slot,
                distinct,
            } => Self::new(
                Some(target_slot.clone()),
                *distinct,
                PreparedSqlScalarAggregateDomain::NumericField,
                PreparedSqlScalarAggregateOrderingRequirement::None,
                PreparedSqlScalarAggregateRowSource::NumericField,
                PreparedSqlScalarAggregateEmptySetBehavior::Null,
                PreparedSqlScalarAggregateDescriptorShape::SumField,
            ),
            TypedSqlGlobalAggregateTerminal::AvgField {
                target_slot,
                distinct,
            } => Self::new(
                Some(target_slot.clone()),
                *distinct,
                PreparedSqlScalarAggregateDomain::NumericField,
                PreparedSqlScalarAggregateOrderingRequirement::None,
                PreparedSqlScalarAggregateRowSource::NumericField,
                PreparedSqlScalarAggregateEmptySetBehavior::Null,
                PreparedSqlScalarAggregateDescriptorShape::AvgField,
            ),
            TypedSqlGlobalAggregateTerminal::MinField(target_slot) => Self::new(
                Some(target_slot.clone()),
                false,
                PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
                PreparedSqlScalarAggregateOrderingRequirement::FieldOrder,
                PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
                PreparedSqlScalarAggregateEmptySetBehavior::Null,
                PreparedSqlScalarAggregateDescriptorShape::MinField,
            ),
            TypedSqlGlobalAggregateTerminal::MaxField(target_slot) => Self::new(
                Some(target_slot.clone()),
                false,
                PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
                PreparedSqlScalarAggregateOrderingRequirement::FieldOrder,
                PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
                PreparedSqlScalarAggregateEmptySetBehavior::Null,
                PreparedSqlScalarAggregateDescriptorShape::MaxField,
            ),
        }
    }

    fn from_lowered_terminal_with_model(
        model: &'static EntityModel,
        terminal: &SqlGlobalAggregateTerminal,
    ) -> Result<Self, SqlLoweringError> {
        let resolve_target_slot = |field: &str| {
            resolve_aggregate_target_field_slot(model, field).map_err(SqlLoweringError::from)
        };

        match terminal {
            SqlGlobalAggregateTerminal::CountRows => Ok(Self::new(
                None,
                false,
                PreparedSqlScalarAggregateDomain::ExistingRows,
                PreparedSqlScalarAggregateOrderingRequirement::None,
                PreparedSqlScalarAggregateRowSource::ExistingRows,
                PreparedSqlScalarAggregateEmptySetBehavior::Zero,
                PreparedSqlScalarAggregateDescriptorShape::CountRows,
            )),
            SqlGlobalAggregateTerminal::CountField { field, distinct } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::new(
                    Some(target_slot),
                    *distinct,
                    PreparedSqlScalarAggregateDomain::ProjectionField,
                    PreparedSqlScalarAggregateOrderingRequirement::None,
                    PreparedSqlScalarAggregateRowSource::ProjectedField,
                    PreparedSqlScalarAggregateEmptySetBehavior::Zero,
                    PreparedSqlScalarAggregateDescriptorShape::CountField,
                ))
            }
            SqlGlobalAggregateTerminal::SumField { field, distinct } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::new(
                    Some(target_slot),
                    *distinct,
                    PreparedSqlScalarAggregateDomain::NumericField,
                    PreparedSqlScalarAggregateOrderingRequirement::None,
                    PreparedSqlScalarAggregateRowSource::NumericField,
                    PreparedSqlScalarAggregateEmptySetBehavior::Null,
                    PreparedSqlScalarAggregateDescriptorShape::SumField,
                ))
            }
            SqlGlobalAggregateTerminal::AvgField { field, distinct } => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::new(
                    Some(target_slot),
                    *distinct,
                    PreparedSqlScalarAggregateDomain::NumericField,
                    PreparedSqlScalarAggregateOrderingRequirement::None,
                    PreparedSqlScalarAggregateRowSource::NumericField,
                    PreparedSqlScalarAggregateEmptySetBehavior::Null,
                    PreparedSqlScalarAggregateDescriptorShape::AvgField,
                ))
            }
            SqlGlobalAggregateTerminal::MinField(field) => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::new(
                    Some(target_slot),
                    false,
                    PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
                    PreparedSqlScalarAggregateOrderingRequirement::FieldOrder,
                    PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
                    PreparedSqlScalarAggregateEmptySetBehavior::Null,
                    PreparedSqlScalarAggregateDescriptorShape::MinField,
                ))
            }
            SqlGlobalAggregateTerminal::MaxField(field) => {
                let target_slot = resolve_target_slot(field.as_str())?;

                Ok(Self::new(
                    Some(target_slot),
                    false,
                    PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
                    PreparedSqlScalarAggregateOrderingRequirement::FieldOrder,
                    PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
                    PreparedSqlScalarAggregateEmptySetBehavior::Null,
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

    /// Prepare the first typed SQL scalar aggregate strategy for legacy single-terminal callers.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn prepared_scalar_strategy(&self) -> PreparedSqlScalarAggregateStrategy {
        PreparedSqlScalarAggregateStrategy::from_typed_terminal(self.terminal())
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
    pub(in crate::db) fn prepared_scalar_strategies_with_model(
        &self,
        model: &'static EntityModel,
    ) -> Result<Vec<PreparedSqlScalarAggregateStrategy>, SqlLoweringError> {
        self.terminals
            .iter()
            .map(|terminal| {
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal_with_model(
                    model, terminal,
                )
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

    lower_global_aggregate_terminals(statement.projection.clone()).is_ok()
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
        SqlGlobalAggregateTerminal::SumField { field, distinct } => {
            Ok(TypedSqlGlobalAggregateTerminal::SumField {
                target_slot: resolve_target_slot(field.as_str())?,
                distinct,
            })
        }
        SqlGlobalAggregateTerminal::AvgField { field, distinct } => {
            Ok(TypedSqlGlobalAggregateTerminal::AvgField {
                target_slot: resolve_target_slot(field.as_str())?,
                distinct,
            })
        }
        SqlGlobalAggregateTerminal::MinField(field) => Ok(
            TypedSqlGlobalAggregateTerminal::MinField(resolve_target_slot(field.as_str())?),
        ),
        SqlGlobalAggregateTerminal::MaxField(field) => Ok(
            TypedSqlGlobalAggregateTerminal::MaxField(resolve_target_slot(field.as_str())?),
        ),
    }
}

#[cfg(test)]
fn bind_lowered_sql_global_aggregate_terminals<E: EntityKind>(
    terminals: Vec<SqlGlobalAggregateTerminal>,
) -> Result<Vec<TypedSqlGlobalAggregateTerminal>, SqlLoweringError> {
    terminals
        .into_iter()
        .map(bind_lowered_sql_global_aggregate_terminal::<E>)
        .collect()
}

pub(in crate::db::sql::lowering) fn lower_global_aggregate_select_shape(
    statement: SqlSelectStatement,
) -> Result<LoweredSqlGlobalAggregateCommand, SqlLoweringError> {
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
        return Err(SqlLoweringError::unsupported_select_group_by());
    }
    if !having.is_empty() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    let lowered_terminals = lower_global_aggregate_terminals(projection)?;

    Ok(LoweredSqlGlobalAggregateCommand {
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

#[cfg(test)]
pub(in crate::db::sql::lowering) fn bind_lowered_sql_global_aggregate_command<E: EntityKind>(
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let LoweredSqlGlobalAggregateCommand {
        query,
        terminals,
        output_remap,
    } = lowered;
    let terminals = bind_lowered_sql_global_aggregate_terminals::<E>(terminals)?;

    Ok(SqlGlobalAggregateCommand {
        query: Query::from_inner(crate::db::sql::lowering::apply_lowered_base_query_shape(
            StructuralQuery::new(E::MODEL, consistency),
            query,
        )),
        terminals,
        output_remap,
    })
}

fn bind_lowered_sql_global_aggregate_command_structural(
    model: &'static EntityModel,
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> SqlGlobalAggregateCommandCore {
    let LoweredSqlGlobalAggregateCommand {
        query,
        terminals,
        output_remap,
    } = lowered;

    SqlGlobalAggregateCommandCore {
        query: crate::db::sql::lowering::apply_lowered_base_query_shape(
            StructuralQuery::new(model, consistency),
            query,
        ),
        terminals,
        output_remap,
    }
}

fn lower_global_aggregate_terminal(
    item: SqlSelectItem,
) -> Result<SqlGlobalAggregateTerminal, SqlLoweringError> {
    let SqlSelectItem::Aggregate(aggregate) = item else {
        return Err(SqlLoweringError::unsupported_select_projection());
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
        } => Err(SqlLoweringError::unsupported_select_projection()),
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

fn lower_global_aggregate_terminals(
    projection: SqlProjection,
) -> Result<LoweredSqlGlobalAggregateTerminals, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };
    if items.is_empty() {
        return Err(SqlLoweringError::unsupported_select_projection());
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

    Ok(LoweredSqlGlobalAggregateTerminals {
        terminals,
        output_remap,
    })
}

fn lower_sql_aggregate_shape(
    call: SqlAggregateCall,
) -> Result<LoweredSqlAggregateShape, SqlLoweringError> {
    match (call.kind, call.field, call.distinct) {
        (SqlAggregateKind::Count, None, false) => Ok(LoweredSqlAggregateShape::CountRows),
        (SqlAggregateKind::Count, Some(field), distinct) => {
            Ok(LoweredSqlAggregateShape::CountField { field, distinct })
        }
        (
            kind @ (SqlAggregateKind::Sum
            | SqlAggregateKind::Avg
            | SqlAggregateKind::Min
            | SqlAggregateKind::Max),
            Some(field),
            distinct,
        ) => Ok(LoweredSqlAggregateShape::FieldTarget {
            kind,
            field,
            distinct,
        }),
        _ => Err(SqlLoweringError::unsupported_select_projection()),
    }
}

pub(in crate::db::sql::lowering) fn grouped_projection_aggregate_calls(
    projection: &SqlProjection,
    group_by_fields: &[String],
) -> Result<Vec<SqlAggregateCall>, SqlLoweringError> {
    if group_by_fields.is_empty() {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::unsupported_select_group_by());
    };

    let grouped_field_names = group_by_fields
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut aggregate_calls = Vec::<SqlAggregateCall>::new();
    let mut seen_aggregate = false;

    for item in items {
        match item {
            SqlSelectItem::Aggregate(aggregate) => {
                seen_aggregate = true;
                aggregate_calls.push(aggregate.clone());
            }
            SqlSelectItem::Field(_)
            | SqlSelectItem::TextFunction(_)
            | SqlSelectItem::Arithmetic(_)
            | SqlSelectItem::Round(_) => {
                if seen_aggregate {
                    return Err(SqlLoweringError::unsupported_select_group_by());
                }
                let expr = crate::db::sql::lowering::select::lower_select_item_expr(item)?;
                if expr_contains_aggregate(&expr)
                    || !expr_references_only_fields(&expr, grouped_field_names.as_slice())
                {
                    return Err(SqlLoweringError::unsupported_select_group_by());
                }
            }
        }
    }

    if aggregate_calls.is_empty() {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    Ok(aggregate_calls)
}

// Keep grouped aggregate extraction narrow: non-aggregate projection items may
// reference grouped fields, but aggregate leaves still belong only to the
// explicit grouped aggregate list.
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
    }
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
