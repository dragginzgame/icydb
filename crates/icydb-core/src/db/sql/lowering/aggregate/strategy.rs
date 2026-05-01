use crate::{
    db::{
        query::plan::{AggregateKind, FieldSlot, expr::Expr, resolve_aggregate_target_field_slot},
        sql::lowering::{
            SqlLoweringError,
            aggregate::{
                lowering::validate_model_bound_scalar_expr,
                semantics::{
                    AggregateTerminalSemantics, PreparedAggregateSemantics,
                    PreparedAggregateTarget, aggregate_input_from_semantics,
                },
                terminal::{AggregateInput, SqlGlobalAggregateTerminal},
            },
        },
    },
    model::entity::EntityModel,
};

///
/// PreparedSqlScalarAggregatePlanFragment
///
/// Stable query-facing shape fragment for one prepared typed SQL scalar
/// aggregate strategy.
/// Session SQL aggregate execution consumes this fragment instead of
/// rebuilding aggregate shape choice from raw SQL terminal variants or
/// parallel metadata tuple matches.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedSqlScalarAggregatePlanFragment {
    CountRows,
    CountField,
    NumericField { kind: AggregateKind },
    ExtremalWinnerField { kind: AggregateKind },
}

pub(crate) type PreparedSqlScalarAggregateDescriptorShape = PreparedSqlScalarAggregatePlanFragment;

impl PreparedSqlScalarAggregatePlanFragment {
    /// Return the stable query-facing plan fragment for this descriptor shape.
    #[must_use]
    pub(crate) const fn plan_fragment(self) -> Self {
        self
    }
}

///
/// PreparedSqlScalarAggregateStrategy
///
/// PreparedSqlScalarAggregateStrategy is the single typed SQL scalar aggregate
/// binding boundary after SQL aggregate semantics have been normalized.
/// It resolves descriptor shape and target-slot ownership once so session
/// execution and EXPLAIN do not re-derive that shape from raw SQL
/// terminal variants.
/// Explain-visible aggregate expressions are projected on demand from this
/// prepared strategy instead of being carried as owned metadata.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedSqlScalarAggregateStrategy {
    semantics: PreparedAggregateSemantics,
    filter_expr: Option<Expr>,
}

impl PreparedSqlScalarAggregateStrategy {
    // Build one prepared aggregate strategy from normalized aggregate semantics.
    // Strategy stores no raw DISTINCT flag, so invalid extrema DISTINCT
    // combinations cannot be represented after semantic preparation.
    const fn from_semantics(
        semantics: PreparedAggregateSemantics,
        filter_expr: Option<Expr>,
    ) -> Self {
        Self {
            semantics,
            filter_expr,
        }
    }

    // Keep terminal preparation on one owner-local seam so field-target and
    // expression-input aggregate shapes cannot drift apart across parallel
    // helpers.
    pub(in crate::db::sql::lowering::aggregate) fn from_lowered_terminal(
        model: &'static EntityModel,
        terminal: SqlGlobalAggregateTerminal,
    ) -> Result<Self, SqlLoweringError> {
        let (semantic_identity, filter_expr) =
            AggregateTerminalSemantics::from_owned_terminal(terminal).into_parts();
        let kind = semantic_identity.kind();
        let distinct_input = semantic_identity.distinct();
        let target = match aggregate_input_from_semantics(semantic_identity) {
            AggregateInput::Rows => PreparedAggregateTarget::Rows,
            AggregateInput::Field(field) => {
                let target_slot = resolve_aggregate_target_field_slot(model, field.as_str())
                    .map_err(SqlLoweringError::from)?;
                PreparedAggregateTarget::Field(target_slot)
            }
            AggregateInput::Expr(input_expr) => {
                validate_model_bound_scalar_expr(
                    model,
                    &input_expr,
                    SqlLoweringError::unsupported_aggregate_input_expressions,
                )?;
                PreparedAggregateTarget::Expr(input_expr)
            }
        };

        Ok(Self::from_semantics(
            PreparedAggregateSemantics::from_parts(kind, target, distinct_input),
            filter_expr,
        ))
    }

    /// Borrow the resolved target slot when this prepared SQL scalar strategy is field-targeted.
    #[must_use]
    pub(crate) const fn target_slot(&self) -> Option<&FieldSlot> {
        self.semantics.target_slot()
    }

    /// Borrow the aggregate input expression when this prepared SQL scalar strategy is expression-backed.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn input_expr(&self) -> Option<&Expr> {
        self.semantics.input_expr()
    }

    /// Borrow the aggregate filter expression when this prepared SQL scalar strategy is filtered.
    #[must_use]
    pub(in crate::db) const fn filter_expr(&self) -> Option<&Expr> {
        self.filter_expr.as_ref()
    }

    /// Return whether this prepared SQL scalar aggregate deduplicates field inputs.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn is_distinct(&self) -> bool {
        self.semantics.distinct_input()
    }

    /// Return the stable descriptor shape label for this prepared strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn descriptor_shape(&self) -> PreparedSqlScalarAggregateDescriptorShape {
        self.prepared_descriptor_shape()
    }

    /// Return the stable query-facing plan fragment for this prepared SQL
    /// scalar aggregate strategy.
    #[must_use]
    pub(crate) const fn plan_fragment(&self) -> PreparedSqlScalarAggregatePlanFragment {
        self.prepared_descriptor_shape().plan_fragment()
    }

    /// Return the canonical aggregate kind for this prepared SQL scalar strategy.
    #[must_use]
    pub(crate) const fn aggregate_kind(&self) -> AggregateKind {
        self.semantics.aggregate_kind()
    }

    // Project the aggregate semantics shape onto the compact plan fragment
    // consumed at the SQL session boundary.
    const fn prepared_descriptor_shape(&self) -> PreparedSqlScalarAggregateDescriptorShape {
        match &self.semantics {
            PreparedAggregateSemantics::Count {
                target: PreparedAggregateTarget::Rows,
                ..
            } => PreparedSqlScalarAggregateDescriptorShape::CountRows,
            PreparedAggregateSemantics::Count { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::CountField
            }
            PreparedAggregateSemantics::Sum { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::NumericField {
                    kind: AggregateKind::Sum,
                }
            }
            PreparedAggregateSemantics::Avg { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::NumericField {
                    kind: AggregateKind::Avg,
                }
            }
            PreparedAggregateSemantics::Min { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::ExtremalWinnerField {
                    kind: AggregateKind::Min,
                }
            }
            PreparedAggregateSemantics::Max { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::ExtremalWinnerField {
                    kind: AggregateKind::Max,
                }
            }
        }
    }

    /// Split the prepared strategy into executor-neutral aggregate plan parts.
    ///
    /// SQL lowering owns the semantic projection into this compact plan
    /// fragment, but it does not construct executor terminal DTOs. Session
    /// SQL execution performs that final adaptation at the query -> executor
    /// boundary.
    pub(in crate::db) fn into_aggregate_plan_parts(
        self,
    ) -> (
        PreparedSqlScalarAggregatePlanFragment,
        Option<FieldSlot>,
        Option<Expr>,
        Option<Expr>,
        bool,
    ) {
        let descriptor = self.plan_fragment();
        let Self {
            semantics,
            filter_expr,
        } = self;
        let distinct_input = semantics.distinct_input();
        let (target_slot, input_expr) = semantics.into_executor_parts();

        (
            descriptor,
            target_slot,
            input_expr,
            filter_expr,
            distinct_input,
        )
    }

    /// Return the projected field label for descriptor/explain projection when
    /// this prepared strategy is field-targeted.
    #[must_use]
    pub(crate) fn projected_field(&self) -> Option<&str> {
        self.target_slot().map(FieldSlot::field)
    }
}
