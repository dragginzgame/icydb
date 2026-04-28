use crate::{
    db::{
        executor::{StructuralAggregateTerminal, StructuralAggregateTerminalKind},
        query::plan::{AggregateKind, FieldSlot, expr::Expr, resolve_aggregate_target_field_slot},
        sql::lowering::{
            SqlLoweringError,
            aggregate::{
                identity::{
                    PreparedAggregateIdentity, PreparedAggregateTarget, PreparedAggregateTerminal,
                    aggregate_input_from_identity,
                },
                lowering::validate_model_bound_scalar_expr,
                terminal::{AggregateInput, SqlGlobalAggregateTerminal},
            },
        },
    },
    model::entity::EntityModel,
};

///
/// PreparedSqlScalarAggregateRuntimeDescriptor
///
/// Stable runtime-family projection for one prepared typed SQL scalar
/// aggregate strategy.
/// Session SQL aggregate execution consumes this descriptor instead of
/// rebuilding runtime boundary choice from raw SQL terminal variants or
/// parallel metadata tuple matches.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedSqlScalarAggregateRuntimeDescriptor {
    CountRows,
    CountField,
    NumericField { kind: AggregateKind },
    ExtremalWinnerField { kind: AggregateKind },
}

pub(crate) type PreparedSqlScalarAggregateDescriptorShape =
    PreparedSqlScalarAggregateRuntimeDescriptor;

impl PreparedSqlScalarAggregateRuntimeDescriptor {
    /// Return the stable runtime-family projection for this descriptor shape.
    #[must_use]
    pub(crate) const fn runtime_descriptor(self) -> Self {
        self
    }
}

///
/// PreparedSqlScalarAggregateStrategy
///
/// PreparedSqlScalarAggregateStrategy is the single typed SQL scalar aggregate
/// execution boundary after SQL aggregate identity has been normalized.
/// It resolves descriptor shape, target-slot ownership, and runtime behavior
/// once so runtime and EXPLAIN do not re-derive that behavior from raw SQL
/// terminal variants.
/// Explain-visible aggregate expressions are projected on demand from this
/// prepared strategy instead of being carried as owned execution metadata.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedSqlScalarAggregateStrategy {
    identity: PreparedAggregateIdentity,
    filter_expr: Option<Expr>,
}

impl PreparedSqlScalarAggregateStrategy {
    // Build one prepared aggregate strategy from normalized aggregate identity.
    // Strategy stores no raw DISTINCT flag, so invalid extrema DISTINCT
    // combinations cannot be represented after identity preparation.
    const fn from_identity(identity: PreparedAggregateIdentity, filter_expr: Option<Expr>) -> Self {
        Self {
            identity,
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
        let (identity, filter_expr) =
            PreparedAggregateTerminal::from_terminal(terminal).into_parts();
        let kind = identity.kind();
        let distinct_input = identity.distinct();
        let target = match aggregate_input_from_identity(identity) {
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

        Ok(Self::from_identity(
            PreparedAggregateIdentity::from_parts(kind, target, distinct_input),
            filter_expr,
        ))
    }

    /// Borrow the resolved target slot when this prepared SQL scalar strategy is field-targeted.
    #[must_use]
    pub(crate) const fn target_slot(&self) -> Option<&FieldSlot> {
        self.identity.target_slot()
    }

    /// Borrow the aggregate input expression when this prepared SQL scalar strategy is expression-backed.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn input_expr(&self) -> Option<&Expr> {
        self.identity.input_expr()
    }

    /// Borrow the aggregate filter expression when this prepared SQL scalar strategy is filtered.
    #[must_use]
    pub(crate) const fn filter_expr(&self) -> Option<&Expr> {
        self.filter_expr.as_ref()
    }

    /// Return whether this prepared SQL scalar aggregate deduplicates field inputs.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn is_distinct(&self) -> bool {
        self.identity.distinct_input()
    }

    /// Return the stable descriptor/runtime shape label for this prepared strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn descriptor_shape(&self) -> PreparedSqlScalarAggregateDescriptorShape {
        self.prepared_descriptor_shape()
    }

    /// Return the stable runtime-family projection for this prepared SQL
    /// scalar aggregate strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn runtime_descriptor(&self) -> PreparedSqlScalarAggregateRuntimeDescriptor {
        self.descriptor_shape().runtime_descriptor()
    }

    /// Return the canonical aggregate kind for this prepared SQL scalar strategy.
    #[must_use]
    pub(crate) const fn aggregate_kind(&self) -> AggregateKind {
        self.identity.aggregate_kind()
    }

    // Project the aggregate identity shape onto the executor descriptor family.
    const fn prepared_descriptor_shape(&self) -> PreparedSqlScalarAggregateDescriptorShape {
        match &self.identity {
            PreparedAggregateIdentity::Count {
                target: PreparedAggregateTarget::Rows,
                ..
            } => PreparedSqlScalarAggregateDescriptorShape::CountRows,
            PreparedAggregateIdentity::Count { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::CountField
            }
            PreparedAggregateIdentity::Sum { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::NumericField {
                    kind: AggregateKind::Sum,
                }
            }
            PreparedAggregateIdentity::Avg { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::NumericField {
                    kind: AggregateKind::Avg,
                }
            }
            PreparedAggregateIdentity::Min { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::ExtremalWinnerField {
                    kind: AggregateKind::Min,
                }
            }
            PreparedAggregateIdentity::Max { .. } => {
                PreparedSqlScalarAggregateDescriptorShape::ExtremalWinnerField {
                    kind: AggregateKind::Max,
                }
            }
        }
    }

    /// Build the executor terminal consumed by structural aggregate execution.
    ///
    /// SQL lowering owns the aggregate strategy and therefore owns the final
    /// executor terminal construction. Session execution keeps only SQL result
    /// labels, fixed scales, cache attribution, and statement shaping.
    pub(in crate::db) fn into_executor_terminal(
        self,
    ) -> Result<StructuralAggregateTerminal, &'static str> {
        let descriptor_shape = self.prepared_descriptor_shape();
        let Self {
            identity,
            filter_expr,
        } = self;
        let distinct_input = identity.distinct_input();
        let (target_slot, input_expr) = identity.into_executor_parts();

        let kind = match descriptor_shape.runtime_descriptor() {
            PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => {
                StructuralAggregateTerminalKind::CountRows
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::CountField => {
                StructuralAggregateTerminalKind::CountValues
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: AggregateKind::Sum,
            } => StructuralAggregateTerminalKind::Sum,
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: AggregateKind::Avg,
            } => StructuralAggregateTerminalKind::Avg,
            PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind: AggregateKind::Min,
            } => StructuralAggregateTerminalKind::Min,
            PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind: AggregateKind::Max,
            } => StructuralAggregateTerminalKind::Max,
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
            | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
                return Err("prepared SQL scalar aggregate strategy drifted outside SQL support");
            }
        };

        Ok(StructuralAggregateTerminal::new(
            kind,
            target_slot,
            input_expr,
            filter_expr,
            distinct_input,
        ))
    }

    /// Return the projected field label for descriptor/explain projection when
    /// this prepared strategy is field-targeted.
    #[must_use]
    pub(crate) fn projected_field(&self) -> Option<&str> {
        self.target_slot().map(FieldSlot::field)
    }
}
