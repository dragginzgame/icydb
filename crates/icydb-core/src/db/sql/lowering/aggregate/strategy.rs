use crate::{
    db::{
        executor::{StructuralAggregateTerminal, StructuralAggregateTerminalKind},
        query::plan::{AggregateKind, FieldSlot, expr::Expr, resolve_aggregate_target_field_slot},
        sql::lowering::{
            SqlLoweringError,
            aggregate::{
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

///
/// AggregateTarget
///
/// AggregateTarget seals the mutually exclusive row, field, and expression
/// input shapes carried by a prepared SQL scalar aggregate strategy. The
/// strategy uses this private enum so invalid combinations cannot be
/// represented by independent optional target fields.
///
#[derive(Clone, Debug, Eq, PartialEq)]
enum AggregateTarget {
    Rows,
    Field(FieldSlot),
    Expr(Expr),
}

impl AggregateTarget {
    // Borrow the field slot when this target is field-backed. Keeping this
    // helper on the enum makes the old strategy accessor a thin compatibility
    // shim over the sealed representation.
    const fn field_slot(&self) -> Option<&FieldSlot> {
        match self {
            Self::Field(field_slot) => Some(field_slot),
            Self::Rows | Self::Expr(_) => None,
        }
    }

    // Borrow the scalar input expression when this target is expression-backed.
    // This keeps expression-owned aggregate behavior observable to tests and
    // label construction without reopening the target-shape invariant.
    #[cfg(test)]
    const fn input_expr(&self) -> Option<&Expr> {
        match self {
            Self::Expr(input_expr) => Some(input_expr),
            Self::Rows | Self::Field(_) => None,
        }
    }

    // Convert the sealed target into the legacy executor terminal tuple. This
    // is the only strategy-local point that expands the enum back into the
    // executor's current field/expression option pair.
    fn into_executor_parts(self) -> (Option<FieldSlot>, Option<Expr>) {
        match self {
            Self::Rows => (None, None),
            Self::Field(target_slot) => (Some(target_slot), None),
            Self::Expr(input_expr) => (None, Some(input_expr)),
        }
    }
}

impl PreparedSqlScalarAggregateRuntimeDescriptor {
    // Map supported SQL aggregate kinds onto the field/expression descriptor
    // family that will own runtime dispatch and EXPLAIN labeling.
    fn from_aggregate_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::CountField,
            AggregateKind::Sum | AggregateKind::Avg => Self::NumericField { kind },
            AggregateKind::Min | AggregateKind::Max => Self::ExtremalWinnerField { kind },
            AggregateKind::Exists | AggregateKind::First | AggregateKind::Last => {
                unreachable!("unsupported SQL aggregate kind reached scalar aggregate descriptor")
            }
        }
    }

    /// Return the stable runtime-family projection for this descriptor shape.
    #[must_use]
    pub(crate) const fn runtime_descriptor(self) -> Self {
        self
    }

    /// Return the canonical aggregate kind for this descriptor shape.
    #[must_use]
    pub(crate) const fn aggregate_kind(self) -> AggregateKind {
        match self {
            Self::CountRows | Self::CountField => AggregateKind::Count,
            Self::NumericField { kind } | Self::ExtremalWinnerField { kind } => kind,
        }
    }
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
    target: AggregateTarget,
    filter_expr: Option<Expr>,
    distinct_input: bool,
    descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
}

impl PreparedSqlScalarAggregateStrategy {
    // Build one prepared aggregate strategy from the already-resolved target
    // slot and descriptor shape so higher entrypoints only own target
    // resolution, not the descriptor policy bundle.
    const fn from_resolved_shape(
        target: AggregateTarget,
        filter_expr: Option<Expr>,
        distinct_input: bool,
        descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
    ) -> Self {
        Self {
            target,
            filter_expr,
            distinct_input,
            descriptor_shape,
        }
    }

    const fn for_rows(filter_expr: Option<Expr>) -> Self {
        Self::from_resolved_shape(
            AggregateTarget::Rows,
            filter_expr,
            false,
            PreparedSqlScalarAggregateDescriptorShape::CountRows,
        )
    }

    fn for_field_target(
        kind: AggregateKind,
        target_slot: FieldSlot,
        filter_expr: Option<Expr>,
        distinct_input: bool,
    ) -> Self {
        Self::from_resolved_shape(
            AggregateTarget::Field(target_slot),
            filter_expr,
            distinct_input,
            PreparedSqlScalarAggregateDescriptorShape::from_aggregate_kind(kind),
        )
    }

    fn for_expression_input(
        kind: AggregateKind,
        input_expr: Expr,
        filter_expr: Option<Expr>,
        distinct_input: bool,
    ) -> Self {
        Self::from_resolved_shape(
            AggregateTarget::Expr(input_expr),
            filter_expr,
            distinct_input,
            PreparedSqlScalarAggregateDescriptorShape::from_aggregate_kind(kind),
        )
    }

    // Keep terminal preparation on one owner-local seam so field-target and
    // expression-input aggregate shapes cannot drift apart across parallel
    // helpers.
    pub(in crate::db::sql::lowering::aggregate) fn from_lowered_terminal(
        model: &'static EntityModel,
        terminal: SqlGlobalAggregateTerminal,
    ) -> Result<Self, SqlLoweringError> {
        match terminal.input {
            AggregateInput::Rows => Ok(Self::for_rows(terminal.filter_expr)),
            AggregateInput::Field(field) => {
                let target_slot = resolve_aggregate_target_field_slot(model, field.as_str())
                    .map_err(SqlLoweringError::from)?;
                Ok(Self::for_field_target(
                    terminal.kind,
                    target_slot,
                    terminal.filter_expr,
                    terminal.distinct,
                ))
            }
            AggregateInput::Expr(input_expr) => {
                validate_model_bound_scalar_expr(
                    model,
                    &input_expr,
                    SqlLoweringError::unsupported_aggregate_input_expressions,
                )?;
                Ok(Self::for_expression_input(
                    terminal.kind,
                    input_expr,
                    terminal.filter_expr,
                    terminal.distinct,
                ))
            }
        }
    }

    /// Borrow the resolved target slot when this prepared SQL scalar strategy is field-targeted.
    #[must_use]
    pub(crate) const fn target_slot(&self) -> Option<&FieldSlot> {
        self.target.field_slot()
    }

    /// Borrow the aggregate input expression when this prepared SQL scalar strategy is expression-backed.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn input_expr(&self) -> Option<&Expr> {
        self.target.input_expr()
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
        self.distinct_input
    }

    /// Return the stable descriptor/runtime shape label for this prepared strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn descriptor_shape(&self) -> PreparedSqlScalarAggregateDescriptorShape {
        self.descriptor_shape
    }

    /// Return the stable runtime-family projection for this prepared SQL
    /// scalar aggregate strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn runtime_descriptor(&self) -> PreparedSqlScalarAggregateRuntimeDescriptor {
        self.descriptor_shape.runtime_descriptor()
    }

    /// Return the canonical aggregate kind for this prepared SQL scalar strategy.
    #[must_use]
    pub(crate) const fn aggregate_kind(&self) -> AggregateKind {
        self.descriptor_shape.aggregate_kind()
    }

    /// Build the executor terminal consumed by structural aggregate execution.
    ///
    /// SQL lowering owns the aggregate strategy and therefore owns the final
    /// executor terminal construction. Session execution keeps only SQL result
    /// labels, fixed scales, cache attribution, and statement shaping.
    pub(in crate::db) fn into_executor_terminal(
        self,
    ) -> Result<StructuralAggregateTerminal, &'static str> {
        let Self {
            target,
            filter_expr,
            distinct_input,
            descriptor_shape,
        } = self;
        let (target_slot, input_expr) = target.into_executor_parts();

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
