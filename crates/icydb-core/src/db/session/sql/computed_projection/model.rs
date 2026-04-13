//! Module: db::session::sql::computed_projection::model
//! Responsibility: model the bounded computed SQL projection surface supported
//! at the session edge outside generic structural planning.
//! Does not own: computed projection execution or parser tokenization.
//! Boundary: keeps computed projection statement/column modeling separate from generic query planning.

use crate::db::{
    query::builder::{TextProjectionExpr, TextProjectionTransform},
    sql::parser::{SqlStatement, SqlTextFunction},
};
use crate::value::Value;

impl SqlTextFunction {
    // Return the stable SQL function label used by the session-owned computed
    // projection lane.
    pub(in crate::db::session::sql::computed_projection) const fn projection_label(
        self,
    ) -> &'static str {
        self.projection_transform().label()
    }

    // Map the parser-owned text-function taxonomy onto the session-owned
    // computed projection transform lane.
    const fn projection_transform(self) -> TextProjectionTransform {
        match self {
            Self::Trim => TextProjectionTransform::Trim,
            Self::Ltrim => TextProjectionTransform::Ltrim,
            Self::Rtrim => TextProjectionTransform::Rtrim,
            Self::Lower => TextProjectionTransform::Lower,
            Self::Upper => TextProjectionTransform::Upper,
            Self::Length => TextProjectionTransform::Length,
            Self::Left => TextProjectionTransform::Left,
            Self::Right => TextProjectionTransform::Right,
            Self::StartsWith => TextProjectionTransform::StartsWith,
            Self::EndsWith => TextProjectionTransform::EndsWith,
            Self::Contains => TextProjectionTransform::Contains,
            Self::Position => TextProjectionTransform::Position,
            Self::Replace => TextProjectionTransform::Replace,
            Self::Substring => TextProjectionTransform::Substring,
        }
    }
}

///
/// SqlComputedProjectionItem
///
/// One computed SQL projection item paired with its source field and output
/// label.
/// Rows are first loaded through the existing structural field projection lane,
/// then transformed at the session SQL boundary according to this contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql::computed_projection) struct SqlComputedProjectionItem {
    pub(in crate::db::session::sql::computed_projection) expr: TextProjectionExpr,
    pub(in crate::db::session::sql::computed_projection) output_label: String,
}

impl SqlComputedProjectionItem {
    /// Build one plain field passthrough projection item.
    #[must_use]
    pub(in crate::db::session::sql::computed_projection) fn field(field: String) -> Self {
        Self {
            output_label: field.clone(),
            expr: TextProjectionExpr::new(field, TextProjectionTransform::Field),
        }
    }

    /// Build one passthrough projection item for an already-computed output column.
    #[must_use]
    pub(in crate::db::session::sql::computed_projection) fn passthrough(
        output_label: String,
    ) -> Self {
        Self {
            expr: TextProjectionExpr::new(output_label.clone(), TextProjectionTransform::Field),
            output_label,
        }
    }

    /// Build one text-function projection item.
    #[must_use]
    pub(in crate::db::session::sql::computed_projection) fn text_function(
        function: SqlTextFunction,
        field: String,
        literal: Option<Value>,
        literal2: Option<Value>,
        literal3: Option<Value>,
    ) -> Self {
        let expr = TextProjectionExpr::new(field, function.projection_transform())
            .with_optional_literal(literal)
            .with_optional_second_literal(literal2)
            .with_optional_third_literal(literal3);
        let output_label = expr.sql_label();

        Self { expr, output_label }
    }

    /// Return the underlying shared text-projection expression.
    #[must_use]
    pub(in crate::db::session::sql::computed_projection) const fn expr(
        &self,
    ) -> &TextProjectionExpr {
        &self.expr
    }
}

///
/// SqlComputedProjectionPlan
///
/// Narrow session-owned execution plan for computed SQL projection execution.
/// This rewrites one supported computed projection into a base field-only
/// select, then applies the requested transforms after structural row loading.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) struct SqlComputedProjectionPlan {
    pub(in crate::db::session::sql::computed_projection) base_statement: SqlStatement,
    pub(in crate::db::session::sql::computed_projection) items: Vec<SqlComputedProjectionItem>,
}

impl SqlComputedProjectionPlan {
    /// Clone the rewritten base statement consumed by shared lowering.
    #[must_use]
    pub(in crate::db::session::sql) fn cloned_base_statement(&self) -> SqlStatement {
        self.base_statement.clone()
    }

    /// Consume this plan and return the rewritten base statement.
    #[must_use]
    pub(in crate::db::session::sql) fn into_base_statement(self) -> SqlStatement {
        self.base_statement
    }

    /// Return whether this computed-projection plan targets a grouped SQL surface.
    #[must_use]
    pub(in crate::db::session::sql) const fn is_grouped(&self) -> bool {
        self.group_key_arity() != 0
    }

    /// Return the number of grouped key items carried by the rewritten base statement.
    #[must_use]
    pub(in crate::db::session::sql) const fn group_key_arity(&self) -> usize {
        let SqlStatement::Select(select) = &self.base_statement else {
            return 0;
        };

        select.group_by.len()
    }

    /// Return the outward projection labels requested by this computed-projection plan.
    #[must_use]
    pub(in crate::db::session::sql) fn output_labels(&self) -> Vec<String> {
        self.items
            .iter()
            .map(|item| item.output_label.clone())
            .collect()
    }
}
