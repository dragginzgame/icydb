//! Module: db::session::sql::computed_projection::model
//! Responsibility: model the bounded computed SQL projection surface supported
//! at the session edge outside generic structural planning.
//! Does not own: computed projection execution or parser tokenization.
//! Boundary: keeps computed projection statement/column modeling separate from generic query planning.

use crate::{
    db::sql::parser::{SqlStatement, SqlTextFunction},
    value::Value,
};

///
/// SqlComputedProjectionTransform
///
/// Session-owned transform taxonomy for the narrow computed SQL projection
/// lane.
/// This stays local to SQL dispatch so the first `0.66` text bundle does not
/// reopen generic planner/executor expression ownership.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql::computed_projection) enum SqlComputedProjectionTransform {
    Field,
    Trim,
    Ltrim,
    Rtrim,
    Lower,
    Upper,
    Length,
    Left,
    Right,
    StartsWith,
    EndsWith,
    Contains,
    Position,
    Replace,
    Substring,
}

impl SqlComputedProjectionTransform {
    // Return the stable function label used in computed SQL projection errors.
    pub(in crate::db::session::sql::computed_projection) const fn label(self) -> &'static str {
        match self {
            Self::Field => "FIELD",
            Self::Trim => "TRIM",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
            Self::Length => "LENGTH",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::StartsWith => "STARTS_WITH",
            Self::EndsWith => "ENDS_WITH",
            Self::Contains => "CONTAINS",
            Self::Position => "POSITION",
            Self::Replace => "REPLACE",
            Self::Substring => "SUBSTRING",
        }
    }
}

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
    const fn projection_transform(self) -> SqlComputedProjectionTransform {
        match self {
            Self::Trim => SqlComputedProjectionTransform::Trim,
            Self::Ltrim => SqlComputedProjectionTransform::Ltrim,
            Self::Rtrim => SqlComputedProjectionTransform::Rtrim,
            Self::Lower => SqlComputedProjectionTransform::Lower,
            Self::Upper => SqlComputedProjectionTransform::Upper,
            Self::Length => SqlComputedProjectionTransform::Length,
            Self::Left => SqlComputedProjectionTransform::Left,
            Self::Right => SqlComputedProjectionTransform::Right,
            Self::StartsWith => SqlComputedProjectionTransform::StartsWith,
            Self::EndsWith => SqlComputedProjectionTransform::EndsWith,
            Self::Contains => SqlComputedProjectionTransform::Contains,
            Self::Position => SqlComputedProjectionTransform::Position,
            Self::Replace => SqlComputedProjectionTransform::Replace,
            Self::Substring => SqlComputedProjectionTransform::Substring,
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
    pub(in crate::db::session::sql::computed_projection) source_field: String,
    pub(in crate::db::session::sql::computed_projection) output_label: String,
    pub(in crate::db::session::sql::computed_projection) transform: SqlComputedProjectionTransform,
    pub(in crate::db::session::sql::computed_projection) literal: Option<Value>,
    pub(in crate::db::session::sql::computed_projection) literal2: Option<Value>,
    pub(in crate::db::session::sql::computed_projection) literal3: Option<Value>,
}

impl SqlComputedProjectionItem {
    /// Build one plain field passthrough projection item.
    #[must_use]
    pub(in crate::db::session::sql::computed_projection) fn field(field: String) -> Self {
        Self {
            output_label: field.clone(),
            source_field: field,
            transform: SqlComputedProjectionTransform::Field,
            literal: None,
            literal2: None,
            literal3: None,
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
        let transform = function.projection_transform();
        let output_label = render_text_function_projection_label(
            function,
            field.as_str(),
            literal.as_ref(),
            literal2.as_ref(),
            literal3.as_ref(),
        );

        Self {
            output_label,
            source_field: field,
            transform,
            literal,
            literal2,
            literal3,
        }
    }
}

///
/// SqlComputedProjectionPlan
///
/// Narrow session-owned execution plan for computed SQL projection dispatch.
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
}

// Render one narrow computed-projection literal back into a stable SQL-style
// label fragment for the final column names.
fn render_computed_sql_projection_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Value::Int(value) => value.to_string(),
        Value::Uint(value) => value.to_string(),
        _ => "<invalid-text-literal>".to_string(),
    }
}

// Render the final computed SQL projection label using the narrow shipped
// text-function surface and its current literal shapes.
fn render_text_function_projection_label(
    function: SqlTextFunction,
    field: &str,
    literal: Option<&Value>,
    literal2: Option<&Value>,
    literal3: Option<&Value>,
) -> String {
    let function_name = function.projection_label();

    match (function, literal, literal2, literal3) {
        (SqlTextFunction::Position, Some(literal), _, _) => format!(
            "{function_name}({}, {field})",
            render_computed_sql_projection_literal(literal),
        ),
        (
            SqlTextFunction::StartsWith | SqlTextFunction::EndsWith | SqlTextFunction::Contains,
            Some(literal),
            _,
            _,
        ) => format!(
            "{function_name}({field}, {})",
            render_computed_sql_projection_literal(literal),
        ),
        (SqlTextFunction::Replace, Some(from), Some(to), _) => format!(
            "{function_name}({field}, {}, {})",
            render_computed_sql_projection_literal(from),
            render_computed_sql_projection_literal(to),
        ),
        (SqlTextFunction::Left | SqlTextFunction::Right, Some(length), _, _) => format!(
            "{function_name}({field}, {})",
            render_computed_sql_projection_literal(length),
        ),
        (SqlTextFunction::Substring, Some(start), Some(len), _) => format!(
            "{function_name}({field}, {}, {})",
            render_computed_sql_projection_literal(start),
            render_computed_sql_projection_literal(len),
        ),
        (SqlTextFunction::Substring, Some(start), None, _) => format!(
            "{function_name}({field}, {})",
            render_computed_sql_projection_literal(start),
        ),
        _ => format!("{function_name}({field})"),
    }
}
