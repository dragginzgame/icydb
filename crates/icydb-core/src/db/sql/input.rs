//! SQL syntax adapter for the shared, borrowed query-input budget.
//! Does not own binding admission, normalization, parsing quotas or DDL policy.

#[cfg(test)]
mod tests;

use crate::db::{
    query::admission::input::QueryInputBudget,
    sql::parser::{
        SqlAggregateCall, SqlDeleteStatement, SqlExpr, SqlInsertSource, SqlMembershipValue,
        SqlOrderTerm, SqlProjection, SqlReturningProjection, SqlSelectItem, SqlSelectStatement,
        SqlStatement, SqlWriteValue,
    },
};
use icydb_diagnostic_code::QueryReadAdmissionCode;

struct SqlInput<'a> {
    budget: QueryInputBudget,
    binding_payloads: &'a [usize],
}

impl<'a> SqlInput<'a> {
    const fn new(binding_payloads: &'a [usize]) -> Self {
        Self {
            budget: QueryInputBudget::new(),
            binding_payloads,
        }
    }

    fn optional_name(&mut self, name: Option<&str>) -> Result<(), QueryReadAdmissionCode> {
        if let Some(name) = name {
            self.budget.name(name, 1)?;
        }
        Ok(())
    }

    fn names(&mut self, names: &[String]) -> Result<(), QueryReadAdmissionCode> {
        for name in names {
            self.budget.name(name, 1)?;
        }
        Ok(())
    }

    fn select(&mut self, select: &SqlSelectStatement) -> Result<(), QueryReadAdmissionCode> {
        self.budget.name(&select.entity, 1)?;
        self.optional_name(select.table_alias.as_deref())?;
        if let SqlProjection::Items(items) = &select.projection {
            for item in items {
                match item {
                    SqlSelectItem::Field(field) => self.budget.name(field, 1)?,
                    SqlSelectItem::Aggregate(aggregate) => {
                        self.budget.node(1)?;
                        self.aggregate_children(aggregate, 2)?;
                    }
                    SqlSelectItem::Expr(expr) => self.expr(expr, 1)?,
                }
            }
        }
        // None still occupies an authored projection-alias slot.
        for alias in &select.projection_aliases {
            self.budget.node(1)?;
            if let Some(alias) = alias {
                self.budget.payload(alias.len())?;
            }
        }
        self.optional_expr(select.predicate.as_ref())?;
        self.names(&select.group_by)?;
        for having in &select.having {
            self.expr(having, 1)?;
        }
        self.order(&select.order_by)
    }

    fn order(&mut self, terms: &[SqlOrderTerm]) -> Result<(), QueryReadAdmissionCode> {
        for term in terms {
            self.expr(&term.field, 1)?;
        }
        Ok(())
    }

    fn delete(&mut self, delete: &SqlDeleteStatement) -> Result<(), QueryReadAdmissionCode> {
        self.budget.name(&delete.entity, 1)?;
        self.optional_name(delete.table_alias.as_deref())?;
        self.optional_expr(delete.predicate.as_ref())?;
        self.order(&delete.order_by)?;
        self.returning(delete.returning.as_ref())
    }

    fn optional_expr(&mut self, expr: Option<&SqlExpr>) -> Result<(), QueryReadAdmissionCode> {
        if let Some(expr) = expr {
            self.expr(expr, 1)?;
        }
        Ok(())
    }

    fn returning(
        &mut self,
        returning: Option<&SqlReturningProjection>,
    ) -> Result<(), QueryReadAdmissionCode> {
        if let Some(SqlReturningProjection::Fields(fields)) = returning {
            self.names(fields)?;
        }
        Ok(())
    }

    fn write_value(
        &mut self,
        value: &SqlWriteValue,
        depth: usize,
    ) -> Result<(), QueryReadAdmissionCode> {
        match value {
            SqlWriteValue::Literal(value) => self.budget.value(value, depth),
            SqlWriteValue::Default => self.budget.node(depth),
        }
    }

    fn aggregate_children(
        &mut self,
        aggregate: &SqlAggregateCall,
        depth: usize,
    ) -> Result<(), QueryReadAdmissionCode> {
        if let Some(input) = &aggregate.input {
            self.expr(input, depth)?;
        }
        if let Some(filter) = &aggregate.filter_expr {
            self.expr(filter, depth)?;
        }
        Ok(())
    }

    fn expr(&mut self, expr: &SqlExpr, depth: usize) -> Result<(), QueryReadAdmissionCode> {
        self.budget.node(depth)?;
        match expr {
            SqlExpr::Field(field) => self.budget.payload(field.len()),
            SqlExpr::FieldPath { root, segments } => {
                self.budget.payload(root.len())?;
                for segment in segments {
                    self.budget.name(segment, depth + 1)?;
                }
                Ok(())
            }
            SqlExpr::Literal(value) => self.budget.value(value, depth + 1),
            SqlExpr::Param { index } => {
                if let Some(bytes) = self.binding_payloads.get(*index) {
                    // Substitution adds a Value below the literal-expression node.
                    self.budget.node(depth + 1)?;
                    self.budget.payload(*bytes)?;
                }
                Ok(())
            }
            SqlExpr::Aggregate(aggregate) => self.aggregate_children(aggregate, depth + 1),
            SqlExpr::Membership { expr, values, .. } => {
                self.expr(expr, depth + 1)?;
                for value in values {
                    match value {
                        SqlMembershipValue::Literal(value) => {
                            self.budget.value(value, depth + 1)?;
                        }
                        SqlMembershipValue::Param { index } => {
                            self.budget.node(depth + 1)?;
                            if let Some(bytes) = self.binding_payloads.get(*index) {
                                self.budget.payload(*bytes)?;
                            }
                        }
                    }
                }
                Ok(())
            }
            SqlExpr::Like { expr, pattern, .. } => {
                self.budget.payload(pattern.len())?;
                self.expr(expr, depth + 1)
            }
            SqlExpr::NullTest { expr, .. } | SqlExpr::Unary { expr, .. } => {
                self.expr(expr, depth + 1)
            }
            SqlExpr::Binary { left, right, .. } => {
                self.expr(left, depth + 1)?;
                self.expr(right, depth + 1)
            }
            SqlExpr::FunctionCall { args, .. } => {
                for arg in args {
                    self.expr(arg, depth + 1)?;
                }
                Ok(())
            }
            SqlExpr::Case { arms, else_expr } => {
                for arm in arms {
                    self.expr(&arm.condition, depth + 1)?;
                    self.expr(&arm.result, depth + 1)?;
                }
                if let Some(expr) = else_expr {
                    self.expr(expr, depth + 1)?;
                }
                Ok(())
            }
        }
    }
}

/// Admit authored syntax and, when supplied, each effective scalar operand copy.
/// Binding arity/family admission remains at the binding owner; missing slots
/// are not fabricated here. Empty sizes represent unbound authored syntax.
pub(in crate::db) fn validate_sql_statement_input(
    statement: &SqlStatement,
    binding_payloads: &[usize],
) -> Result<(), QueryReadAdmissionCode> {
    let mut input = SqlInput::new(binding_payloads);
    match statement {
        SqlStatement::Select(select) => input.select(select),
        SqlStatement::Delete(delete) => input.delete(delete),
        SqlStatement::Update(update) => {
            input.budget.name(&update.entity, 1)?;
            input.optional_name(update.table_alias.as_deref())?;
            for assignment in &update.assignments {
                input.budget.name(&assignment.field, 1)?;
                input.write_value(&assignment.value, 1)?;
            }
            input.optional_expr(update.predicate.as_ref())?;
            input.order(&update.order_by)?;
            input.returning(update.returning.as_ref())
        }
        SqlStatement::Insert(insert) => {
            input.budget.name(&insert.entity, 1)?;
            input.names(&insert.columns)?;
            match &insert.source {
                SqlInsertSource::Values(rows) => {
                    for row in rows {
                        input.budget.node(1)?;
                        for value in row {
                            input.write_value(value, 2)?;
                        }
                    }
                }
                SqlInsertSource::DefaultValues => {}
                SqlInsertSource::Select(select) => input.select(select)?,
            }
            input.returning(insert.returning.as_ref())
        }
        #[cfg(feature = "sql")]
        SqlStatement::Explain(explain) => match &explain.statement {
            crate::db::sql::parser::SqlExplainTarget::Select(select) => input.select(select),
            crate::db::sql::parser::SqlExplainTarget::Delete(delete) => input.delete(delete),
        },
        SqlStatement::Describe(statement) => input.budget.name(&statement.entity, 1),
        SqlStatement::ShowConstraints(statement) => input.budget.name(&statement.entity, 1),
        SqlStatement::ShowIndexes(statement) => input.budget.name(&statement.entity, 1),
        SqlStatement::ShowColumns(statement) => input.budget.name(&statement.entity, 1),
        SqlStatement::ShowRelations(statement) => input.budget.name(&statement.entity, 1),
        SqlStatement::ShowEntities(statement) => input.optional_name(statement.entity.as_deref()),
        // CHECK carries the same SQL expression grammar. Keep that syntax floor
        // bounded without replacing catalog-owned DDL admission with query policy.
        SqlStatement::Ddl(
            crate::db::sql::parser::SqlDdlStatement::AlterTableAddCheckConstraint(check),
        ) => input.expr(&check.expression, 1),
        SqlStatement::ShowStores(_) | SqlStatement::ShowMemory(_) | SqlStatement::Ddl(_) => Ok(()),
    }
}

/// Bound the concrete BETWEEN rewrite before copying its left operand.
pub(in crate::db::sql) fn validate_sql_between_input(
    left: &SqlExpr,
    lower: &SqlExpr,
    upper: &SqlExpr,
) -> Result<(), QueryReadAdmissionCode> {
    let mut input = SqlInput::new(&[]);
    input.budget.node(1)?;
    for bound in [lower, upper] {
        input.budget.node(2)?;
        input.expr(left, 3)?;
        input.expr(bound, 3)?;
    }
    Ok(())
}
