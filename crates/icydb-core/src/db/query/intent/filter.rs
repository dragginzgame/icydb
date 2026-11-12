//! Current-schema filter admission owns expression/predicate agreement and
//! original input facts. Planning and metadata shortcuts consume its result.

#[cfg(feature = "sql")]
mod sql;

use crate::db::{
    QueryError,
    predicate::{CompareOp, Predicate, normalize_enum_literals, normalize_with_budget},
    query::{
        plan::{
            PrimaryKeyInputResourceSummary,
            expr::{
                BinaryOp, Expr, derive_normalized_bool_expr_predicate_subset,
                is_normalized_bool_expr, normalize_bool_expr,
            },
            primary_key_input_resource_from_value_list,
        },
        preparation::PreparationWork,
    },
    schema::SchemaInfo,
};
use crate::value::Value;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

///
/// AdmittedFilter
///
/// Canonical scalar filter representation stored by query intent.
/// It owns the normalized expression that preserves full runtime semantics and
/// the optional runtime-predicate subset derived once at append boundaries.
///

#[derive(Clone, Debug)]
pub(in crate::db::query) struct AdmittedFilter {
    authority: crate::db::schema::AcceptedSchemaAuthority,
    entity_name: Option<String>,
    primary_key_input_resource: Option<PrimaryKeyInputResourceSummary>,
    expression: Expr,
    predicate_subset: Option<Predicate>,
    // True proves a present subset covers every appended clause. Absence versus
    // partial coverage is already represented by predicate_subset itself.
    predicate_is_complete: bool,
}

impl AdmittedFilter {
    /// Bind a semantic expression once against the current accepted schema.
    pub(in crate::db::query::intent) fn from_expr(
        schema: &SchemaInfo,
        expr: Expr,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Self::admit(schema, normalize_bool_expr(expr, work)?, false, work)
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::query::intent) fn from_sql_expr(
        schema: &SchemaInfo,
        expr: Expr,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Self::admit(
            schema,
            sql::canonicalize_sql_filter_expr_for_schema(schema, expr),
            true,
            work,
        )
    }

    // Frontends supply expressions, never an independently asserted predicate or
    // coverage proof. Capture input facts before schema/boolean normalization.
    fn admit(
        schema: &SchemaInfo,
        expr: Expr,
        sql_literals: bool,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        let predicate = derive_normalized_bool_expr_predicate_subset(&expr, work)?;
        let predicate_is_complete = predicate.is_some();
        #[cfg(feature = "sql")]
        let predicate = predicate.map(|predicate| {
            if sql_literals {
                sql::canonicalize_sql_predicate_for_schema(schema, predicate)
            } else {
                predicate
            }
        });
        #[cfg(not(feature = "sql"))]
        let _ = sql_literals;
        let primary_key_input_resource =
            primary_key_input_resource_from_predicate(schema, predicate.as_ref(), work)?;
        let predicate_subset = normalize_query_predicate(schema, predicate.as_ref(), work)?;
        Ok(Self {
            authority: schema.value_catalog_handle().authority().clone(),
            entity_name: schema
                .entity_name()
                .map(|name| work.copy_text(name))
                .transpose()?,
            primary_key_input_resource,
            expression: expr,
            predicate_subset,
            predicate_is_complete,
        })
    }

    /// Refuse reuse under another accepted entity or schema root.
    pub(in crate::db::query) fn validate_schema(
        &self,
        schema: &SchemaInfo,
    ) -> Result<(), QueryError> {
        if &self.authority != schema.value_catalog_handle().authority()
            || self.entity_name.as_deref() != schema.entity_name()
        {
            return Err(QueryError::invariant());
        }
        Ok(())
    }

    pub(in crate::db::query) const fn primary_key_input_resource(
        &self,
    ) -> Option<PrimaryKeyInputResourceSummary> {
        self.primary_key_input_resource
    }

    /// Borrow the normalized expression that owns this filter's semantics.
    #[must_use]
    pub(in crate::db::query::intent) const fn logical_filter_expr(&self) -> &Expr {
        &self.expression
    }

    /// Borrow the already-derived predicate subset used by access planning.
    #[must_use]
    pub(in crate::db::query::intent) const fn predicate_subset(&self) -> Option<&Predicate> {
        self.predicate_subset.as_ref()
    }

    /// Return whether the predicate subset fully represents the visible
    /// expression view used by existing logical-planning inputs.
    #[must_use]
    pub(in crate::db::query::intent) const fn predicate_subset_covers_expr(&self) -> bool {
        self.predicate_is_complete
    }

    // Append one filter clause by AND-ing the semantic expression and combining
    // only the predicate subsets that were derived at their original boundary.
    pub(in crate::db::query::intent) fn append(
        &mut self,
        filter: Self,
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        if self.authority != filter.authority || self.entity_name != filter.entity_name {
            return Err(QueryError::invariant());
        }
        let filter_is_complete = filter.predicate_is_complete;
        let subset = combine_predicate_subset(
            self.predicate_subset
                .as_ref()
                .map(|predicate| work.copy_predicate(predicate))
                .transpose()?,
            filter.predicate_subset,
        );
        let subset = subset
            .map(|predicate| normalize_with_budget(predicate, work).map_err(QueryError::execute))
            .transpose()?;
        let primary_key_input_resource = match (
            self.primary_key_input_resource,
            filter.primary_key_input_resource,
        ) {
            (Some(a), Some(b)) => Some(PrimaryKeyInputResourceSummary::new(
                a.raw_term_count().saturating_add(b.raw_term_count()),
                a.estimated_payload_bytes()
                    .saturating_add(b.estimated_payload_bytes()),
            )),
            (a, b) => a.or(b),
        };

        self.expression = normalize_bool_expr(
            Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(self.expression.take()),
                right: Box::new(filter.expression),
            },
            work,
        )?;
        debug_assert!(is_normalized_bool_expr(&self.expression));

        self.primary_key_input_resource = primary_key_input_resource;
        self.predicate_subset = subset;
        self.predicate_is_complete &= filter_is_complete;
        Ok(())
    }
}

// Preserve independently proved restrictions when another clause requires the
// expression executor. Conjunction normalization stays inside this owner.
fn combine_predicate_subset(
    existing: Option<Predicate>,
    appended: Option<Predicate>,
) -> Option<Predicate> {
    match (existing, appended) {
        (Some(existing), Some(appended)) => Some(Predicate::And(vec![existing, appended])),
        (Some(existing), None) => Some(existing),
        (None, Some(appended)) => Some(appended),
        (None, None) => None,
    }
}

// Normalize one optional predicate into canonical planner form.
pub(in crate::db::query) fn normalize_query_predicate(
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
    work: &PreparationWork<'_>,
) -> Result<Option<Predicate>, QueryError> {
    predicate
        .map(|predicate| {
            let predicate = normalize_enum_literals(schema_info, predicate, work)?;

            normalize_with_budget(predicate, work).map_err(QueryError::execute)
        })
        .transpose()
}

fn primary_key_input_resource_from_predicate(
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
    work: &PreparationWork<'_>,
) -> Result<Option<PrimaryKeyInputResourceSummary>, QueryError> {
    let (Some(primary_key_name), Some(predicate)) =
        (schema_info.scalar_primary_key_name(), predicate)
    else {
        return Ok(None);
    };
    let mut resource = PrimaryKeyInputResourceAccumulator::default();
    collect_primary_key_in_resource(predicate, primary_key_name, &mut resource, work)?;

    Ok(resource.into_summary())
}

fn collect_primary_key_in_resource(
    predicate: &Predicate,
    primary_key_name: &str,
    resource: &mut PrimaryKeyInputResourceAccumulator,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match predicate {
        Predicate::Compare(cmp) => {
            // Ineligible operators and different name lengths need no byte scan.
            if cmp.op == CompareOp::In && cmp.field.len() == primary_key_name.len() {
                work.charge(
                    Resource::PredicateExpressionSteps,
                    primary_key_name.len() as u64,
                )?;
                if cmp.field == primary_key_name
                    && let Value::List(values) = &cmp.value
                {
                    resource.add_values(values, work)?;
                }
            }
        }
        Predicate::And(children) => {
            for child in children {
                collect_primary_key_in_resource(child, primary_key_name, resource, work)?;
            }
        }
        Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::CompareFields(_)
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::True
        | Predicate::False => {}
    }
    Ok(())
}

#[derive(Default)]
struct PrimaryKeyInputResourceAccumulator {
    raw_term_count: u32,
    estimated_payload_bytes: u32,
}

impl PrimaryKeyInputResourceAccumulator {
    fn add_values(
        &mut self,
        values: &[Value],
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        let Some(summary) = primary_key_input_resource_from_value_list(values, work)
            .map_err(QueryError::execute)?
        else {
            return Ok(());
        };
        self.raw_term_count = self.raw_term_count.saturating_add(summary.raw_term_count());
        self.estimated_payload_bytes = self
            .estimated_payload_bytes
            .saturating_add(summary.estimated_payload_bytes());
        Ok(())
    }

    const fn into_summary(self) -> Option<PrimaryKeyInputResourceSummary> {
        if self.raw_term_count == 0 {
            return None;
        }

        Some(PrimaryKeyInputResourceSummary::new(
            self.raw_term_count,
            self.estimated_payload_bytes,
        ))
    }
}
crate::retained::retained_fields!(AdmittedFilter {
    Self { authority, entity_name, primary_key_input_resource: _, expression, predicate_subset, predicate_is_complete: _ } => [authority, entity_name, expression, predicate_subset],
});

#[cfg(test)]
mod tests;
