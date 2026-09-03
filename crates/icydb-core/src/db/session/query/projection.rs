//! Module: db::session::query::projection
//! Responsibility: query-owned structural projection preparation and payloads.
//! Does not own: SQL statement shaping, parser semantics, or typed row decoding.
//! Boundary: one accepted-plan projection contract feeds dynamic, typed, and
//! SQL frontend adapters.

use crate::{
    db::{
        DbSession, QueryError, TraceReuseEvent,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::{
            builder::scalar_projection::render_scalar_projection_expr_plan_label,
            intent::StructuralQuery,
            plan::{
                CardinalityTiebreakRoutePin,
                expr::{Expr, ProjectionField, ProjectionSpec},
            },
        },
        schema::{AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, output_value_from_runtime},
    },
    traits::CanisterKind,
    value::{OutputValue, Value},
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

type StructuralProjectionPayloadComponents =
    (Vec<String>, Vec<Option<u32>>, Vec<Vec<OutputValue>>, u32);
#[cfg(feature = "sql")]
type StructuralProjectionRuntimeComponents = (Vec<String>, Vec<Option<u32>>, Vec<Vec<Value>>, u32);

/// Frozen outward projection labels and decimal display scales derived from
/// one accepted structural plan.
#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralProjectionContract {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
}

impl StructuralProjectionContract {
    #[must_use]
    pub(in crate::db) fn from_projection_spec(projection: &ProjectionSpec) -> Self {
        Self {
            columns: projection_labels_from_projection_spec(projection),
            fixed_scales: projection_fixed_scales_from_projection_spec(projection),
        }
    }

    #[must_use]
    pub(in crate::db) fn into_components(self) -> (Vec<String>, Vec<Option<u32>>) {
        (self.columns, self.fixed_scales)
    }
}

/// Runtime-value projection payload shared below dynamic and SQL response
/// adapters.
#[derive(Debug)]
pub(in crate::db::session) struct StructuralProjectionPayload {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: Vec<Vec<Value>>,
    row_count: u32,
    value_catalog: AcceptedValueCatalogHandle,
}

impl StructuralProjectionPayload {
    #[must_use]
    pub(in crate::db::session) const fn new(
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<Vec<Value>>,
        row_count: u32,
        value_catalog: AcceptedValueCatalogHandle,
    ) -> Self {
        Self {
            columns,
            fixed_scales,
            rows,
            row_count,
            value_catalog,
        }
    }

    pub(in crate::db::session) fn into_output_components(
        self,
    ) -> Result<StructuralProjectionPayloadComponents, QueryError> {
        let catalog = self.value_catalog.enum_catalog();
        let rows = self
            .rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|value| {
                        output_value_from_runtime(catalog, value)
                            .map_err(|_error| QueryError::invariant())
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok((self.columns, self.fixed_scales, rows, self.row_count))
    }

    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::session) fn into_runtime_components(
        self,
    ) -> StructuralProjectionRuntimeComponents {
        (self.columns, self.fixed_scales, self.rows, self.row_count)
    }
}

// Render canonical projection labels from one frozen projection spec for every
// frontend. SQL may add debug annotations, but it does not own these labels.
pub(in crate::db::session) fn projection_labels_from_projection_spec(
    projection: &ProjectionSpec,
) -> Vec<String> {
    let mut labels = Vec::with_capacity(projection.len());

    for field in projection.fields() {
        match field {
            ProjectionField::Scalar {
                expr: _,
                alias: Some(alias),
            } => labels.push(alias.as_str().to_string()),
            ProjectionField::Scalar { expr, alias: None } => {
                labels.push(match expr {
                    Expr::Field(field) => field.as_str().to_string(),
                    Expr::Aggregate(aggregate) => {
                        let kind = aggregate.kind().canonical_label();
                        let distinct = if aggregate.is_distinct() {
                            "DISTINCT "
                        } else {
                            ""
                        };
                        if let Some(input_expr) = aggregate.input_expr() {
                            let input = render_scalar_projection_expr_plan_label(input_expr);

                            format!("{kind}({distinct}{input})")
                        } else {
                            format!("{kind}({distinct}*)")
                        }
                    }
                    #[cfg(test)]
                    Expr::Alias { name, .. } => name.as_str().to_string(),
                    Expr::FieldPath(_)
                    | Expr::Literal(_)
                    | Expr::FunctionCall { .. }
                    | Expr::Case { .. }
                    | Expr::Binary { .. }
                    | Expr::Unary { .. } => render_scalar_projection_expr_plan_label(expr),
                });
            }
        }
    }

    labels
}

fn projection_fixed_scales_from_projection_spec(projection: &ProjectionSpec) -> Vec<Option<u32>> {
    projection
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar { expr, .. } => {
                let Expr::FunctionCall { function, args } = expr else {
                    return None;
                };
                function.fixed_decimal_scale(args)
            }
        })
        .collect()
}

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session) fn structural_projection_prepared_plan_for_accepted_authority(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        lane: DiagnosticExecutionLane,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            StructuralProjectionContract,
            TraceReuseEvent,
        ),
        QueryError,
    > {
        let schema_fingerprint = authority.accepted_schema_fingerprint();
        let (prepared_plan, reuse) = self
            .cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority.clone(),
                accepted_schema,
                schema_fingerprint,
                query,
                lane,
            )?;
        let projection_spec = prepared_plan.logical_plan().projection_spec_with_schema(
            authority
                .accepted_schema_info()
                .ok_or_else(QueryError::invariant)?,
        );
        let projection = StructuralProjectionContract::from_projection_spec(&projection_spec);

        Ok((prepared_plan, projection, reuse))
    }

    pub(in crate::db::session) fn structural_projection_prepared_plan_for_accepted_authority_with_route_pin(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        lane: DiagnosticExecutionLane,
        route_pin: CardinalityTiebreakRoutePin,
    ) -> Result<
        Option<(
            SharedPreparedExecutionPlan,
            StructuralProjectionContract,
            TraceReuseEvent,
        )>,
        QueryError,
    > {
        let schema_fingerprint = authority.accepted_schema_fingerprint();
        let Some((prepared_plan, reuse)) = self
            .shared_query_plan_for_accepted_authority_with_route_pin(
                authority.clone(),
                accepted_schema,
                schema_fingerprint,
                query,
                lane,
                route_pin,
            )?
        else {
            return Ok(None);
        };
        let projection_spec = prepared_plan.logical_plan().projection_spec_with_schema(
            authority
                .accepted_schema_info()
                .ok_or_else(QueryError::invariant)?,
        );
        let projection = StructuralProjectionContract::from_projection_spec(&projection_spec);

        Ok(Some((prepared_plan, projection, reuse)))
    }
}
