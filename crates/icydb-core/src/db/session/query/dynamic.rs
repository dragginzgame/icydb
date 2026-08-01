//! Module: db::session::query::dynamic
//! Responsibility: lower and execute public dynamic reads against accepted schema.
//! Does not own: query planning, accepted schema construction, or row projection.
//! Boundary: entity-name requests converge on the shared structural read lane.

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicTypedEntityBinding, MissingRowPolicy, QueryError,
        RowProjectionOutput,
        query::{admission::QueryAdmissionPolicy, intent::StructuralQuery},
        session::AcceptedSchemaCatalogContext,
    },
    error::InternalError,
    traits::CanisterKind,
};

#[derive(Clone, Copy)]
enum DynamicReadLane {
    Public,
    Trusted,
}

impl<C: CanisterKind> DbSession<C> {
    fn execute_dynamic_query_against_catalog(
        &self,
        request: &DynamicQuery,
        lane: DynamicReadLane,
        catalog: AcceptedSchemaCatalogContext,
    ) -> Result<RowProjectionOutput, QueryError> {
        let schema = catalog.accepted_schema_info();
        let mut query = StructuralQuery::new(MissingRowPolicy::Ignore);
        if let Some(filter) = request.filter_expr() {
            query = query.filter_for_schema(&schema, filter.clone());
        }
        for order in request.order_terms() {
            query = query.order_term(order.clone());
        }
        if !request.selected_fields().is_empty() {
            query = query.select_fields(request.selected_fields().iter().cloned());
        }
        if let Some(limit) = request.row_limit() {
            query = query.limit(limit);
        }

        let authority = catalog
            .accepted_entity_authority()
            .map_err(QueryError::execute)?;
        let public_admission = match lane {
            DynamicReadLane::Public => Some(QueryAdmissionPolicy::default_bounded_read()),
            DynamicReadLane::Trusted => None,
        };
        let (payload, _) = self.execute_structural_projection_from_query(
            query,
            authority,
            catalog.snapshot(),
            public_admission.as_ref(),
        )?;
        let (columns, _fixed_scales, rows, row_count) = payload.into_output_components()?;

        Ok(RowProjectionOutput {
            entity: catalog.snapshot().entity_name().to_string(),
            columns,
            rows,
            row_count,
        })
    }

    fn execute_dynamic_query(
        &self,
        request: &DynamicQuery,
        lane: DynamicReadLane,
    ) -> Result<RowProjectionOutput, QueryError> {
        if request.entity().is_empty() {
            return Err(QueryError::execute(
                InternalError::query_invalid_logical_plan(),
            ));
        }

        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(request.entity()))
            .map_err(QueryError::execute)?;
        self.execute_dynamic_query_against_catalog(request, lane, catalog)
    }

    /// Execute one ordinary entity-name-driven dynamic read.
    ///
    /// The selected accepted plan must satisfy the built-in bounded public-read
    /// policy before any row is executed.
    pub fn execute_public_dynamic_query(
        &self,
        request: &DynamicQuery,
    ) -> Result<RowProjectionOutput, QueryError> {
        self.execute_dynamic_query(request, DynamicReadLane::Public)
    }

    /// Execute one typed read through the binding's immutable accepted entity
    /// identity. `None` means the opaque binding is stale.
    #[doc(hidden)]
    pub fn execute_public_dynamic_query_for_typed_binding(
        &self,
        binding: &DynamicTypedEntityBinding,
        request: &DynamicQuery,
    ) -> Result<Option<RowProjectionOutput>, QueryError> {
        let Some(catalog) = self
            .current_typed_entity_binding_catalog(binding)
            .map_err(QueryError::execute)?
        else {
            return Ok(None);
        };
        self.execute_dynamic_query_against_catalog(request, DynamicReadLane::Public, catalog)
            .map(Some)
    }

    /// Execute one trusted entity-name-driven dynamic read.
    ///
    /// This uses accepted schema, planner, executor, and projection authority
    /// only. Like trusted SQL reads, callers own authorization and resource
    /// policy.
    pub fn execute_trusted_dynamic_query(
        &self,
        request: &DynamicQuery,
    ) -> Result<RowProjectionOutput, QueryError> {
        self.execute_dynamic_query(request, DynamicReadLane::Trusted)
    }
}
