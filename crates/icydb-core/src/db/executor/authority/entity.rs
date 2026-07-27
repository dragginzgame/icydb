#[cfg(any(test, feature = "sql"))]
use crate::db::query::plan::CoveringHybridReadExecutionPlan;
#[cfg(any(test, feature = "sql"))]
use crate::db::query::plan::covering_hybrid_projection_execution_plan_with_schema_info;
use crate::{
    db::{
        access::validate_access_runtime_invariants_with_schema,
        executor::{planning::route::AggregateRouteShape, terminal::RowLayout},
        query::plan::{
            AccessPlannedQuery, AggregateKind, CoveringReadExecutionPlan,
            covering_read_execution_plan_with_schema_info,
        },
        schema::{AcceptedRowDecodeContract, AcceptedSchemaAuthority, SchemaInfo},
    },
    error::InternalError,
    metrics::sink::record_prepared_shape_already_finalized_for_path,
    types::EntityTag,
};
use std::sync::Arc;

///
/// EntityAuthority
///
/// EntityAuthority is the canonical structural entity-identity bundle used by
/// executor runtime preparation once a session has resolved accepted schema
/// authority. It deliberately carries no generated application model.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct EntityAuthority {
    entity_path: &'static str,
    row_layout: Option<RowLayout>,
    entity_tag: EntityTag,
    store_path: &'static str,
    accepted_schema_info: Option<Arc<SchemaInfo>>,
}

impl EntityAuthority {
    /// Build complete runtime authority from accepted schema contracts.
    #[must_use]
    pub(in crate::db) fn from_accepted_row_decode_contract(
        entity_path: &'static str,
        entity_tag: EntityTag,
        store_path: &'static str,
        accepted_decode_contract: AcceptedRowDecodeContract,
        accepted_schema_info: SchemaInfo,
    ) -> Self {
        let row_layout =
            RowLayout::from_accepted_decode_contract(entity_path, accepted_decode_contract);

        Self {
            entity_path,
            row_layout: Some(row_layout),
            entity_tag,
            store_path,
            accepted_schema_info: Some(Arc::new(accepted_schema_info)),
        }
    }

    /// Borrow the frozen structural row-decode layout for this entity.
    pub(in crate::db::executor) fn row_layout(&self) -> Result<RowLayout, InternalError> {
        Ok(self.row_layout_ref()?.clone())
    }

    /// Borrow the frozen structural row-decode layout for metadata-only callers.
    pub(in crate::db::executor) fn row_layout_ref(&self) -> Result<&RowLayout, InternalError> {
        self.row_layout
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)
    }

    /// Borrow the immutable store/revision authority that admitted this
    /// executor's accepted row layout.
    pub(in crate::db) fn accepted_schema_authority(
        &self,
    ) -> Result<&AcceptedSchemaAuthority, InternalError> {
        Ok(self.accepted_value_catalog_handle()?.authority())
    }

    /// Borrow the immutable accepted catalog handle frozen into this
    /// executor's row layout.
    pub(in crate::db) fn accepted_value_catalog_handle(
        &self,
    ) -> Result<&crate::db::schema::AcceptedValueCatalogHandle, InternalError> {
        Ok(self
            .row_layout_ref()?
            .contract()
            .accepted_value_catalog_handle())
    }

    /// Borrow the accepted schema view attached to this executor authority.
    #[must_use]
    pub(in crate::db) fn accepted_schema_info(&self) -> Option<&SchemaInfo> {
        self.accepted_schema_info
            .as_ref()
            .map(std::convert::AsRef::as_ref)
    }

    /// Borrow structural entity-tag authority.
    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    /// Borrow structural entity-path authority.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    /// Borrow structural store-path authority.
    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &'static str {
        self.store_path
    }

    /// Finalize planner-owned static execution contract through canonical entity authority.
    pub(in crate::db::executor) fn finalize_static_execution_planning_contract(
        &self,
        plan: &mut AccessPlannedQuery,
    ) -> Result<(), InternalError> {
        // Cached/session planning may already have frozen static execution
        // metadata with accepted schema authority. Do not overwrite that
        // schema-selected slot contract while lowering the executor core.
        if plan.has_static_execution_planning_contract() {
            record_prepared_shape_already_finalized_for_path(self.entity_path());
            return Ok(());
        }

        let schema_info = self
            .accepted_schema_info
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)?;
        plan.finalize_static_execution_planning_contract_with_schema(schema_info)
            .map_err(|_err| InternalError::query_executor_invariant())?;

        Ok(())
    }

    /// Finalize planner-owned route profiling through canonical entity authority.
    pub(in crate::db::executor) fn finalize_planner_route_profile(
        &self,
        plan: &mut AccessPlannedQuery,
    ) -> Result<(), InternalError> {
        let schema_info = self
            .accepted_schema_info
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)?;
        plan.finalize_planner_route_profile_for_model_with_schema(schema_info);

        Ok(())
    }

    /// Validate one access-planned query against authority-owned structural contracts.
    pub(in crate::db::executor) fn validate_executor_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Result<(), InternalError> {
        if !plan.has_static_execution_planning_contract() {
            return Err(InternalError::query_executor_invariant());
        }

        let schema_info = self
            .accepted_schema_info
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)?;

        validate_access_runtime_invariants_with_schema(schema_info.as_ref(), &plan.access)
            .map_err(crate::db::access::AccessPlanError::into_internal_error)
    }

    /// Resolve one aggregate route shape through authority-owned schema metadata.
    pub(in crate::db) fn aggregate_route_shape<'a>(
        &self,
        kind: AggregateKind,
        target_field: Option<&'a str>,
    ) -> Result<AggregateRouteShape<'a>, InternalError> {
        let schema_info = self
            .accepted_schema_info
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)?;

        Ok(AggregateRouteShape::new_from_schema_info(
            kind,
            target_field,
            schema_info,
        ))
    }

    /// Derive one covering-read execution contract through authority-owned schema metadata.
    #[must_use]
    pub(in crate::db::executor) fn covering_read_execution_plan(
        &self,
        plan: &AccessPlannedQuery,
        strict_predicate_compatible: bool,
    ) -> Option<CoveringReadExecutionPlan> {
        let schema_info = self.accepted_schema_info.as_ref()?;

        covering_read_execution_plan_with_schema_info(
            schema_info,
            plan,
            strict_predicate_compatible,
        )
    }

    /// Derive one hybrid covering projection contract through authority-owned schema metadata.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::executor) fn covering_hybrid_projection_plan(
        &self,
        plan: &AccessPlannedQuery,
        strict_predicate_compatible: bool,
    ) -> Option<CoveringHybridReadExecutionPlan> {
        let schema_info = self.accepted_schema_info.as_ref()?;

        covering_hybrid_projection_execution_plan_with_schema_info(
            schema_info,
            plan,
            strict_predicate_compatible,
        )
    }
}
