//! Module: db::session::query::exact_count
//! Responsibility: fail-closed exact-cardinality planning and public query adaptation.
//! Does not own: cardinality maintenance, accepted schema, or row execution.
//! Boundary: admits only whole-entity or bounded strict indexed-prefix metadata proofs.

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicTypedEntityBinding, QueryError,
        access::{
            MAX_INDEX_BRANCH_SET_VALUES,
            lower_exact_user_index_prefix_cardinality_keys_for_prefix_access,
        },
        executor::{
            EntityAuthority, ExactCardinalityTarget, execute_exact_cardinality_for_canister,
        },
        index::UserIndexPrefixCardinalityKey,
        query::{
            expr::{CompareOperator, FilterExpr, SetOperator},
            intent::StructuralQuery,
            plan::VisibleIndexes,
        },
        schema::SchemaInfo,
        session::AcceptedSchemaCatalogContext,
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

enum ExactCountPlan {
    Entity,
    UserIndexPrefixes(Vec<UserIndexPrefixCardinalityKey>),
}

pub(in crate::db::session) fn exact_count_cardinality_prefix_keys_for_accepted_authority(
    authority: &EntityAuthority,
    query: &StructuralQuery,
    visible_indexes: &VisibleIndexes,
    schema_info: &SchemaInfo,
) -> Result<Option<Vec<UserIndexPrefixCardinalityKey>>, QueryError> {
    let Some(access) = query
        .try_build_count_cardinality_prefix_access_with_schema_info(visible_indexes, schema_info)?
    else {
        return Ok(None);
    };
    let prefix_keys = lower_exact_user_index_prefix_cardinality_keys_for_prefix_access(
        authority.entity_tag(),
        &access,
        schema_info,
    )
    .map_err(|_err| QueryError::invariant())?;

    Ok((!prefix_keys.is_empty()).then_some(prefix_keys))
}

impl<C: CanisterKind> DbSession<C> {
    fn exact_count_request_is_scalar_metadata_shape(request: &DynamicQuery) -> bool {
        if !request.order_terms().is_empty()
            || !request.selected_fields().is_empty()
            || request.row_limit().is_some()
            || request.has_grouping()
            || request.grouped_execution_limits().is_some()
            || request.continuation_cursor().is_some()
        {
            return false;
        }
        #[cfg(test)]
        if request.projection_is_distinct() {
            return false;
        }

        true
    }

    fn exact_count_plan_against_catalog(
        &self,
        request: &DynamicQuery,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ExactCountPlan, QueryError> {
        if !Self::exact_count_request_is_scalar_metadata_shape(request) {
            return Err(QueryError::unsupported_query());
        }
        match request.filter_expr() {
            None => return Ok(ExactCountPlan::Entity),
            Some(FilterExpr::Compare {
                operator: CompareOperator::Eq,
                ..
            }) => {}
            Some(FilterExpr::Set {
                operator: SetOperator::In,
                values,
                ..
            }) if !values.is_empty() && values.len() <= MAX_INDEX_BRANCH_SET_VALUES => {}
            Some(_) => return Err(QueryError::unsupported_query()),
        }

        let query = Self::structural_query_from_dynamic_request(request, catalog)?;
        let schema_info = catalog.accepted_schema_info();
        let authority = catalog.accepted_entity_authority();
        let visible_indexes =
            self.visible_indexes_for_store_accepted_schema(authority.store_path(), schema_info)?;
        let prefix_keys = exact_count_cardinality_prefix_keys_for_accepted_authority(
            &authority,
            &query,
            &visible_indexes,
            schema_info,
        )?
        .ok_or_else(QueryError::unsupported_query)?;

        Ok(ExactCountPlan::UserIndexPrefixes(prefix_keys))
    }

    fn execute_exact_count_against_catalog(
        &self,
        request: &DynamicQuery,
        catalog: AcceptedSchemaCatalogContext,
    ) -> Result<u64, QueryError> {
        let plan = self.exact_count_plan_against_catalog(request, &catalog)?;
        let authority = catalog.accepted_entity_authority();
        let target = match &plan {
            ExactCountPlan::Entity => ExactCardinalityTarget::Entity,
            ExactCountPlan::UserIndexPrefixes(prefix_keys) => {
                ExactCardinalityTarget::UserIndexPrefixes(prefix_keys)
            }
        };
        self.with_metrics(|| {
            execute_exact_cardinality_for_canister(
                &self.db,
                authority,
                DiagnosticExecutionLane::PublicRead,
                target,
            )
        })
        .map_err(QueryError::execute)?
        .ok_or_else(QueryError::unsupported_query)
    }

    /// Return exact visible cardinality without entering row execution.
    ///
    /// The request must be a bare entity count or one strict equality/`IN`
    /// filter over the leading field of an accepted unfiltered field-path user
    /// index. The index may contain additional trailing fields. Metadata that
    /// is not ready fails closed; this terminal never falls back to a scan.
    pub fn execute_public_exact_count(&self, request: &DynamicQuery) -> Result<u64, QueryError> {
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(request.entity()))
            .map_err(QueryError::execute)?;
        self.execute_exact_count_against_catalog(request, catalog)
    }

    /// Execute one exact count through a typed binding's immutable accepted
    /// entity identity. `None` means the opaque binding is stale.
    #[doc(hidden)]
    pub fn execute_public_exact_count_for_typed_binding(
        &self,
        binding: &DynamicTypedEntityBinding,
        request: &DynamicQuery,
    ) -> Result<Option<u64>, QueryError> {
        let Some(catalog) = self
            .current_typed_entity_binding_catalog(binding)
            .map_err(QueryError::execute)?
        else {
            return Ok(None);
        };
        self.execute_exact_count_against_catalog(request, catalog)
            .map(Some)
    }
}
