#[cfg(any(test, feature = "sql"))]
use crate::db::query::plan::CoveringHybridReadExecutionPlan;
#[cfg(any(test, feature = "sql"))]
use crate::db::query::plan::covering_hybrid_projection_execution_plan_with_schema_info;
#[cfg(test)]
use crate::db::schema::{
    AcceptedRowLayoutRuntimeContract, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
    AcceptedValueCatalogHandle, compiled_schema_proposal_for_model,
};
#[cfg(test)]
use crate::entity::EntityKind;
#[cfg(test)]
use crate::model::field::FieldModel;
#[cfg(test)]
use crate::traits::Path;
use crate::{
    db::{
        access::{SemanticIndexRangeSpec, validate_access_runtime_invariants_with_schema},
        cursor::{CursorPlanError, ValidatedCursor},
        executor::{planning::route::AggregateRouteShape, terminal::RowLayout},
        index::IndexKey,
        key_taxonomy::PrimaryKeyValue,
        query::plan::{
            AccessPlannedQuery, AggregateKind, CoveringReadExecutionPlan,
            PlannedContinuationContract, covering_read_execution_plan_with_schema_info,
        },
        schema::{
            AcceptedGeneratedRowCompatibilityProof, AcceptedRowDecodeContract,
            AcceptedSchemaAuthority, SchemaInfo,
        },
    },
    error::InternalError,
    metrics::sink::record_prepared_shape_already_finalized_for_path,
    model::entity::EntityModel,
    types::EntityTag,
    value::Value,
};
use std::sync::Arc;

///
/// EntityAuthority
///
/// EntityAuthority is the canonical structural entity-identity bundle used by
/// executor runtime preparation once typed API boundaries have resolved the
/// concrete entity type.
/// It keeps model, entity-tag, and store path authority aligned while deriving
/// the entity path from the model itself so execution-core code does not pass
/// duplicated metadata independently.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct EntityAuthority {
    model: &'static EntityModel,
    row_layout: Option<RowLayout>,
    entity_tag: EntityTag,
    store_path: &'static str,
    accepted_schema_info: Option<Arc<SchemaInfo>>,
}

impl EntityAuthority {
    /// Build complete runtime authority from accepted schema contracts.
    #[must_use]
    pub(in crate::db) fn from_accepted_row_decode_contract(
        model: &'static EntityModel,
        entity_tag: EntityTag,
        store_path: &'static str,
        row_proof: AcceptedGeneratedRowCompatibilityProof,
        accepted_decode_contract: AcceptedRowDecodeContract,
        accepted_schema_info: SchemaInfo,
    ) -> Self {
        let row_layout = RowLayout::from_generated_compatible_accepted_decode_contract(
            model.path(),
            row_proof,
            accepted_decode_contract,
        );

        Self {
            model,
            row_layout: Some(row_layout),
            entity_tag,
            store_path,
            accepted_schema_info: Some(Arc::new(accepted_schema_info)),
        }
    }

    #[cfg(test)]
    const fn raw_for_test(
        model: &'static EntityModel,
        entity_tag: EntityTag,
        store_path: &'static str,
    ) -> Self {
        Self {
            model,
            row_layout: None,
            entity_tag,
            store_path,
            accepted_schema_info: None,
        }
    }

    /// Build raw generated authority from one resolved entity type for tests.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn for_generated_type_for_test<E: EntityKind>() -> Self {
        Self::raw_for_test(E::MODEL, E::ENTITY_TAG, E::Store::PATH)
    }

    /// Build production-shaped accepted authority for executor tests.
    #[cfg(test)]
    pub(in crate::db) fn for_accepted_generated_type_for_test<E: EntityKind>() -> Self {
        let proposal = compiled_schema_proposal_for_model(E::MODEL);
        let (catalog, composite_catalog) =
            crate::db::schema::build_initial_accepted_catalogs_for_tests(&[E::MODEL])
                .expect("generated model catalogs should build");
        let snapshot = proposal
            .initial_persisted_schema_snapshot_with_catalogs(&catalog, &composite_catalog)
            .expect("generated model proposal should resolve through its test catalogs");
        let accepted = AcceptedSchemaSnapshot::try_new(snapshot)
            .expect("generated model proposal should produce an accepted test schema");
        let (descriptor, row_proof) =
            AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                &accepted,
                E::MODEL,
                &catalog,
                &composite_catalog,
            )
            .expect("generated model should match its accepted test schema");
        let catalog = AcceptedValueCatalogHandle::new_for_tests(
            catalog,
            composite_catalog,
            AcceptedSchemaRevision::INITIAL,
        );
        let row_contract = descriptor.row_decode_contract(catalog.clone());
        let schema_info = SchemaInfo::from_accepted_snapshot_and_catalog_for_model(
            E::MODEL,
            &accepted,
            catalog,
            false,
        );

        Self::from_accepted_row_decode_contract(
            E::MODEL,
            E::ENTITY_TAG,
            E::Store::PATH,
            row_proof,
            row_contract,
            schema_info,
        )
    }

    /// Return authority with generated row decode attached for test-only
    /// prepared plan construction.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn with_generated_row_layout_for_test(self) -> Self {
        Self {
            row_layout: Some(RowLayout::from_model_proposal_for_test(self.model)),
            ..self
        }
    }

    /// Return authority with cursor schema facts supplied by test-only
    /// generated plan construction.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn with_cursor_schema_info_for_test(self, schema_info: SchemaInfo) -> Self {
        let authority = self.with_generated_row_layout_for_test();

        Self {
            accepted_schema_info: Some(Arc::new(schema_info)),
            ..authority
        }
    }

    /// Borrow the entity model authority.
    #[must_use]
    pub(in crate::db) const fn model(&self) -> &'static EntityModel {
        self.model
    }

    /// Borrow the authoritative generated field table for this entity.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn fields(&self) -> &'static [FieldModel] {
        self.model.fields()
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

    /// Return whether `field` is exactly the scalar primary-key field.
    ///
    /// Composite primary keys deliberately return false here because aggregate
    /// field-target shortcuts can only treat a single selected field as the
    /// whole row identity when the entity has a scalar key.
    #[must_use]
    pub(in crate::db::executor) fn is_scalar_primary_key_field(&self, field: &str) -> bool {
        self.accepted_schema_info
            .as_ref()
            .and_then(|schema| schema.scalar_primary_key_name())
            .is_some_and(|primary_key_name| primary_key_name == field)
    }

    /// Borrow structural entity-tag authority.
    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    /// Borrow structural entity-path authority.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &'static str {
        self.model.path()
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
        plan.finalize_static_execution_planning_contract_for_model_with_schema(
            self.model,
            schema_info,
        )
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

    /// Validate and decode one scalar continuation cursor through authority-owned contracts.
    pub(in crate::db::executor) fn prepare_scalar_cursor(
        &self,
        contract: &PlannedContinuationContract,
        bytes: Option<&[u8]>,
    ) -> Result<ValidatedCursor, CursorPlanError> {
        let schema_info = self.cursor_schema_info()?;

        contract.prepare_scalar_cursor(self.entity_path(), self.entity_tag, schema_info, bytes)
    }

    /// Revalidate one scalar continuation cursor through authority-owned contracts.
    pub(in crate::db::executor) fn revalidate_scalar_cursor(
        &self,
        contract: &PlannedContinuationContract,
        cursor: ValidatedCursor,
    ) -> Result<ValidatedCursor, CursorPlanError> {
        let schema_info = self.cursor_schema_info()?;

        contract.revalidate_scalar_cursor(self.entity_tag, schema_info, cursor)
    }

    fn cursor_schema_info(&self) -> Result<&SchemaInfo, CursorPlanError> {
        self.accepted_schema_info
            .as_ref()
            .ok_or_else(CursorPlanError::continuation_cursor_invariant)
            .map(AsRef::as_ref)
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

    /// Build one structural index key from already-materialized row slots
    /// without cloning field values back out of the row cache first.
    pub(in crate::db::executor) fn index_range_anchor_key_from_slot_ref_reader<'a>(
        &self,
        primary_key: &PrimaryKeyValue,
        index_range: &SemanticIndexRangeSpec,
        read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
    ) -> Result<Option<IndexKey>, InternalError> {
        let schema_info = self.index_key_schema_info()?;
        let index = index_range.index();

        if index.has_expression_key_items() {
            return IndexKey::new_from_slot_ref_reader_with_access_contract(
                self.entity_tag,
                primary_key,
                schema_info,
                index,
                read_slot,
            );
        }

        let accepted_index = schema_info
            .field_path_indexes()
            .iter()
            .find(|accepted| accepted.name() == index.name())
            .ok_or_else(InternalError::query_executor_invariant)?;

        IndexKey::new_from_slot_ref_reader_with_accepted_field_path_index(
            self.entity_tag,
            primary_key,
            accepted_index,
            read_slot,
        )
    }

    fn index_key_schema_info(&self) -> Result<&SchemaInfo, InternalError> {
        self.accepted_schema_info
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)
            .map(AsRef::as_ref)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            executor::EntityAuthority,
            predicate::MissingRowPolicy,
            query::plan::{
                AccessPlannedQuery,
                expr::{FieldId as ExprFieldId, ProjectionSelection},
            },
            schema::{
                AcceptedFieldKind, AcceptedSchemaSnapshot, FieldId, PersistedFieldSnapshot,
                PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaInfo,
                SchemaRowLayout, SchemaVersion,
            },
        },
        metrics::{metrics_report, metrics_reset_all},
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
            index::IndexModel,
        },
        testing::entity_model_from_static,
        types::EntityTag,
    };

    const AUTHORITY_SCHEMA_SLOT_TEST_STORE_PATH: &str = "authority_schema_slot_test_store";

    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated(
            "profile",
            FieldKind::empty_test_composite("executor::authority::tests::Profile"),
        ),
    ];
    static INDEXES: [&IndexModel; 0] = [];
    static MODEL: EntityModel = entity_model_from_static(
        "executor::authority::tests::Entity",
        "Entity",
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    // Build one accepted schema with a deliberately divergent row-layout slot
    // for `profile` so this test catches generated-fallback re-finalization.
    fn accepted_schema_with_profile_slot(slot: SchemaFieldSlot) -> SchemaInfo {
        let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "executor::authority::tests::Entity".to_string(),
            "Entity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), slot),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    AcceptedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Structural,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "profile".to_string(),
                    SchemaFieldSlot::new(1),
                    AcceptedFieldKind::test_composite(),
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::CatalogValue,
                    LeafCodec::Structural,
                ),
            ],
        ));

        SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &snapshot)
    }

    #[test]
    fn authority_finalization_preserves_schema_finalized_static_contract() {
        metrics_reset_all();
        let authority = EntityAuthority::raw_for_test(
            &MODEL,
            EntityTag::new(0x1460_0013),
            AUTHORITY_SCHEMA_SLOT_TEST_STORE_PATH,
        );
        let schema = accepted_schema_with_profile_slot(SchemaFieldSlot::new(7));
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.projection_selection = ProjectionSelection::Fields(vec![ExprFieldId::new("profile")]);

        plan.finalize_static_execution_planning_contract_for_model_with_schema(&MODEL, &schema)
            .expect("schema-finalized static shape should build");
        assert_eq!(plan.frozen_direct_projection_slots(), Some([7].as_slice()));

        authority
            .finalize_static_execution_planning_contract(&mut plan)
            .expect("authority finalization should preserve finalized static contract");

        assert_eq!(plan.frozen_direct_projection_slots(), Some([7].as_slice()));

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("authority finalization should record metrics");
        assert_eq!(counters.ops().prepared_shape_already_finalized(), 1);
    }

    #[test]
    fn authority_finalization_uses_authority_schema_when_shape_is_missing() {
        metrics_reset_all();
        let authority = EntityAuthority::raw_for_test(
            &MODEL,
            EntityTag::new(0x1460_0014),
            AUTHORITY_SCHEMA_SLOT_TEST_STORE_PATH,
        )
        .with_cursor_schema_info_for_test(accepted_schema_with_profile_slot(SchemaFieldSlot::new(
            7,
        )));
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.projection_selection = ProjectionSelection::Fields(vec![ExprFieldId::new("profile")]);

        authority
            .finalize_static_execution_planning_contract(&mut plan)
            .expect("authority finalization should use accepted schema layout");

        assert_eq!(plan.frozen_direct_projection_slots(), Some([7].as_slice()));
    }
}
