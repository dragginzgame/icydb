use crate::{
    db::{
        access::{AccessPlanError, validate_access_structure_model},
        cursor::{CursorPlanError, PlannedCursor},
        data::StorageKey,
        executor::terminal::RowLayout,
        index::IndexKey,
        query::plan::{
            AccessPlannedQuery, CoveringReadExecutionPlan, CoveringReadPlan,
            PlannedContinuationContract, covering_hybrid_projection_plan_from_fields,
            covering_read_execution_plan_from_fields,
        },
        schema::{AcceptedGeneratedCompatibleRowShape, AcceptedRowDecodeContract, SchemaInfo},
    },
    error::InternalError,
    metrics::sink::{
        PreparedShapeFinalizationOutcome, record_prepared_shape_finalization_for_path,
    },
    model::{entity::EntityModel, field::FieldModel, index::IndexModel},
    traits::{EntityKind, Path},
    types::EntityTag,
    value::Value,
};

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
pub struct EntityAuthority {
    model: &'static EntityModel,
    row_layout: RowLayout,
    primary_key_name: &'static str,
    entity_tag: EntityTag,
    store_path: &'static str,
}

impl EntityAuthority {
    /// Build authority from explicit runtime metadata.
    #[must_use]
    pub const fn new(
        model: &'static EntityModel,
        entity_tag: EntityTag,
        store_path: &'static str,
    ) -> Self {
        Self {
            model,
            row_layout: RowLayout::from_model(model),
            primary_key_name: model.primary_key.name,
            entity_tag,
            store_path,
        }
    }

    /// Build authority from one resolved entity type.
    #[must_use]
    pub const fn for_type<E: EntityKind>() -> Self {
        Self::new(E::MODEL, E::ENTITY_TAG, E::Store::PATH)
    }

    /// Return authority with row decode frozen from accepted schema field contracts.
    ///
    /// The generated-compatible proof remains an explicit input so callers
    /// cannot attach accepted decode contracts to layouts that the current
    /// generated write/materialization bridge cannot still handle.
    #[must_use]
    pub(in crate::db) fn with_accepted_row_decode_contract(
        self,
        row_shape: AcceptedGeneratedCompatibleRowShape,
        accepted_decode_contract: AcceptedRowDecodeContract,
    ) -> Self {
        let _ = row_shape;
        let row_layout =
            RowLayout::from_accepted_decode_contract(self.model, accepted_decode_contract);

        Self { row_layout, ..self }
    }

    /// Borrow the entity model authority.
    #[must_use]
    pub const fn model(&self) -> &'static EntityModel {
        self.model
    }

    /// Borrow the cached schema authority for this entity.
    #[must_use]
    pub(in crate::db::executor) fn schema_info(&self) -> &'static SchemaInfo {
        SchemaInfo::cached_for_entity_model(self.model)
    }

    /// Borrow the authoritative generated field table for this entity.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &'static [FieldModel] {
        self.model.fields()
    }

    /// Borrow the frozen structural row-decode layout for this entity.
    #[must_use]
    pub(in crate::db::executor) fn row_layout(&self) -> RowLayout {
        self.row_layout.clone()
    }

    /// Borrow the frozen structural row-decode layout for metadata-only callers.
    #[must_use]
    pub(in crate::db::executor) const fn row_layout_ref(&self) -> &RowLayout {
        &self.row_layout
    }

    /// Borrow the frozen structural primary-key field name for this entity.
    #[must_use]
    pub const fn primary_key_name(&self) -> &'static str {
        self.primary_key_name
    }

    /// Borrow structural entity-tag authority.
    #[must_use]
    pub const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    /// Borrow structural entity-path authority.
    #[must_use]
    pub const fn entity_path(&self) -> &'static str {
        self.model.path()
    }

    /// Borrow structural store-path authority.
    #[must_use]
    pub const fn store_path(&self) -> &'static str {
        self.store_path
    }

    /// Finalize planner-owned static execution shape through canonical entity authority.
    pub(in crate::db::executor) fn finalize_static_planning_shape(
        &self,
        plan: &mut AccessPlannedQuery,
    ) {
        // Cached/session planning may already have frozen static execution
        // metadata with accepted schema authority. Do not overwrite that
        // schema-selected slot contract with the generated fallback while
        // lowering the executor core.
        if plan.has_static_planning_shape() {
            record_prepared_shape_finalization_for_path(
                self.entity_path(),
                PreparedShapeFinalizationOutcome::AlreadyFinalized,
            );
            return;
        }

        plan.finalize_static_planning_shape_for_model(self.model)
            .expect("executable plan core requires planner-frozen static execution shape");
        record_prepared_shape_finalization_for_path(
            self.entity_path(),
            PreparedShapeFinalizationOutcome::GeneratedFallback,
        );
    }

    /// Finalize planner-owned route profiling through canonical entity authority.
    pub(in crate::db::executor) fn finalize_planner_route_profile(
        &self,
        plan: &mut AccessPlannedQuery,
    ) {
        plan.finalize_planner_route_profile_for_model(self.model);
    }

    /// Validate one access-planned query against authority-owned structural contracts.
    pub(in crate::db::executor) fn validate_executor_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Result<(), InternalError> {
        validate_access_structure_model(self.schema_info(), self.model, &plan.access)
            .map_err(AccessPlanError::into_internal_error)
    }

    /// Validate and decode one scalar continuation cursor through authority-owned contracts.
    pub(in crate::db::executor) fn prepare_scalar_cursor(
        &self,
        contract: &PlannedContinuationContract,
        bytes: Option<&[u8]>,
    ) -> Result<PlannedCursor, CursorPlanError> {
        contract.prepare_scalar_cursor(self.entity_path(), self.entity_tag, self.model, bytes)
    }

    /// Revalidate one scalar continuation cursor through authority-owned contracts.
    pub(in crate::db::executor) fn revalidate_scalar_cursor(
        &self,
        contract: &PlannedContinuationContract,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, CursorPlanError> {
        contract.revalidate_scalar_cursor(self.entity_tag, self.model, cursor)
    }

    /// Derive one covering-read execution contract through authority-owned schema metadata.
    #[must_use]
    pub(in crate::db::executor) fn covering_read_execution_plan(
        &self,
        plan: &AccessPlannedQuery,
        strict_predicate_compatible: bool,
    ) -> Option<CoveringReadExecutionPlan> {
        covering_read_execution_plan_from_fields(
            self.fields(),
            plan,
            self.primary_key_name,
            strict_predicate_compatible,
        )
    }

    /// Derive one hybrid covering projection contract through authority-owned schema metadata.
    #[must_use]
    pub(in crate::db::executor) fn covering_hybrid_projection_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Option<CoveringReadPlan> {
        covering_hybrid_projection_plan_from_fields(self.fields(), plan, self.primary_key_name)
    }

    /// Build one structural index key from already-materialized row slots
    /// without cloning field values back out of the row cache first.
    pub(in crate::db::executor) fn index_key_from_slot_ref_reader<'a>(
        &self,
        storage_key: StorageKey,
        index: &IndexModel,
        read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
    ) -> Result<Option<IndexKey>, InternalError> {
        IndexKey::new_from_slot_ref_reader(
            self.entity_tag,
            storage_key,
            self.model,
            index,
            read_slot,
        )
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
                AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
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
        FieldModel::generated("profile", FieldKind::Structured { queryable: true }),
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
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "profile".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Structured { queryable: true },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::Value,
                    LeafCodec::StructuralFallback,
                ),
            ],
        ));

        SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot)
    }

    #[test]
    fn authority_finalization_preserves_schema_finalized_static_shape() {
        metrics_reset_all();
        let authority = EntityAuthority::new(
            &MODEL,
            EntityTag::new(0x1460_0013),
            AUTHORITY_SCHEMA_SLOT_TEST_STORE_PATH,
        );
        let schema = accepted_schema_with_profile_slot(SchemaFieldSlot::new(7));
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.projection_selection = ProjectionSelection::Fields(vec![ExprFieldId::new("profile")]);

        plan.finalize_static_planning_shape_for_model_with_schema(&MODEL, &schema)
            .expect("schema-finalized static shape should build");
        assert_eq!(plan.frozen_direct_projection_slots(), Some([7].as_slice()));

        authority.finalize_static_planning_shape(&mut plan);

        assert_eq!(plan.frozen_direct_projection_slots(), Some([7].as_slice()));

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("authority finalization should record metrics");
        assert_eq!(counters.ops().prepared_shape_already_finalized(), 1);
        assert_eq!(counters.ops().prepared_shape_generated_fallback(), 0);
    }

    #[test]
    fn authority_finalization_records_generated_fallback_shape() {
        metrics_reset_all();
        let authority = EntityAuthority::new(
            &MODEL,
            EntityTag::new(0x1460_0014),
            AUTHORITY_SCHEMA_SLOT_TEST_STORE_PATH,
        );
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.projection_selection = ProjectionSelection::Fields(vec![ExprFieldId::new("profile")]);

        authority.finalize_static_planning_shape(&mut plan);

        assert_eq!(plan.frozen_direct_projection_slots(), Some([1].as_slice()));

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("authority finalization should record metrics");
        assert_eq!(counters.ops().prepared_shape_already_finalized(), 0);
        assert_eq!(counters.ops().prepared_shape_generated_fallback(), 1);
    }
}
