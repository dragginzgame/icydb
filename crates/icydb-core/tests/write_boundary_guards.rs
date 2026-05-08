use std::{fs, path::PathBuf};

fn read_source(relative_path: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);

    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

fn rust_sources_under(relative_path: &str) -> Vec<PathBuf> {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.push(relative_path);

    let mut sources = Vec::new();
    let mut pending = vec![root];
    while let Some(path) = pending.pop() {
        let entries = fs::read_dir(&path)
            .unwrap_or_else(|err| panic!("failed to list {}: {err}", path.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|err| panic!("failed to read directory entry: {err}"))
                .path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }

    sources.sort();
    sources
}

fn compact_source(source: &str) -> String {
    source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

#[test]
fn data_store_insert_stays_canonical_row_only() {
    let source = read_source("src/db/data/store.rs");

    assert!(
        source.contains("pub(in crate::db) fn insert(&mut self, key: RawDataKey, row: CanonicalRow) -> Option<RawRow>"),
        "DataStore::insert must remain CanonicalRow-only at the production write boundary",
    );
    assert!(
        !source.contains("pub fn insert(&mut self, key: RawDataKey, row: RawRow)"),
        "DataStore::insert must not accept RawRow in production code",
    );
}

#[test]
fn prepared_row_write_payloads_stay_canonical() {
    let prepared_op = read_source("src/db/commit/prepared_op.rs");
    let typed_save = read_source("src/db/executor/mutation/save/typed.rs");
    let structural_save = read_source("src/db/executor/mutation/save/structural.rs");

    assert!(
        prepared_op.contains("pub(crate) data_value: Option<CanonicalRow>,"),
        "prepared row commit ops must carry CanonicalRow after-images",
    );
    assert!(
        !prepared_op.contains("pub(crate) data_value: Option<RawRow>,"),
        "prepared row commit ops must not regress to RawRow after-images",
    );
    assert!(
        typed_save.contains("canonical_row_from_entity_with_accepted_contract(")
            && !typed_save.contains("CanonicalRow::from_entity(entity)?"),
        "typed save after-image construction must use accepted-contract row emission",
    );
    assert!(
        structural_save
            .contains("fn build_structural_update_after_image_row_with_accepted_contract(")
            && structural_save.contains("accepted_row_decode_contract: AcceptedRowDecodeContract,")
            && structural_save.contains(") -> Result<CanonicalRow, InternalError>"),
        "structural update after-image builder must stay accepted-contract aware and return CanonicalRow",
    );
    assert!(
        structural_save
            .contains("fn build_normalized_structural_after_image_row_with_accepted_contract(")
            && structural_save.contains("canonical_row_from_entity_with_accepted_contract(")
            && !structural_save.contains("MutationInput::from_entity("),
        "normalized structural save after-image builder must use accepted-contract row emission",
    );
    assert!(
        structural_save
            .contains("materialize_entity_from_serialized_structural_patch_with_accepted_contract")
            && !structural_save.contains("materialize_entity_from_serialized_structural_patch::<"),
        "structural insert/replace materialization must use accepted-contract decode authority",
    );
}

#[test]
fn accepted_storage_row_contracts_do_not_retain_generated_field_bridge() {
    let structural_row = read_source("src/db/data/structural_row.rs");
    let structural_row_compact = compact_source(&structural_row);
    let row_reader = read_source("src/db/data/persisted_row/reader/core.rs");
    let row_reader_compact = compact_source(&row_reader);
    let persisted_patch = read_source("src/db/data/persisted_row/patch.rs");
    let persisted_patch_compact = compact_source(&persisted_patch);
    let primary_key_reader = read_source("src/db/data/persisted_row/reader/primary_key.rs");
    let primary_key_reader_compact = compact_source(&primary_key_reader);
    let reverse_index = read_source("src/db/relation/reverse_index.rs");
    let reverse_index_compact = compact_source(&reverse_index);
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let save_validation_compact = compact_source(&save_validation);
    let row_decode = read_source("src/db/executor/terminal/row_decode/mod.rs");

    assert!(
        structural_row.contains("pub(in crate::db) fn from_accepted_schema_snapshot(")
            && structural_row.contains("Self::from_accepted_decode_contract(")
            && !structural_row.contains("fn from_model_with_accepted_schema_snapshot(")
            && !structural_row.contains(
                "Ok(Self::from_model_with_accepted_decode_contract(\n            model,\n            descriptor.row_decode_contract(),\n        ))",
        ),
        "storage row readers must use accepted-only row contracts after the generated-compatibility proof",
    );
    assert!(
        row_decode
            .contains("pub(in crate::db) fn from_generated_compatible_accepted_decode_contract(")
            && row_decode.contains("StructuralRowContract::from_accepted_decode_contract(")
            && !row_decode
                .contains("StructuralRowContract::from_model_with_accepted_decode_contract("),
        "accepted executor row layouts must not retain the generated field bridge after compatibility proof",
    );
    assert!(
        persisted_patch.contains("StructuralRowContract::from_accepted_decode_contract(")
            && persisted_patch
                .contains("Self::validate_payload_slot(&contract, generated_fields, slot)?")
            && !persisted_patch
                .contains("StructuralRowContract::from_model_with_accepted_decode_contract("),
        "accepted structural patch materialization must validate payload slots through accepted row contracts without retaining the generated field bridge",
    );
    assert!(
        structural_row_compact.contains("required_accepted_field_decode_contract(&self,slot:usize")
            && structural_row_compact
                .contains(".required_field_for_slot(self.entity_path(),slot)?")
            && !structural_row_compact.contains("fnaccepted_field_decode_contract(")
            && structural_row_compact.contains(
                "ifself.accepted_decode_contract.is_some(){letfield=self.required_accepted_field_decode_contract(slot)?;returnOk(field.leaf_codec());}",
            )
            && structural_row_compact.contains(
                "ifself.accepted_decode_contract.is_some(){letfield=self.required_accepted_field_decode_contract(slot)?;returnOk(field.field_name());}",
            ),
        "accepted structural row field-name and leaf-codec lookups must fail closed instead of falling back to generated field metadata",
    );
    assert!(
        row_reader_compact.contains("fnrequired_accepted_field_decode_contract(&self,slot:usize")
            && row_reader_compact.contains(
                "self.contract.required_accepted_field_decode_contract(slot)",
            )
            && row_reader_compact.contains(
                "letaccepted_field=self.required_accepted_field_decode_contract(slot)?;ifletSome(value)=self.required_accepted_value_storage_scalar(slot,accepted_field)?",
            )
            && !row_reader_compact.contains("pub(incrate::db)fnaccepted_field_decode_contract")
            && !row_reader_compact.contains(
                "ifletSome(accepted_field)=self.contract.accepted_field_decode_contract(slot)",
            ),
        "accepted structural row reader scalar fast paths must fail closed instead of treating missing accepted field metadata as a non-fast-path value",
    );
    assert!(
        primary_key_reader_compact.contains("ifcontract.has_accepted_decode_contract(){")
            && primary_key_reader_compact
                .contains("contract.required_accepted_field_decode_contract(primary_key_slot)?")
            && !primary_key_reader_compact
                .contains(".accepted_field_decode_contract(primary_key_slot).ok_or_else(")
            && !primary_key_reader_compact
                .contains("ifletSome(primary_key_field)=contract.accepted_field_decode_contract(primary_key_slot)"),
        "accepted primary-key validation must fail closed instead of falling back to generated field metadata",
    );
    assert!(
        persisted_patch_compact.contains("contract.required_accepted_field_decode_contract(slot)?")
            && persisted_patch_compact
                .contains("contract.required_accepted_field_decode_contract(slot.index())?"),
        "accepted structural patch payload handling must use the shared required accepted-field contract",
    );
    assert!(
        reverse_index_compact
            .contains("source_row_contract.required_accepted_field_decode_contract(slot)?")
            && save_validation_compact
                .contains("contract.required_accepted_field_decode_contract(slot)?")
            && save_validation_compact
                .contains(".required_accepted_field_decode_contract(field_index)?"),
        "relation and save validation accepted field scans must use the shared required accepted-field contract",
    );
}

#[test]
fn commit_and_delete_relation_row_contracts_use_accepted_snapshots() {
    let structural_row = read_source("src/db/data/structural_row.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let relation_validate = read_source("src/db/relation/validate.rs");

    assert!(
        structural_row.contains("pub(in crate::db) fn from_accepted_schema_snapshot(")
            && !structural_row.contains("fn from_model_with_accepted_schema_snapshot("),
        "structural row contracts must expose accepted-snapshot construction without retaining the generated-compatible snapshot constructor",
    );
    assert!(
        commit_prepare.contains(
            "StructuralRowContract::from_accepted_schema_snapshot(authority.entity_path, &accepted)",
        ) && relation_validate
            .contains("StructuralRowContract::from_accepted_schema_snapshot(S::PATH, &accepted)")
            && !commit_prepare
                .contains("StructuralRowContract::from_model_with_accepted_schema_snapshot")
            && !relation_validate
                .contains("StructuralRowContract::from_model_with_accepted_schema_snapshot"),
        "commit preflight and delete relation validation must build accepted-only row contracts after schema acceptance",
    );
}

#[test]
fn accepted_row_decode_contract_runtime_lookups_fail_closed() {
    let schema_runtime = read_source("src/db/schema/runtime.rs");
    let schema_runtime_compact = compact_source(&schema_runtime);
    let relation_save_validate = read_source("src/db/relation/save_validate.rs");
    let relation_save_validate_compact = compact_source(&relation_save_validate);
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let save_validation_compact = compact_source(&save_validation);

    assert!(
        schema_runtime_compact.contains("pub(incrate::db)fnrequired_field_for_slot(")
            && schema_runtime_compact.contains(
                "InternalError::persisted_row_slot_lookup_out_of_bounds(entity_path,slot)",
            ),
        "accepted row-decode contracts must expose a required slot lookup for accepted runtime authority",
    );
    assert!(
        save_validation_compact
            .contains("accepted_contract.required_field_for_slot(E::PATH,slot)?")
            && save_validation_compact
                .contains("accepted_contract.required_field_for_slot(E::PATH,primary_key_slot)?")
            && save_validation_compact
                .contains("accepted_contract.required_field_for_slot(E::PATH,field_index)?")
            && relation_save_validate_compact
                .contains("accepted_row_decode_contract.required_field_for_slot(E::PATH,slot)?")
            && !save_validation_compact.contains(".field_for_slot(primary_key_slot).ok_or_else(")
            && !save_validation_compact.contains(".field_for_slot(field_index).ok_or_else("),
        "accepted typed-save validation must use required accepted row-decode field lookup",
    );
}

#[test]
fn save_preflight_relations_use_accepted_contracts() {
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let relation_save_validate = read_source("src/db/relation/save_validate.rs");

    assert!(
        save_validation.contains("validate_save_strong_relations_with_accepted_contract::<E>(")
            && save_validation.contains("self.accepted_row_decode_contract(),")
            && !save_validation.contains("validate_save_strong_relations::<E>(&self.db, entity)?"),
        "save relation preflight must use accepted row contracts instead of reopening E::MODEL relation metadata",
    );
    assert!(
        relation_save_validate
            .contains("validate_save_strong_relations_with_accepted_contract<E>",)
            && !relation_save_validate.contains("strong_relations_for_model_iter")
            && !relation_save_validate.contains("E::MODEL"),
        "save relation validation must derive relation metadata from accepted row contracts",
    );
}

#[test]
fn reverse_relation_runtime_paths_use_accepted_contracts() {
    let reverse_index = read_source("src/db/relation/reverse_index.rs");
    let delete_validate = read_source("src/db/relation/validate.rs");
    let runtime_hooks = read_source("src/db/runtime_hooks/mod.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");

    assert!(
        reverse_index.contains("accepted_strong_relations_for_row_contract(")
            && reverse_index.contains("source_row_contract: StructuralRowContract,")
            && !reverse_index.contains("strong_relations_for_model_iter")
            && !reverse_index.contains("source_model: &'static EntityModel"),
        "reverse-index mutation preparation must derive relation fields from accepted row contracts",
    );
    assert!(
        delete_validate.contains("accepted_strong_relations_for_row_contract(")
            && delete_validate.contains("accepted_source_row_contract::<S>(")
            && !delete_validate.contains("model_has_strong_relations_to_target(S::MODEL"),
        "delete-side relation validation must derive relation fields from accepted row contracts",
    );
    assert!(
        !runtime_hooks.contains("model_has_strong_relations_to_target("),
        "delete hook traversal must not use generated model relation metadata as a runtime prefilter",
    );
    assert!(
        commit_prepare
            .contains("row_contract.clone(),\n        structural.data_key.storage_key(),"),
        "commit reverse-index preparation must receive the accepted structural row contract",
    );
}

#[test]
fn forward_index_write_keys_use_accepted_row_contract_slots() {
    let index_key_build = read_source("src/db/index/key/build.rs");
    let index_plan = read_source("src/db/index/plan/mod.rs");
    let unique_plan = read_source("src/db/index/plan/unique.rs");
    let structural_row = read_source("src/db/data/structural_row.rs");
    let predicate_runtime = read_source("src/db/predicate/runtime/mod.rs");

    assert!(
        structural_row.contains("pub(in crate::db) fn field_slot_index_by_name(")
            && structural_row.contains("self.accepted_decode_contract.is_some()"),
        "structural row contracts must expose accepted-first field-name to slot lookup",
    );
    assert!(
        index_key_build.contains("pub(crate) fn new_from_slots_with_contract(")
            && index_key_build.contains("row_contract.field_slot_index_by_name(field)?")
            && !index_key_build.contains("pub(crate) fn new_from_slots(\n")
            && !index_key_build.contains("compile_scalar_index_key_item_program("),
        "write-time index key construction must resolve field slots through accepted row contracts",
    );
    assert!(
        index_plan.contains("IndexKey::new_from_slots_with_contract(")
            && index_plan.contains("PredicateProgram::compile_with_row_contract(")
            && index_plan.contains("row_contract,")
            && !index_plan.contains("IndexKey::new_from_slots("),
        "forward-index mutation planning must pass accepted row contracts into index key and predicate construction",
    );
    assert!(
        unique_plan.contains("IndexKey::new_from_slots_with_contract(")
            && unique_plan.contains("row_contract,"),
        "unique-index validation must rebuild stored index keys through accepted row contracts",
    );
    assert!(
        predicate_runtime.contains("slots.field_leaf_codec(field_slot)")
            && predicate_runtime.contains("slots.required_value_storage_scalar(field_slot)")
            && !predicate_runtime.contains("slots\n        .field_decode_contract(field_slot)")
            && !predicate_runtime.contains("slots.field_decode_contract(field_slot)"),
        "conditional-index predicate fast paths must use accepted-aware scalar slot helpers",
    );
}

#[test]
fn global_distinct_grouped_runtime_keeps_prepared_authority() {
    let grouped_entrypoints = read_source("src/db/executor/pipeline/entrypoints/grouped.rs");
    let aggregate_distinct = read_source("src/db/executor/aggregate/distinct.rs");
    let aggregate_numeric = read_source("src/db/executor/aggregate/numeric/mod.rs");
    let aggregate_execution = read_source("src/db/executor/aggregate/execution.rs");

    assert!(
        grouped_entrypoints.contains(
            "fn grouped_path_runtime(\n        &self,\n        authority: EntityAuthority,"
        ) && grouped_entrypoints.contains("self.grouped_path_runtime(authority)?")
            && !grouped_entrypoints.contains("let authority = EntityAuthority::for_type::<E>();"),
        "grouped runtime preparation must consume prepared accepted authority instead of reopening generated authority",
    );
    assert!(
        aggregate_execution.contains("GlobalDistinct {\n        authority: EntityAuthority,")
            && aggregate_numeric.contains("let authority = plan.authority();")
            && aggregate_distinct
                .contains("self.prepare_grouped_route_runtime(route, authority, None, None)?"),
        "global DISTINCT aggregate execution must carry prepared accepted authority into grouped runtime",
    );
}

#[test]
fn generated_only_prepared_plan_constructor_is_test_only() {
    let prepared_plan = read_source("src/db/executor/prepared_execution_plan/mod.rs");
    let executor_mod = read_source("src/db/executor/mod.rs");
    let query_intent = read_source("src/db/query/intent/query.rs");
    let save_mod = read_source("src/db/executor/mutation/save/mod.rs");
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let save_typed = read_source("src/db/executor/mutation/save/typed.rs");
    let save_batch = read_source("src/db/executor/mutation/save/batch.rs");
    let save_structural = read_source("src/db/executor/mutation/save/structural.rs");
    let session_mod = read_source("src/db/session/mod.rs");
    let session_query_explain = read_source("src/db/session/query/explain.rs");
    let session_query_explain_compact = compact_source(&session_query_explain);
    let session_query_cache = read_source("src/db/session/query/cache.rs");
    let session_sql_explain = read_source("src/db/session/sql/execute/explain.rs");
    let session_sql_explain_compact = compact_source(&session_sql_explain);
    let sql_aggregate_binding = read_source("src/db/sql/lowering/aggregate/command/binding.rs");

    assert!(
        prepared_plan.contains("#[cfg(test)]\n    pub(in crate::db) fn new(")
            && prepared_plan.contains("#[cfg(test)]\n    fn build(")
            && prepared_plan.contains("let authority = EntityAuthority::for_type::<E>();")
            && executor_mod.contains(
                "#[cfg(test)]\nimpl<E> From<CompiledQuery<E>> for PreparedExecutionPlan<E>"
            ),
        "generated-only PreparedExecutionPlan constructors must stay test-only so runtime plans use accepted-backed shared authority",
    );
    assert!(
        query_intent.contains("#[cfg(test)]\n    pub(in crate::db) fn into_plan("),
        "compiled-query plan extraction must remain test-only with the generated-only prepared-plan conversion",
    );
    assert!(
        save_mod.contains("accepted_schema_info: SchemaInfo,")
            && save_mod
                .contains("pub(in crate::db::executor::mutation) const fn accepted_schema_info(")
            && save_validation.contains("schema: &SchemaInfo,")
            && save_typed.contains("let schema = self.accepted_schema_info();")
            && save_batch.contains("let schema = self.accepted_schema_info();")
            && save_structural.contains("let schema = self.accepted_schema_info();")
            && !save_mod.contains("SchemaInfo::cached_for_entity_model(E::MODEL)")
            && !save_validation.contains("SchemaInfo::cached_for_entity_model(E::MODEL)")
            && !save_typed.contains("Self::schema_info()")
            && !save_batch.contains("Self::schema_info()")
            && !save_structural.contains("Self::schema_info()")
            && !save_validation.contains("EntityAuthority::for_type::<E>().schema_info()"),
        "save validation metadata lookup must use session-selected accepted SchemaInfo instead of reopening generated schema authority",
    );
    assert!(
        session_mod.contains("self.db.recovered_store(E::Store::PATH)?")
            && session_mod.contains("ensure_accepted_schema_snapshot(schema_store, E::ENTITY_TAG, E::PATH, E::MODEL)")
            && session_mod.contains("SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, &accepted_schema)")
            && session_mod.contains("self.save_executor::<E>(contract, schema_info, schema_fingerprint)")
            && !session_mod.contains(
                "self.ensure_accepted_schema_snapshot_for_authority(&EntityAuthority::for_type::<E>())",
            ),
        "session save bootstrap must pass accepted schema metadata into SaveExecutor without constructing generated executor authority",
    );
    assert!(
        session_query_cache
            .contains("query.try_build_trivial_scalar_load_plan_with_schema_info(schema_info)?")
            && !session_query_cache.contains("query.try_build_trivial_scalar_load_plan()?"),
        "shared query cache trivial scalar fast path must finalize executor metadata with accepted SchemaInfo instead of generated schema fallback",
    );
    assert!(
        session_query_explain.contains(
            "plan.finalize_access_choice_for_model_with_indexes_and_schema("
        )
            && session_query_explain_compact.contains(
                "SchemaInfo::from_accepted_snapshot_for_model(query.structural().model(),&accepted_schema,"
            )
            && !session_query_explain.contains("plan.finalize_access_choice_for_model_with_indexes(")
            && session_sql_explain.contains(
                "plan.finalize_access_choice_for_model_with_indexes_and_schema("
            )
            && session_sql_explain
                .contains("bind_lowered_sql_query_structural_with_schema(")
            && session_sql_explain.contains(
                "bind_lowered_sql_explain_global_aggregate_structural_with_schema("
            )
            && session_sql_explain_compact.contains(
                "SchemaInfo::from_accepted_snapshot_for_model(authority.model(),&accepted_schema)"
            )
            && !session_sql_explain.contains("plan.finalize_access_choice_for_model_with_indexes(")
            && !session_sql_explain.contains("bind_lowered_sql_query_structural(")
            && !session_sql_explain
                .contains("bind_lowered_sql_explain_global_aggregate_structural(")
            && sql_aggregate_binding.contains(
                "apply_lowered_base_query_shape_with_schema("
            ),
        "session explain binding and access-choice finalization must use accepted SchemaInfo instead of generated schema fallback",
    );
}

#[test]
fn value_stays_out_of_persisted_field_contracts() {
    let forbidden_impls = [
        "implPersistedFieldSlotCodecforValue",
        "implPersistedFieldSlotCodecforVec<Value>",
        "implPersistedStructuredFieldCodecforValue",
        "implPersistedStructuredFieldCodecforVec<Value>",
        "implFieldTypeMetaforValue",
        "implFieldTypeMetaforVec<Value>",
    ];
    let mut violations = Vec::new();

    // Scan production source only. Compile-fail fixtures intentionally mention
    // these shapes so user-facing errors stay locked down separately.
    for path in rust_sources_under("src") {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let compact = compact_source(&source);
        for forbidden in forbidden_impls {
            if compact.contains(forbidden) {
                violations.push(format!("{} contains {forbidden}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Value is runtime-only and must not implement persisted-field contracts:\n{}",
        violations.join("\n"),
    );
}
