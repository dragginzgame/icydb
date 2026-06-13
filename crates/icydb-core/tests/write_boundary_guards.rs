mod support;

use std::{fs, path::PathBuf};

use support::source_guard::{
    compact_source, entity_attribute_blocks, read_rust_sources_under, read_source, read_sources,
    relative_source_path, rust_sources_under, rust_sources_under_path, strip_cfg_test_items,
};

fn assert_source_excludes(label: &str, source: &str, forbidden: &[&str]) {
    let violations = forbidden
        .iter()
        .copied()
        .filter(|symbol| source.contains(symbol))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "{label} contains forbidden boundary symbols: {}",
        violations.join(", "),
    );
}

fn read_sources_from_strings(sources: &[String]) -> String {
    sources.join("\n")
}

#[test]
fn data_store_insert_stays_canonical_row_only() {
    let source = compact_source(&read_source("src/db/data/store.rs"));

    assert_source_excludes(
        "DataStore production insert boundary",
        &source,
        &["fninsert(&mutself,key:RawDataStoreKey,row:RawRow"],
    );
}

#[test]
fn prepared_row_write_payloads_stay_canonical() {
    let prepared_op = read_source("src/db/commit/prepared_op.rs");
    let save_sources = read_sources(&[
        "src/db/executor/mutation/save/typed.rs",
        "src/db/executor/mutation/save/structural.rs",
    ]);
    let save_sources = strip_cfg_test_items(&save_sources);

    assert_source_excludes(
        "prepared row commit op payloads",
        &prepared_op,
        &["data_value: Option<RawRow>"],
    );
    assert_source_excludes(
        "save after-image construction",
        &save_sources,
        &[
            "CanonicalRow::from_entity(entity)?",
            "CanonicalRow::from_generated_entity_for_test(entity)?",
            "MutationInput::from_entity(",
            "materialize_entity_from_serialized_structural_patch_for_generated_model_for_test::<",
        ],
    );
}

#[test]
fn accepted_storage_row_contracts_do_not_retain_generated_field_bridge() {
    let production_sources = read_sources(&[
        "src/db/data/structural_row.rs",
        "src/db/data/persisted_row/reader/structural_slot_reader.rs",
        "src/db/data/persisted_row/reader/primary_key.rs",
        "src/db/data/persisted_row/patch.rs",
        "src/db/executor/terminal/row_decode/mod.rs",
        "src/db/relation/reverse_index.rs",
        "src/db/executor/mutation/save_validation.rs",
    ]);
    let production_sources = strip_cfg_test_items(&production_sources);

    assert_source_excludes(
        "accepted storage row-contract runtime",
        &production_sources,
        &[
            "fn from_model(",
            "fn from_model_with_accepted_decode_contract(",
            "fn from_model_with_accepted_schema_snapshot(",
            "from_generated_model_with_accepted_decode_contract_for_test(",
            "StructuralRowContract::from_generated_model_with_accepted_decode_contract_for_test(",
            "if let Some(primary_key_field) = contract.accepted_field_decode_contract(primary_key_slot)",
        ],
    );
}

#[test]
fn generated_persisted_row_bridge_helpers_are_named_test_only() {
    let row_bridge_sources = read_sources(&[
        "src/db/data/persisted_row/mod.rs",
        "src/db/data/persisted_row/patch.rs",
        "src/db/data/row.rs",
        "src/db/data/persisted_row/reader/structural_slot_reader.rs",
        "src/db/data/persisted_row/writer.rs",
    ]);
    let production_sources = strip_cfg_test_items(&row_bridge_sources);
    let production_sources = compact_source(&production_sources);

    assert_source_excludes(
        "generated row bridge production surface",
        &production_sources,
        &[
            "fnfrom_entity<E>",
            "fnfrom_complete_serialized_structural_patch(",
            "fntry_decode<E",
            "fnfrom_raw_row(",
            "fnfrom_raw_row_with_model(",
            "fnfor_model(",
        ],
    );
}

#[test]
fn commit_and_delete_relation_row_contracts_use_accepted_snapshots() {
    let structural_row = read_source("src/db/data/structural_row.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let relation_validate = read_source("src/db/relation/validate.rs");

    assert_source_excludes(
        "commit/delete accepted row-contract boundaries",
        &read_sources_from_strings(&[structural_row, commit_prepare, relation_validate]),
        &[
            "fn from_model_with_accepted_schema_snapshot(",
            "StructuralRowContract::from_generated_model_for_test_with_accepted_schema_snapshot",
        ],
    );
}

#[test]
fn accepted_row_decode_contract_runtime_lookups_fail_closed() {
    let relation_save_validate = read_source("src/db/relation/save_validate.rs");
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let validation_sources = read_sources_from_strings(&[relation_save_validate, save_validation]);

    assert_source_excludes(
        "accepted save/relation validation",
        &validation_sources,
        &[
            ".field_for_slot(primary_key_slot).ok_or_else(",
            ".field_for_slot(field_index).ok_or_else(",
        ],
    );
}

#[test]
fn accepted_schema_info_index_membership_uses_persisted_index_contracts() {
    let schema_info = read_source("src/db/schema/info.rs");

    assert_source_excludes(
        "accepted SchemaInfo index membership",
        &schema_info,
        &[
            "indexed: generated_field_is_indexed(model, field.name())",
            "indexes: model.indexes()",
            "schema_index_info_from_generated_index(index)",
        ],
    );
}

#[test]
fn sql_ddl_drop_index_uses_persisted_index_origin() {
    let ddl = read_rust_sources_under("src/db/sql/ddl");
    let mutation = read_rust_sources_under("src/db/schema/mutation");

    assert_source_excludes(
        "SQL DDL binding",
        &ddl,
        &[
            "use crate::model::EntityModel",
            "model: &EntityModel",
            "model: &'static EntityModel",
            "model.indexes()",
            "E::MODEL",
        ],
    );
    assert_source_excludes(
        "schema mutation DROP INDEX resolution",
        &mutation,
        &[
            "model.indexes()",
            "model: &EntityModel",
            "use crate::model::EntityModel",
        ],
    );
}

#[test]
fn sql_ddl_add_column_uses_schema_owned_field_allocation() {
    let ddl_field = read_source("src/db/sql/ddl/field.rs");
    let ddl_field_compact = compact_source(&ddl_field);

    assert!(
        ddl_field.contains("resolve_sql_ddl_field_addition_name_candidate(")
            && ddl_field.contains("build_sql_ddl_field_addition_candidate(")
            && ddl_field.contains("resolve_sql_ddl_field_type_contract(")
            && !ddl_field.contains(".field_nullable(statement.column_name.as_str())")
            && !ddl_field_compact.contains("!statement.nullable&&default.is_none()")
            && !ddl_field.contains("fn next_sql_ddl_field_id(")
            && !ddl_field.contains("fn next_sql_ddl_field_slot(")
            && !ddl_field.contains("PersistedFieldOrigin::SqlDdl")
            && !ddl_field.contains("SchemaFieldWritePolicy::from_model_policies(None, None)")
            && !ddl_field.contains("fn persisted_field_contract_for_sql_column_type(")
            && !ddl_field.contains("DEFAULT_BIG_INT_MAX_BYTES")
            && !ddl_field.contains("ScalarCodec::"),
        "SQL DDL ADD COLUMN must bind author intent without owning field existence, type/codec selection, required-default, ID, slot, origin, or write-policy allocation",
    );
}

#[test]
fn sql_ddl_version_contract_preflight_uses_schema_owned_admission() {
    let ddl_mod = read_source("src/db/sql/ddl/mod.rs");
    let ddl_mod_compact = compact_source(&ddl_mod);
    let ddl_sql_admission = read_source("src/db/sql/ddl/admission.rs");
    let ddl_admission = read_source("src/db/schema/mutation/ddl_admission.rs");

    assert!(
        ddl_mod.contains("use admission::{")
            && ddl_mod.contains("validate_bound_sql_ddl_version_contract,")
            && !ddl_mod.contains("validate_schema_ddl_version_contract_preflight(")
            && !ddl_mod.contains("sql_ddl_version_contract_preflight_error(")
            && !ddl_mod.contains("pub(in crate::db) struct BoundSqlDdlSchemaVersionContract")
            && !ddl_mod_compact.contains(
                "ifmatches!(bound.statement(),BoundSqlDdlStatement::NoOp(_)){ifcontract.expected_schema_version().is_none()"
            )
            && !ddl_mod_compact.contains(
                "ifcontract.next_schema_version().is_none(){returnErr(SqlDdlBindError::MissingNextSchemaVersion)"
            ),
        "SQL DDL hub must delegate version-contract binding and preflight to ddl::admission",
    );
    assert!(
        ddl_sql_admission.contains("pub(in crate::db) struct BoundSqlDdlSchemaVersionContract")
            && ddl_sql_admission.contains("pub(in crate::db) const fn ddl_version_contract(")
            && ddl_sql_admission
                .contains("pub(in crate::db) fn bind_sql_ddl_schema_version_contract(")
            && ddl_sql_admission
                .contains("pub(in crate::db) fn validate_bound_sql_ddl_version_contract(")
            && ddl_sql_admission.contains("validate_schema_ddl_version_contract_preflight(")
            && ddl_sql_admission.contains("sql_ddl_version_contract_preflight_error("),
        "SQL DDL admission must map schema-owned version-contract preflight errors instead of the DDL hub owning the matrix",
    );
    assert!(
        ddl_admission.contains("pub(in crate::db) enum SchemaDdlVersionContractPreflightError")
            && ddl_admission.contains("MissingExpectedSchemaVersion")
            && ddl_admission.contains("MissingNextSchemaVersion")
            && ddl_admission.contains("StaleExpectedSchemaVersion")
            && ddl_admission.contains("EmptySchemaVersionBump")
            && ddl_admission
                .contains("pub(in crate::db) fn validate_schema_ddl_version_contract_preflight("),
        "schema mutation admission must own DDL version-contract preflight classification",
    );
}

#[test]
fn generated_sql_update_surface_stays_policy_validated() {
    let generated_sql =
        strip_cfg_test_items(&read_source("../../crates/icydb-build/src/db/sql.rs"));
    let public_update = read_source("src/db/session/sql/execute/write/update.rs");

    assert!(
        generated_sql.contains("fn sql_surface_update_dispatch_arm(")
            && generated_sql.contains("policy: BuildSqlUpdatePolicy,")
            && generated_sql.contains("BuildSqlUpdatePolicy::PublicPrimaryKeyOnly")
            && generated_sql.contains("quote! { execute_sql_public_primary_key_update }")
            && generated_sql.contains("BuildSqlUpdatePolicy::PublicBoundedDeterministic")
            && generated_sql.contains("quote! { execute_sql_public_bounded_update }")
            && generated_sql.contains("update_policy.is_some().then(||")
            && generated_sql.contains("fn __icydb_update(")
            && generated_sql.contains("db().#executor::<#entity_ty>(sql)"),
        "generated SQL update glue must expose __icydb_update only through explicit generated update policies",
    );
    assert!(
        !generated_sql.contains("execute_sql_update::<")
            && !generated_sql.contains("execute_sql_update("),
        "generated SQL update glue must not call the broad session SQL update executor",
    );
    assert!(
        public_update.contains("fn schema_derived_sql_update_plan<E>(")
            && public_update.contains("checked_accepted_write_descriptor::<E>(&schema)?")
            && public_update.contains("SqlUpdatePolicyContext::public_generated(")
            && public_update.contains("descriptor.primary_key_names(),")
            && public_update.contains("generated_fields.as_slice(),")
            && public_update.contains("managed_fields.as_slice(),"),
        "public generated-update helpers must derive policy context from accepted runtime schema descriptors",
    );
    assert!(
        public_update.contains("plan: &SqlPublicPrimaryKeyUpdatePlan,")
            && public_update.contains("plan: &SqlPublicBoundedUpdatePlan,")
            && public_update.contains("SqlUpdateExposurePolicy::PublicPrimaryKeyOnly")
            && public_update.contains("SqlUpdateExposurePolicy::PublicBoundedDeterministic")
            && public_update
                .contains("let SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(plan) = plan else")
            && public_update.contains(
                "let SqlValidatedUpdatePlan::PublicBoundedDeterministic(plan) = plan else",
            )
            && public_update
                .contains("self.execute_validated_sql_public_primary_key_update::<E>(&plan)")
            && public_update
                .contains("self.execute_validated_sql_public_bounded_update::<E>(&plan)"),
        "generated/public update entrypoints must consume policy-specific validated plans before execution",
    );
}

#[test]
fn sql_ddl_default_encoding_uses_schema_owned_field_codecs() {
    let ddl_field = read_source("src/db/sql/ddl/field.rs");

    assert_source_excludes(
        "SQL DDL default binding",
        &ddl_field,
        &[
            "encode_runtime_value_for_accepted_field_contract",
            "AcceptedFieldDecodeContract",
            "canonicalize_strict_sql_literal_for_persisted_kind",
        ],
    );
}

#[test]
fn sql_ddl_drop_column_uses_schema_owned_field_drop_candidate_resolution() {
    let ddl_field = read_source("src/db/sql/ddl/field.rs");

    assert_source_excludes(
        "SQL DDL DROP COLUMN binding",
        &ddl_field,
        &[
            "primary_key_field_ids().contains",
            "resolve_sql_ddl_field_drop_dependent_index",
            "field.generated()",
        ],
    );
}

#[test]
fn sql_ddl_field_metadata_changes_use_schema_owned_candidate_resolution() {
    let ddl_field = read_source("src/db/sql/ddl/field.rs");
    let ddl_mod = read_source("src/db/sql/ddl/mod.rs");

    assert!(
        ddl_field.contains("resolve_sql_ddl_field_set_default_candidate(")
            && ddl_field.contains("resolve_sql_ddl_field_drop_default_candidate(")
            && ddl_field.contains("resolve_sql_ddl_field_nullability_candidate(")
            && ddl_field.contains("resolve_sql_ddl_field_rename_candidate(")
            && !ddl_field.contains("field.generated()"),
        "SQL DDL field metadata changes must map schema-owned candidate classification instead of checking field ownership directly",
    );
    assert!(
        !ddl_mod.contains("clone_with_name(")
            && ddl_mod.contains("admit_sql_ddl_field_rename_candidate(")
            && ddl_mod.contains("rename.field(),")
            && ddl_mod.contains("rename.new_name(),"),
        "SQL DDL lowering must ask schema mutation to construct rename admission targets instead of synthesizing renamed field snapshots",
    );
}

#[test]
fn sql_ddl_create_index_uses_schema_owned_index_candidate_identity() {
    let ddl_index = read_source("src/db/sql/ddl/index.rs");

    assert_source_excludes(
        "SQL DDL CREATE INDEX binding",
        &ddl_index,
        &[
            "accepted_index_field_path_snapshot(",
            "PersistedIndexKeySnapshot::",
            "PersistedIndexKeyItemSnapshot::",
            "PersistedIndexExpressionSnapshot::new(",
            "PersistedIndexExpressionOp",
            "format!(\"expr:v1:{}\"",
            "find_field_path_index_by_name(",
            "existing_field_path_index_matches_request(",
            "find_expression_index_by_name(",
            "existing_expression_index_matches_request(",
            "reject_duplicate_field_path_index(",
            "reject_duplicate_expression_index(",
            "PersistedIndexSnapshot::new_sql_ddl",
            "next_secondary_index_ordinal",
        ],
    );
}

#[test]
fn sql_ddl_frontend_does_not_write_schema_store_directly() {
    let sql_ddl = read_rust_sources_under("src/db/sql");
    let session_sql = read_rust_sources_under("src/db/session/sql");

    for (surface, source) in [("SQL parser/DDL", sql_ddl), ("session SQL", session_sql)] {
        assert!(
            !source.contains("insert_persisted_snapshot")
                && !source.contains("publish_accepted_snapshot")
                && !source.contains("with_schema_mut")
                && !source.contains("SchemaStore")
                && !source.contains("schema_store"),
            "{surface} must not write accepted schema storage directly; SQL DDL must publish through schema-owned mutation/reconciliation surfaces",
        );
    }
}

#[test]
fn schema_mutation_field_path_runner_stays_accepted_schema_authority() {
    let field_path_runner = read_rust_sources_under("src/db/schema/mutation/field_path");

    assert_source_excludes(
        "schema mutation field-path runner modules",
        &field_path_runner,
        &[
            "EntityModel",
            "IndexModel",
            "model_only_from_generated_index",
            "SchemaInfo::cached_for_generated_entity_model",
            "EntityAuthority::for_generated_type_for_test",
        ],
    );
}

#[test]
fn accepted_visible_index_runtime_paths_do_not_reopen_generated_expression_authority() {
    let access_choice = read_source("src/db/query/plan/access_choice/mod.rs");
    let access_plan = read_source("src/db/query/plan/access_plan.rs");
    let pipeline = read_source("src/db/query/plan/pipeline.rs");
    let planner_mod = read_source("src/db/query/plan/planner/mod.rs");
    let session_mod = read_source("src/db/session/mod.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");

    assert_source_excludes(
        "accepted visible-index runtime paths",
        &read_sources_from_strings(&[
            access_choice,
            access_plan,
            pipeline,
            planner_mod,
            session_mod,
            session_query_cache,
        ]),
        &[
            "GeneratedExpressionCandidateIndex",
            "generated_expression_candidate_indexes",
            "generated_candidate_bridge_indexes",
            "VisibleIndexes::accepted_schema_visible(model.indexes(),",
            "accepted_schema_visible(model.indexes(),",
            ".generated_expression_candidate_indexes()",
            "generated_index_bridge",
        ],
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
            && relation_save_validate.contains("PrimaryKeyComponent::from_runtime_value(value)")
            && !relation_save_validate.contains("strong_relations_for_model_iter")
            && !relation_save_validate.contains("E::MODEL"),
        "save relation validation must derive relation metadata from accepted row contracts and fail closed at the admitted strong-relation target boundary",
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
            && reverse_index.contains("source_key_value: &PrimaryKeyValue,")
            && reverse_index.contains("relation_target_keys_for_source_slots(")
            && reverse_index.contains("target_key_value: &PrimaryKeyValue,")
            && !reverse_index.contains("strong_relations_for_model_iter")
            && !reverse_index.contains("source_model: &'static EntityModel"),
        "reverse-index mutation preparation must derive relation fields from accepted row contracts and keep source/target identity on PrimaryKeyValue boundaries",
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
            .contains("schema_contracts.row_contract.clone(),\n        &source_primary_key,"),
        "commit reverse-index preparation must receive the accepted structural row contract and fail-closed source row identity",
    );
}

#[test]
fn index_write_and_recovery_paths_do_not_use_generated_index_fallbacks() {
    let index_key_build = read_source("src/db/index/key/build.rs");
    let index_plan = read_source("src/db/index/plan/mod.rs");
    let index_plan_read = read_source("src/db/index/plan/read.rs");
    let index_readers = read_source("src/db/index/readers.rs");
    let unique_plan = read_source("src/db/index/plan/unique.rs");
    let commit_rebuild = read_source("src/db/commit/rebuild.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");

    assert_source_excludes(
        "index write/recovery accepted paths",
        &read_sources_from_strings(&[
            index_key_build,
            index_plan,
            index_plan_read,
            index_readers,
            unique_plan,
            commit_rebuild,
            commit_prepare,
        ]),
        &[
            "IndexKey::new_from_slots_with_contract(",
            "IndexKey::new_from_slots(\n",
            "GeneratedExpressionIndex",
            "GeneratedExpressionCandidateIndex",
            "GeneratedExpression(&'a IndexModel)",
            "GeneratedExpression(",
            "predicate_bridge: Option<&IndexModel>",
            "index.model_index()",
            "authority.model.indexes()",
            "plan_generated_expression_index_mutation_for_slot_reader_structural(",
            "compile_generated_expression_index_membership_predicate_structural",
            "fn generated_predicate_program_for_accepted_field_path_index(",
        ],
    );
}

#[test]
fn accepted_schema_fingerprints_are_snapshot_only() {
    let fingerprint = read_source("src/db/schema/fingerprint.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");

    assert_source_excludes(
        "accepted schema fingerprint authority",
        &read_sources_from_strings(&[fingerprint, commit_prepare, session_query_cache]),
        &[
            "accepted_commit_schema_fingerprint_for_model",
            "accepted_schema_cache_fingerprint_for_model",
            "accepted_schema_runtime_fingerprint_for_model",
        ],
    );
}

#[test]
fn schema_admission_fingerprints_stay_out_of_query_hot_paths() {
    for (label, source) in [
        ("session", read_rust_sources_under("src/db/session")),
        ("query", read_rust_sources_under("src/db/query")),
        ("executor", read_rust_sources_under("src/db/executor")),
    ] {
        assert!(
            !source.contains("accepted_schema_admission_fingerprint"),
            "{label} hot-path sources must not compute candidate admission fingerprints",
        );
        assert!(
            !source.contains("SchemaAdmissionIdentityComparison"),
            "{label} hot-path sources must not compare candidate admission identities",
        );
        assert!(
            !source.contains("schema_admission_rejection"),
            "{label} hot-path sources must not run schema-version admission policy",
        );
    }
}

#[test]
fn generated_schema_versions_stay_proposal_or_test_only() {
    for (label, source) in [
        ("session", read_rust_sources_under("src/db/session")),
        ("query", read_rust_sources_under("src/db/query")),
        ("executor", read_rust_sources_under("src/db/executor")),
        ("fingerprint", read_source("src/db/schema/fingerprint.rs")),
        ("runtime", read_source("src/db/schema/runtime.rs")),
    ] {
        let production_source = strip_cfg_test_items(&source);
        assert!(
            !production_source.contains("declared_schema_version()"),
            "{label} production code must not read generated declared schema versions",
        );
    }
}

#[test]
fn catalog_diagnostics_expose_method_qualified_schema_fingerprints() {
    let schema_store = read_source("src/db/schema/store.rs");
    let diagnostics_model = read_source("src/db/diagnostics/model.rs");
    let storage_report = read_source("src/db/diagnostics/storage_report.rs");
    let diagnostics_tests = read_source("src/db/diagnostics/tests/mod.rs");

    assert!(
        schema_store.contains("schema_fingerprint_method_version: u8")
            && schema_store.contains("schema_fingerprint_method_version,")
            && schema_store.contains(
                "pub(in crate::db) const fn schema_fingerprint_method_version(self) -> u8"
            )
            && storage_report.contains("metadata.schema_fingerprint_method_version()")
            && diagnostics_model.contains("schema_fingerprint_method_version: Option<u8>")
            && diagnostics_model
                .contains("pub const fn schema_fingerprint_method_version(&self) -> Option<u8>")
            && diagnostics_tests.contains("\"schema_fingerprint_method_version\""),
        "catalog diagnostics must expose schema fingerprint bytes together with their method version",
    );
}

#[test]
fn query_catalog_context_exposes_full_accepted_identity_without_version_policy() {
    let session_sql_compiled = read_source("src/db/session/sql/compiled.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");
    let session_sql_cache = read_source("src/db/session/sql/cache.rs");

    for (label, source) in [
        ("query cache", session_query_cache),
        ("SQL cache", session_sql_cache),
        ("compiled SQL context", session_sql_compiled),
    ] {
        assert!(
            !source.contains("schema_admission_rejection")
                && !source.contains("SchemaAdmissionIdentityComparison")
                && !source.contains("accepted_schema_admission_fingerprint"),
            "{label} must consume accepted identity after admission instead of owning schema-version policy",
        );
    }
}

#[test]
fn schema_store_publication_stays_version_passive() {
    let store = read_source("src/db/schema/store.rs");
    let reconcile = read_source("src/db/schema/reconcile.rs");
    let sql_ddl_reconcile = read_source("src/db/schema/reconcile/sql_ddl.rs");

    for (label, source) in [
        ("store", store),
        ("reconcile", reconcile),
        ("sql_ddl_reconcile", sql_ddl_reconcile),
    ] {
        let compact = compact_source(&source);
        assert!(
            !source.contains("next_schema")
                && !source.contains("next_version")
                && !compact.contains("snapshot.version().get()+1")
                && !compact.contains("snapshot.version().get().saturating_add(1)")
                && !compact.contains("row_layout().version().get()+1")
                && !compact.contains("row_layout().version().get().saturating_add(1)"),
            "{label} schema publication code must not auto-increment schema versions",
        );
    }
}

#[test]
fn schema_version_hard_cut_rejects_obsolete_and_non_positive_internal_formats() {
    let codec = read_source("src/db/schema/codec.rs");
    let codec_compact = compact_source(&codec);
    let integrity = read_source("src/db/schema/integrity.rs");
    let entity_model = read_source("src/model/entity.rs");

    assert!(
        codec.contains("const SCHEMA_SNAPSHOT_CODEC_VERSION: u32 =")
            && codec_compact.contains("ifself.codec_version!=SCHEMA_SNAPSHOT_CODEC_VERSION")
            && codec_compact.contains("returnErr(InternalError::store_corruption());"),
        "schema snapshot codec must hard-cut obsolete internal snapshot formats",
    );
    assert!(
        integrity.contains("version.get() == 0")
            && integrity.contains("return Some(());")
            && entity_model.contains("schema_version > 0")
            && entity_model.contains("generated schema_version must be positive"),
        "generated and persisted schema_version boundaries must reject non-positive versions",
    );
    assert!(
        !codec.contains("unwrap_or(SchemaVersion::initial())")
            && !integrity.contains("unwrap_or(SchemaVersion::initial())"),
        "schema decode/integrity must not infer missing versions from SchemaVersion::initial()",
    );
}

#[test]
fn workspace_entity_declarations_keep_explicit_versions() {
    let manifest_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_roots = [
        "../../schema",
        "../../testing/macro-tests/src",
        "../../testing/macro-tests/tests/ui",
        "../../testing/wasm-helpers/src",
    ];
    let omitted_version_negative_fixture = "testing/macro-tests/tests/ui/schema_version_missing.rs";

    let mut sources = Vec::new();
    for repo_root in repo_roots {
        let mut root = manifest_root.clone();
        root.push(repo_root);
        sources.extend(rust_sources_under_path(root));
    }
    sources.sort();
    sources.dedup();

    let mut violations = Vec::new();
    for source_path in sources {
        let relative = source_path
            .strip_prefix(manifest_root.join("../.."))
            .unwrap_or_else(|err| {
                panic!(
                    "failed to compute repo-relative source path for {}: {err}",
                    source_path.display(),
                )
            })
            .to_string_lossy()
            .replace('\\', "/");
        if relative == omitted_version_negative_fixture {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        for entity_attr in entity_attribute_blocks(&source) {
            let compact_attr = compact_source(entity_attr);
            if !compact_attr.contains("version=") || compact_attr.contains("schema_version=") {
                violations.push(relative.clone());
            }
        }
    }

    assert!(
        violations.is_empty(),
        "all non-negative-test generated entity declarations must carry explicit version. Violations: {}",
        violations.join(", "),
    );
}

#[test]
fn predicate_fast_paths_use_accepted_scalar_slot_helpers() {
    let predicate_runtime = read_source("src/db/predicate/runtime/mod.rs");

    assert!(
        predicate_runtime.contains("slots.field_leaf_codec(field_slot)")
            && predicate_runtime.contains("slots.required_value_storage_scalar(field_slot)")
            && !predicate_runtime.contains("slots\n        .field_decode_contract(field_slot)")
            && !predicate_runtime.contains("slots.field_decode_contract(field_slot)"),
        "conditional-index predicate fast paths must use accepted-aware scalar slot helpers",
    );
}

#[test]
fn runtime_access_and_scan_paths_do_not_reopen_generated_index_models() {
    let runtime_sources = read_sources(&[
        "src/db/access/lowering.rs",
        "src/db/access/shape_facts.rs",
        "src/db/access/execution_contract/types.rs",
        "src/db/access/plan.rs",
        "src/db/executor/aggregate/capability.rs",
        "src/db/executor/covering.rs",
        "src/db/executor/planning/route/pushdown.rs",
        "src/db/executor/stream/access/physical.rs",
        "src/db/executor/stream/access/scan.rs",
        "src/db/query/intent/cache_key.rs",
        "src/db/query/plan/access_choice/mod.rs",
        "src/db/query/plan/access_plan.rs",
        "src/db/query/plan/planner/compare.rs",
        "src/db/query/plan/planner/prefix.rs",
        "src/db/query/plan/planner/range/extract.rs",
        "src/db/query/plan/semantics/logical.rs",
    ]);
    let runtime_sources = strip_cfg_test_items(&runtime_sources);

    assert_source_excludes(
        "runtime access, scan, and planner paths",
        &runtime_sources,
        &[
            "model::index::IndexModel",
            "index: IndexModel",
            "selected_index_model",
            "AccessPlan::index_prefix(*index",
            "AccessPlan::index_multi_lookup(**index",
            "SemanticIndexRangeSpec::new(\n            *index",
            "SemanticIndexRangeSpec::new(*index",
        ],
    );
}

#[test]
fn access_candidate_scoring_does_not_reopen_generated_index_predicates() {
    let scoring_sources = read_sources(&[
        "src/db/query/plan/access_choice/evaluator/mod.rs",
        "src/db/query/plan/access_choice/evaluator/prefix.rs",
        "src/db/query/plan/access_choice/evaluator/range.rs",
        "src/db/query/plan/planner/compare.rs",
        "src/db/query/plan/planner/order_select.rs",
        "src/db/query/plan/planner/prefix.rs",
        "src/db/query/plan/planner/range/extract.rs",
        "src/db/query/plan/planner/ranking.rs",
    ]);
    let scoring_sources = strip_cfg_test_items(&scoring_sources);

    assert_source_excludes(
        "access candidate scoring",
        &scoring_sources,
        &[
            "candidate_satisfies_secondary_order(",
            "index.predicate().is_some()",
            "index.fields()",
            "index.is_field_indexable",
        ],
    );
}

#[test]
fn runtime_query_sql_and_executor_paths_do_not_use_generated_authority_fallbacks() {
    let runtime_sources = read_sources(&[
        "src/db/cursor/boundary.rs",
        "src/db/cursor/mod.rs",
        "src/db/cursor/spine.rs",
        "src/db/executor/aggregate/distinct.rs",
        "src/db/executor/aggregate/execution.rs",
        "src/db/executor/aggregate/numeric/mod.rs",
        "src/db/executor/aggregate/scalar_terminals/mod.rs",
        "src/db/executor/aggregate/scalar_terminals/request.rs",
        "src/db/executor/aggregate/scalar_terminals/terminal.rs",
        "src/db/executor/authority/entity.rs",
        "src/db/executor/explain/mod.rs",
        "src/db/executor/mutation/save/batch.rs",
        "src/db/executor/mutation/save/mod.rs",
        "src/db/executor/mutation/save/structural.rs",
        "src/db/executor/mutation/save/typed.rs",
        "src/db/executor/mutation/save_validation.rs",
        "src/db/executor/pipeline/entrypoints/grouped.rs",
        "src/db/executor/prepared_execution_plan/mod.rs",
        "src/db/executor/prepared_execution_plan/shared_plan.rs",
        "src/db/query/fluent/load/builder.rs",
        "src/db/query/fluent/load/validation.rs",
        "src/db/query/intent/model.rs",
        "src/db/query/intent/query.rs",
        "src/db/query/plan/access_plan.rs",
        "src/db/query/plan/continuation.rs",
        "src/db/query/plan/covering/mod.rs",
        "src/db/query/plan/expr/mod.rs",
        "src/db/query/plan/expr/scalar.rs",
        "src/db/query/plan/group.rs",
        "src/db/query/plan/pipeline.rs",
        "src/db/query/plan/semantics/logical.rs",
        "src/db/query/plan/validate/symbols.rs",
        "src/db/session/mod.rs",
        "src/db/session/query/cache.rs",
        "src/db/session/query/explain.rs",
        "src/db/session/sql/cache.rs",
        "src/db/session/sql/compile/semantic_compiler.rs",
        "src/db/session/sql/compile_cache.rs",
        "src/db/session/sql/execute/explain.rs",
        "src/db/session/sql/execute/global_aggregate.rs",
        "src/db/session/sql/execute/mod.rs",
        "src/db/session/sql/mod.rs",
        "src/db/sql/lowering/aggregate/command/binding.rs",
        "src/db/sql/lowering/aggregate/lowering/helpers.rs",
        "src/db/sql/lowering/aggregate/strategy.rs",
        "src/db/sql/lowering/prepare.rs",
        "src/db/sql/lowering/select/aggregate.rs",
        "src/db/sql/lowering/select/mod.rs",
    ]);
    let runtime_sources = strip_cfg_test_items(&runtime_sources);

    assert_source_excludes(
        "runtime query, SQL, and executor paths",
        &runtime_sources,
        &[
            "EntityAuthority::for_generated_type_for_test::<E>()",
            "lower_sql_command_from_prepared_statement(prepared, model)",
            "bind_lowered_sql_query_structural(",
            "bind_lowered_sql_explain_global_aggregate_structural(",
            "compile_scalar_projection_expr_for_model_only(model, expr)",
            "compile_structural_scalar_aggregate_terminal(
                    E::MODEL,",
            "terminal.uses_shared_count_terminal(E::MODEL)",
            "model.resolve_field_slot",
            "self.model.primary_key_names()",
            ".trivial_scalar_load_fast_path_eligible()",
            "validate_access_runtime_invariants_model(",
            "validate_access_structure_model(self.schema_info()",
            "PredicateProgram::compile_for_model_only(model, predicate)",
            "ensure_accepted_schema_snapshot_for_authority(",
            "cached_shared_query_plan_for_authority(",
            "accepted_entity_authority::<E>()",
        ],
    );
}

#[test]
fn production_row_contract_paths_do_not_fall_back_to_generated_contracts() {
    let row_contract_sources = read_sources(&[
        "src/db/data/persisted_row/contract.rs",
        "src/db/data/persisted_row/patch.rs",
    ]);
    let row_contract_sources = strip_cfg_test_items(&row_contract_sources);

    assert_source_excludes(
        "production row-contract decode and canonicalization paths",
        &row_contract_sources,
        &[
            "decode_runtime_value_from_generated_row_contract(",
            "decode_scalar_slot_value_from_generated_row_contract",
            "validate_non_scalar_slot_value_with_generated_row_contract(",
            "canonical_row_from_runtime_value_source_with_generated_contract",
            "canonical_row_from_structural_slot_reader_with_generated_contract(",
            "from_generated_model_for_test",
        ],
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

#[test]
fn storage_capability_policy_roots_do_not_branch_on_concrete_storage_modes() {
    let checked_roots = [
        "src/db/executor",
        "src/db/relation",
        "src/db/commit",
        "src/db/diagnostics",
    ];
    let checked_files = [
        "src/db/schema/reconcile.rs",
        "src/db/schema/reconcile/sql_ddl.rs",
    ];
    let allowed_storage_mode_files = [
        "src/db/diagnostics/storage_report.rs",
        "src/db/diagnostics/model.rs",
    ];
    let forbidden = [
        "StoreStorage::Heap",
        "StoreStorage::Stable",
        "StoreStorage::Journaled",
        "StoreRuntimeStorageMode::Heap",
        "StoreRuntimeStorageMode::Stable",
        "StoreRuntimeStorageMode::Journaled",
        "DataStoreBackend::Heap",
        "DataStoreBackend::Journaled",
        "IndexStoreBackend::Heap",
        "IndexStoreBackend::Journaled",
        "SchemaStoreBackend::Heap",
        "SchemaStoreBackend::Journaled",
        ".storage_mode()",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        sources.extend(rust_sources_under(root));
    }
    sources.extend(checked_files.iter().map(|path| {
        let mut absolute = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        absolute.push(path);
        absolute
    }));
    sources.sort();

    let mut violations = Vec::new();
    for path in sources {
        let relative = relative_source_path(&path);
        if relative.contains("/tests/") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let production_source = strip_cfg_test_items(&source);
        let symbols: Vec<&str> = forbidden
            .iter()
            .copied()
            .filter(|symbol| {
                if (*symbol == ".storage_mode()" || symbol.starts_with("StoreRuntimeStorageMode::"))
                    && allowed_storage_mode_files.contains(&relative.as_str())
                {
                    return false;
                }
                production_source.contains(symbol)
            })
            .collect();
        if !symbols.is_empty() {
            violations.push(format!("{relative} ({})", symbols.join(", ")));
        }
    }

    assert!(
        violations.is_empty(),
        "runtime policy roots must consume storage capability axes, not concrete storage modes:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn relation_and_commit_policy_consume_storage_capability_axes() {
    let relation_save_validate = read_source("src/db/relation/save_validate.rs");
    let commit_window = read_source("src/db/executor/mutation/commit_window.rs");
    let commit_replay = read_source("src/db/commit/replay.rs");
    let commit_rebuild = read_source("src/db/commit/rebuild.rs");

    assert!(
        relation_save_validate.contains(".storage_capabilities().relation_source()")
            && relation_save_validate.contains(".storage_capabilities().relation_target()")
            && relation_save_validate.contains("StoreRelationSourceCapability::DurableSource")
            && relation_save_validate.contains("StoreRelationTargetCapability::VolatileTarget")
            && !relation_save_validate.contains("data_is_heap_storage")
            && !relation_save_validate.contains("StoreRuntimeStorageMode::Journaled")
            && !relation_save_validate.contains("strong_relation_heap_target_unsupported"),
        "strong relation admission must be expressed through source/target relation capability axes",
    );
    assert!(
        commit_window.contains(".storage_capabilities().commit_participation()")
            && commit_window.contains("StoreCommitParticipation::Durable")
            && commit_window.contains("StoreCommitParticipation::LiveOnly")
            && !commit_window.contains(".storage_mode()")
            && !commit_window.contains("StoreRuntimeStorageMode::"),
        "mutation commit classification must consume commit-participation capabilities",
    );
    assert!(
        commit_replay.contains(".storage_capabilities().recovery()")
            && commit_replay.contains("StoreRecoveryCapability::None")
            && commit_rebuild.contains(".storage_capabilities().recovery()")
            && commit_rebuild.contains("StoreRecoveryCapability::StableCommitReplay"),
        "commit recovery replay/rebuild must consume recovery capabilities instead of replaying every registered store",
    );
}

#[test]
fn mutation_durability_trace_stays_proof_facing_and_not_diagnostics_authority() {
    let commit_window = read_source("src/db/executor/mutation/commit_window.rs");
    let save_shared = read_source("src/db/executor/mutation/save/shared.rs");
    let metrics_sink = read_source("src/metrics/sink.rs");
    let diagnostics = read_rust_sources_under("src/db/diagnostics");

    assert!(
        commit_window.contains("fn record_mutation_commit_plan(")
            && commit_window.contains("record(MetricsEvent::MutationCommitPlan"),
        "mutation durability classification should have one executor-owned trace emitter",
    );
    assert!(
        save_shared.contains("record_mutation_commit_plan(E::PATH, commit_class);")
            && !save_shared.contains("MetricsEvent::MutationCommitPlan"),
        "save-row fast paths should use the executor-owned mutation durability trace helper",
    );
    assert!(
        metrics_sink.contains("MetricsEvent::MutationCommitPlan { .. } => {}"),
        "mutation durability trace events should remain proof-facing instead of becoming global diagnostics authority",
    );
    assert!(
        !diagnostics.contains("MutationCommitPlan")
            && !diagnostics.contains("MutationCommitClass")
            && !diagnostics.contains("classify_mutation_commit_plan"),
        "diagnostics must project store capabilities, not own mutation durability classification",
    );
}

#[test]
fn journaled_diagnostics_project_registry_capabilities_without_backend_authority() {
    let diagnostics = rust_sources_under("src/db/diagnostics")
        .into_iter()
        .filter(|path| !relative_source_path(path).contains("/tests/"))
        .map(|path| {
            fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let diagnostics = strip_cfg_test_items(&diagnostics);
    let storage_report = read_source("src/db/diagnostics/storage_report.rs");
    let storage_report_compact = compact_source(&storage_report);

    assert!(
        storage_report_compact.contains("letcapabilities=store_handle.storage_capabilities();")
            && storage_report_compact
                .contains("letstorage_mode=snapshot_storage_mode(capabilities.storage_mode());")
            && storage_report.contains("DataStoreSnapshot::new(")
            && storage_report.contains("IndexStoreSnapshot::new(")
            && storage_report.contains("SchemaStoreSnapshot::new("),
        "storage diagnostics must project capabilities from registry store handles",
    );
    assert!(
        storage_report.contains("StoreRuntimeStorageMode::Journaled")
            && storage_report.contains("StoreSnapshotStorageMode::Journaled"),
        "diagnostics may render journaled storage mode as a projection",
    );

    for forbidden in [
        "StoreRuntimeStorageCapabilities::stable()",
        "StoreRuntimeStorageCapabilities::heap()",
        "StoreRuntimeStorageCapabilities::journaled()",
        "DataStoreBackend::",
        "IndexStoreBackend::",
        "SchemaStoreBackend::",
        "EntityModel",
        "IndexModel",
    ] {
        assert!(
            !diagnostics.contains(forbidden),
            "diagnostics production code must not derive authority from {forbidden}",
        );
    }
}

#[test]
fn storage_capability_policy_stays_out_of_codecs_and_commit_marker_format() {
    let checked_roots = [
        "src/db/codec",
        "src/db/data/persisted_row/codec",
        "src/db/index/key/codec",
        "src/db/commit/store",
    ];
    let checked_files = ["src/db/schema/codec.rs", "src/db/commit/marker.rs"];
    let forbidden = [
        "StoreRuntimeStorageCapabilities",
        "StoreCommitParticipation",
        "StoreRecoveryCapability",
        "MutationCommitClass",
        "storage_capabilities()",
        "commit_participation()",
        "recovery()",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        sources.extend(rust_sources_under(root));
    }
    sources.extend(checked_files.iter().map(|path| {
        let mut absolute = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        absolute.push(path);
        absolute
    }));
    sources.sort();

    let mut violations = Vec::new();
    for path in sources {
        let relative = relative_source_path(&path);
        if relative.ends_with("/tests.rs") || relative.contains("/tests/") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let production_source = strip_cfg_test_items(&source);
        let symbols = forbidden
            .iter()
            .copied()
            .filter(|symbol| production_source.contains(symbol))
            .collect::<Vec<_>>();
        if !symbols.is_empty() {
            violations.push(format!("{relative} ({})", symbols.join(", ")));
        }
    }

    assert!(
        violations.is_empty(),
        "row/index/schema codecs and commit-marker format must stay independent of storage capability policy:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn stable_and_heap_compatibility_contracts_remain_distinct_from_journaled() {
    let registry_handle = read_source("src/db/registry/handle.rs");
    let registry_tests = read_source("src/db/registry/tests.rs");
    let heap_runtime_tests = read_source("src/db/session/tests/heap_runtime.rs");
    let commit_window = read_source("src/db/executor/mutation/commit_window.rs");
    let commit_replay = read_source("src/db/commit/replay.rs");
    let commit_rebuild = read_source("src/db/commit/rebuild.rs");
    let registry_handle_compact = compact_source(&registry_handle);
    let commit_window_compact = compact_source(&commit_window);

    assert!(
        registry_handle_compact.contains("pubconstfnstable()->Self{Self{storage_mode:StoreRuntimeStorageMode::Stable,allocation_identity:StoreAllocationIdentityCapability::Present,durability:StoreDurability::Durable,recovery:StoreRecoveryCapability::StableCommitReplay,commit_participation:StoreCommitParticipation::Durable,schema_metadata:StoreSchemaMetadataCapability::DurableAcceptedHistory,relation_source:StoreRelationSourceCapability::DurableSource,relation_target:StoreRelationTargetCapability::DurableTarget,live_validation:StoreLiveValidationCapability::Supported,}}")
            && registry_handle_compact.contains("pubconstfnheap()->Self{Self{storage_mode:StoreRuntimeStorageMode::Heap,allocation_identity:StoreAllocationIdentityCapability::Absent,durability:StoreDurability::Volatile,recovery:StoreRecoveryCapability::None,commit_participation:StoreCommitParticipation::LiveOnly,schema_metadata:StoreSchemaMetadataCapability::LiveRebuiltMetadata,relation_source:StoreRelationSourceCapability::LiveSource,relation_target:StoreRelationTargetCapability::VolatileTarget,live_validation:StoreLiveValidationCapability::Supported,}}"),
        "stable and heap capability descriptors must keep their pre-journaled allocation, durability, recovery, commit, schema, relation, and validation contracts",
    );
    assert!(
        registry_handle_compact.contains("pubconstfnabsent()->Self{Self{data:None,index:None,schema:None,journal:None,}}")
            && registry_handle_compact.contains("pubconstfnnew(data:StoreAllocationIdentity,index:StoreAllocationIdentity,schema:StoreAllocationIdentity,)->Self{Self{data:Some(data),index:Some(index),schema:Some(schema),journal:None,}}")
            && registry_handle_compact.contains("StoreRuntimeStorageMode::Stable=>{self.data.is_some()&&self.index.is_some()&&self.schema.is_some()&&self.journal.is_none()}")
            && registry_handle_compact.contains("StoreRuntimeStorageMode::Heap=>{self.data.is_none()&&self.index.is_none()&&self.schema.is_none()&&self.journal.is_none()}"),
        "stable allocation identity must stay three-role durable metadata and heap allocation identity must stay explicitly absent",
    );
    assert!(
        registry_tests
            .contains("fn register_store_with_stable_allocation_identities_binds_metadata()")
            && registry_tests.contains("StoreRuntimeStorageMode::Stable")
            && registry_tests.contains("StoreRecoveryCapability::StableCommitReplay")
            && registry_tests.contains(
                "fn register_store_with_absent_allocation_identities_binds_store_handles()"
            )
            && registry_tests.contains("StoreRuntimeStorageMode::Heap")
            && registry_tests.contains("StoreCommitParticipation::LiveOnly")
            && registry_tests.contains("StoreRecoveryCapability::None"),
        "registry tests must continue proving stable metadata is present and heap metadata is absent",
    );
    assert!(
        heap_runtime_tests.contains(
            "fn heap_backed_session_reinit_loses_rows_and_indexes_but_reconciles_live_schema()"
        ) && heap_runtime_tests.contains(
            "heap rows must not be recovered from stable commit state after store reinit"
        ) && heap_runtime_tests.contains(
            "heap schema metadata is rebuilt for live validation/diagnostics, not recovered as rows"
        ) && heap_runtime_tests
            .contains("assert_eq!(heap_classes, vec![MutationCommitClass::LiveOnly]);"),
        "heap runtime tests must continue proving volatility, live-only commit classification, and rebuilt live schema metadata",
    );
    assert!(
        commit_window_compact.contains(
            "StoreRecoveryCapability::StableCommitReplay=>recovery_row_ops.push(row_op.clone())"
        ) && commit_window_compact.contains("StoreRecoveryCapability::None=>{}")
            && commit_replay.contains("StoreRecoveryCapability::None => continue")
            && commit_rebuild.contains("StoreRecoveryCapability::StableCommitReplay")
            && commit_rebuild.contains("StoreRecoveryCapability::StableBasePlusJournalReplay"),
        "commit/recovery code must keep stable row-op replay and heap no-recovery participation distinct from journaled replay",
    );
}

#[test]
fn journaled_storage_schema_build_admission_stays_out_of_runtime_backends() {
    let diagnostics = read_rust_sources_under("src/db/diagnostics");
    let relation = read_rust_sources_under("src/db/relation");

    assert_source_excludes(
        "relation policy",
        &relation,
        &["Journaled", "DataStoreBackend::", "IndexStoreBackend::"],
    );
    assert_source_excludes(
        "diagnostics authority",
        &diagnostics,
        &[
            "DataStoreBackend::Journaled",
            "IndexStoreBackend::Journaled",
            "SchemaStoreBackend::Journaled",
        ],
    );
}

#[test]
fn commit_recovery_boundary_keeps_journal_replay_marker_bound_and_folds_via_watermark() {
    let executor_mutation = read_rust_sources_under("src/db/executor/mutation");
    let relation = read_rust_sources_under("src/db/relation");

    assert_source_excludes(
        "executor mutation and relation policy roots",
        &read_sources_from_strings(&[executor_mutation, relation]),
        &["fold_journal"],
    );
}

#[test]
fn journaled_canonical_btree_mutation_stays_fold_recovery_only() {
    for (label, source) in [
        ("executor", read_rust_sources_under("src/db/executor")),
        ("query", read_rust_sources_under("src/db/query")),
        ("session", read_rust_sources_under("src/db/session")),
        ("relation", read_rust_sources_under("src/db/relation")),
    ] {
        for forbidden in [
            "fold_recovered_journal_put",
            "fold_recovered_journal_delete",
            "fold_persisted_snapshot",
            "fold_journaled_materialized_view",
        ] {
            assert!(
                !source.contains(forbidden),
                "{label} policy roots must not call journaled canonical fold helper {forbidden}",
            );
        }
    }
}

#[test]
fn journaled_ordered_overlay_traversal_stays_streaming_and_fold_only() {
    let executor = read_rust_sources_under("src/db/executor");
    let query = read_rust_sources_under("src/db/query");
    let session_query = read_rust_sources_under("src/db/session/query");
    let session_sql = read_rust_sources_under("src/db/session/sql");

    for (label, source) in [
        ("executor", executor),
        ("query", query),
        ("session query", session_query),
        ("session sql", session_sql),
    ] {
        assert!(
            !source.contains("journaled_entries_snapshot")
                && !source.contains("journaled_entries_snapshot_for_fold")
                && !source.contains("journaled_snapshots")
                && !source.contains("latest_raw_snapshots_by_entity"),
            "{label} hot paths must not call journaled snapshot materialization helpers",
        );
    }
}

#[test]
fn journaled_read_hot_paths_stay_off_recovery_fold_diagnostics_and_authority_rebuilds() {
    let read_hot_paths = [
        (
            "executor stream",
            read_rust_sources_under("src/db/executor/stream"),
        ),
        (
            "executor scan",
            read_rust_sources_under("src/db/executor/scan"),
        ),
        (
            "executor pipeline",
            read_rust_sources_under("src/db/executor/pipeline"),
        ),
        (
            "executor runtime context",
            read_rust_sources_under("src/db/executor/runtime_context"),
        ),
        ("query", read_rust_sources_under("src/db/query")),
        (
            "session query",
            read_rust_sources_under("src/db/session/query"),
        ),
        ("session sql", read_rust_sources_under("src/db/session/sql")),
    ];

    for (label, source) in read_hot_paths {
        for forbidden in [
            "storage_report",
            "StoreDiagnostics",
            "fold_journaled",
            "recover_journaled",
            "read_committed_batches_after",
            "journaled_entries_snapshot",
            "journaled_entries_snapshot_for_fold",
            "journaled_snapshots",
            "latest_raw_snapshots_by_entity",
        ] {
            assert!(
                !source.contains(forbidden),
                "{label} read hot path must not call {forbidden}",
            );
        }
    }
}

#[test]
fn accepted_catalog_context_reaches_filterless_query_plan_cache_before_schema_projection() {
    let session_query_cache = read_source("src/db/session/query/cache.rs");
    let session_query_cache_compact = compact_source(&session_query_cache);
    let schema_store = read_source("src/db/schema/store.rs");
    let schema_store_compact = compact_source(&schema_store);
    let cached_shared_query_plan_for_entity = session_query_cache_compact
        .split("pub(incrate::db::session)fncached_shared_query_plan_for_entity<E>")
        .nth(1)
        .expect("session query cache should keep typed shared-plan cache helper");

    let identity_selection = cached_shared_query_plan_for_entity
        .find("accepted_catalog_snapshot_selection_for_query::<E>()")
        .expect(
            "typed query cache should select accepted catalog identity before filterless lookup",
        );
    let cache_hit = cached_shared_query_plan_for_entity
        .find("try_cached_filterless_query_plan_for_entity_path(")
        .expect("typed query cache should try eligible cache hits by catalog identity");
    let authority_projection = cached_shared_query_plan_for_entity
        .find("accepted_schema_catalog_context_from_selection::<E>")
        .expect("cache misses should decode the selected accepted snapshot");
    assert!(
        identity_selection < cache_hit && cache_hit < authority_projection,
        "eligible filterless query-plan cache hits must return before accepted snapshot decode, authority, and SchemaInfo projection",
    );
    let identity_hit_prefix = &cached_shared_query_plan_for_entity[..cache_hit];
    assert_source_excludes(
        "eligible filterless query-plan cache-hit prefix",
        identity_hit_prefix,
        &[
            "SchemaInfo",
            "AcceptedRowLayoutRuntimeContract",
            "decode_verified",
            "ensure_accepted_schema_snapshot",
            "reconcile_runtime_schemas",
            "schema_admission_rejection",
            "SchemaAdmissionIdentityComparison",
            "accepted_schema_admission_fingerprint",
            "latest_raw_snapshots_by_entity",
            "storage_report",
            "integrity_report",
            "show_entities",
            "show_stores",
        ],
    );

    let latest_catalog_identity = schema_store_compact
        .split("pub(incrate::db)fnlatest_catalog_identity(")
        .nth(1)
        .expect("schema store should expose latest catalog identity")
        .split("///Returnrawschema-storefootprintfactsforoneentity.")
        .next()
        .expect("latest catalog identity section should be bounded");

    assert!(
        schema_store.contains("pub(in crate::db) fn latest_catalog_identity(")
            && schema_store_compact.contains("self.latest_raw_snapshot_entry(entity)")
            && schema_store_compact.contains("raw_snapshot.accepted_schema_fingerprint()?")
            && !latest_catalog_identity.contains("latest_raw_snapshots_by_entity")
            && !latest_catalog_identity.contains("visit_raw_snapshots")
            && !latest_catalog_identity.contains("decode_persisted_snapshot"),
        "accepted catalog identity lookup must use one-entity raw snapshot selection without full decode or all-entity metadata",
    );
    assert_source_excludes(
        "query cache accepted catalog path",
        &session_query_cache,
        &[
            "accepted_entity_authority::<E>()",
            "ensure_accepted_schema_snapshot::<E>()",
            "latest_raw_snapshots_by_entity",
        ],
    );
}

#[test]
fn journal_format_boundary_preserves_row_index_schema_codecs() {
    let checked_roots = [
        "src/db/codec",
        "src/db/data/persisted_row/codec",
        "src/db/index/key/codec",
    ];
    let checked_files = ["src/db/schema/codec.rs"];
    let forbidden = [
        "JournalRecord",
        "JournalRecordEnvelope",
        "JournalCodec",
        "JournalReplay",
        "JournalFold",
        "Journaled",
        "CommittedJournal",
        "journal_record",
        "journal_format",
        "journal_replay",
        "replay_journal",
        "fold_journal",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        sources.extend(rust_sources_under(root));
    }
    sources.extend(checked_files.iter().map(|path| {
        let mut absolute = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        absolute.push(path);
        absolute
    }));
    sources.sort();

    let mut violations = Vec::new();
    for path in sources {
        let relative = relative_source_path(&path);
        if relative.ends_with("/tests.rs") || relative.contains("/tests/") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let production_source = strip_cfg_test_items(&source);
        let symbols = forbidden
            .iter()
            .copied()
            .filter(|symbol| production_source.contains(symbol))
            .collect::<Vec<_>>();
        if !symbols.is_empty() {
            violations.push(format!("{relative} ({})", symbols.join(", ")));
        }
    }

    assert!(
        violations.is_empty(),
        "journaled codec work must stay out of existing row, index, and schema codec surfaces:\n{}",
        violations.join("\n"),
    );
}
