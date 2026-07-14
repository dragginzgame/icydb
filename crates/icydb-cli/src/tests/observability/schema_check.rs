//! Module: Observability schema-check tests.
//! Responsibility: exercise schema-check payload decoding and drift report rendering.
//! Does not own: metrics, schema, or snapshot observability reports.
//! Boundary: test-only assertions over generated-vs-accepted schema comparison output.

use candid::Encode;
use icydb::db::{
    EntityFieldDescription, EntityIndexDescription, EntitySchemaCheckDescription,
    EntitySchemaDescription,
};

use crate::observability::test_support::{decode_schema_check_report, render_schema_check_report};

#[test]
fn decode_schema_check_report_accepts_generated_response_shape() {
    let response: Result<Vec<EntitySchemaCheckDescription>, icydb::Error> = Ok(Vec::new());
    let candid_bytes = Encode!(&response).expect("schema check response should encode");
    let decoded = decode_schema_check_report(candid_bytes.as_slice())
        .expect("schema check response should decode")
        .expect("schema check response should be ok");

    assert_eq!(decoded.len(), 0);
}

#[test]
fn schema_check_report_renders_generated_accepted_drift() {
    let generated = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![
            EntityFieldDescription::new(
                "id".to_string(),
                Some(0),
                "Id".to_string(),
                false,
                true,
                true,
                "generated".to_string(),
            ),
            EntityFieldDescription::new(
                "name".to_string(),
                Some(1),
                "Text".to_string(),
                false,
                false,
                true,
                "generated".to_string(),
            ),
        ],
        vec![EntityIndexDescription::new(
            "idx_character__name".to_string(),
            false,
            vec!["name".to_string()],
            "generated".to_string(),
        )],
        Vec::new(),
    );
    let accepted = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![
            EntityFieldDescription::new(
                "id".to_string(),
                Some(0),
                "Id".to_string(),
                false,
                true,
                true,
                "generated".to_string(),
            ),
            EntityFieldDescription::new(
                "name".to_string(),
                Some(1),
                "Text".to_string(),
                false,
                false,
                true,
                "generated".to_string(),
            ),
            EntityFieldDescription::new(
                "nickname".to_string(),
                Some(2),
                "Text".to_string(),
                true,
                false,
                true,
                "ddl".to_string(),
            ),
        ],
        vec![
            EntityIndexDescription::new(
                "idx_character__name".to_string(),
                false,
                vec!["name".to_string()],
                "generated".to_string(),
            ),
            EntityIndexDescription::new(
                "character_lower_name_idx".to_string(),
                false,
                vec!["expr:v1:LOWER(name)".to_string()],
                "ddl".to_string(),
            ),
        ],
        Vec::new(),
    );

    let text =
        render_schema_check_report(&[EntitySchemaCheckDescription::new(generated, accepted)]);

    assert!(text.contains("IcyDB schema check"));
    assert!(text.contains("status: drift"));
    assert!(text.contains("accepted-only fields: 1"));
    assert!(text.contains("DDL-owned indexes: 1"));
    assert!(text.contains("mismatches: 0"));
    assert!(text.contains("Character   drift"));
    assert!(text.contains("accepted-only field"));
    assert!(text.contains("DDL index"));
    assert!(text.contains("recommendations"));
    assert!(
        text.contains("ok: DDL-owned accepted fields are preserved catalog drift across upgrade")
    );
    assert!(text.contains(
        "action: add DDL-owned fields to Rust schema only when an explicit adoption flow exists"
    ));
    assert!(text.contains("ok: DDL-owned accepted indexes remain planner-visible catalog drift"));
}

#[test]
fn schema_check_report_renders_empty_report_sections() {
    let text = render_schema_check_report(&[]);

    assert!(text.contains("IcyDB schema check"));
    assert!(text.contains("status: ok"));
    assert!(text.contains("entities: 0"));
    assert!(text.contains("entities\n  None"));
    assert!(text.contains("accepted drift\n  None"));
    assert!(text.contains("mismatches\n  None"));
    assert!(text.contains("recommendations\n  ok: generated and accepted schema are aligned"));
}

#[test]
fn schema_check_report_compares_ordered_primary_key_fields() {
    let generated = EntitySchemaDescription::new_with_primary_key_fields(
        "demo::Placement".to_string(),
        "Placement".to_string(),
        "tenant_id, local_id".to_string(),
        vec!["tenant_id".to_string(), "local_id".to_string()],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let accepted = EntitySchemaDescription::new_with_primary_key_fields(
        "demo::Placement".to_string(),
        "Placement".to_string(),
        "tenant_id, local_id".to_string(),
        vec!["local_id".to_string(), "tenant_id".to_string()],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let text =
        render_schema_check_report(&[EntitySchemaCheckDescription::new(generated, accepted)]);

    assert!(text.contains("status: mismatch"));
    assert!(text.contains("primary key"));
    assert!(text.contains("tenant_id, local_id"));
    assert!(text.contains("local_id, tenant_id"));
}

#[test]
fn schema_check_report_treats_accepted_only_generated_fields_as_mismatch() {
    let generated = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![EntityFieldDescription::new(
            "id".to_string(),
            Some(0),
            "Id".to_string(),
            false,
            true,
            true,
            "generated".to_string(),
        )],
        Vec::new(),
        Vec::new(),
    );
    let accepted = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![
            EntityFieldDescription::new(
                "id".to_string(),
                Some(0),
                "Id".to_string(),
                false,
                true,
                true,
                "generated".to_string(),
            ),
            EntityFieldDescription::new(
                "retired_generated".to_string(),
                Some(1),
                "Text".to_string(),
                false,
                false,
                true,
                "generated".to_string(),
            ),
        ],
        Vec::new(),
        Vec::new(),
    );

    let text =
        render_schema_check_report(&[EntitySchemaCheckDescription::new(generated, accepted)]);

    assert!(text.contains("status: mismatch"));
    assert!(text.contains("accepted-only fields: 0"));
    assert!(text.contains("mismatches: 1"));
    assert!(text.contains("accepted-only generated field"));
    assert!(
        text.contains(
            "fix: resolve generated-vs-accepted mismatches before relying on schema parity"
        )
    );
    assert!(text.contains(
        "fix: accepted-only generated fields require explicit catalog-native physical removal"
    ));
}

#[test]
fn schema_check_report_recommends_additive_transition_for_generated_only_fields() {
    let generated = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![
            EntityFieldDescription::new(
                "id".to_string(),
                Some(0),
                "Id".to_string(),
                false,
                true,
                true,
                "generated".to_string(),
            ),
            EntityFieldDescription::new(
                "title".to_string(),
                Some(1),
                "Text".to_string(),
                true,
                false,
                true,
                "generated".to_string(),
            ),
        ],
        Vec::new(),
        Vec::new(),
    );
    let accepted = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![EntityFieldDescription::new(
            "id".to_string(),
            Some(0),
            "Id".to_string(),
            false,
            true,
            true,
            "generated".to_string(),
        )],
        Vec::new(),
        Vec::new(),
    );

    let text =
        render_schema_check_report(&[EntitySchemaCheckDescription::new(generated, accepted)]);

    assert!(text.contains("status: mismatch"));
    assert!(text.contains("generated-only field"));
    assert!(text.contains(
        "action: generated-only fields need an accepted additive transition before deploy"
    ));
}

#[test]
fn schema_check_report_recommends_explicit_flows_for_default_and_nullability_drift() {
    let generated = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![
            EntityFieldDescription::new(
                "level".to_string(),
                Some(1),
                "nat16 default=slot_payload(bytes=4, sha256=aaaaaaaaaaaaaaaa)".to_string(),
                false,
                false,
                true,
                "generated".to_string(),
            ),
            EntityFieldDescription::new(
                "nickname".to_string(),
                Some(2),
                "Text".to_string(),
                false,
                false,
                true,
                "generated".to_string(),
            ),
        ],
        Vec::new(),
        Vec::new(),
    );
    let accepted = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![
            EntityFieldDescription::new(
                "level".to_string(),
                Some(1),
                "nat16 default=slot_payload(bytes=4, sha256=bbbbbbbbbbbbbbbb)".to_string(),
                false,
                false,
                true,
                "generated".to_string(),
            ),
            EntityFieldDescription::new(
                "nickname".to_string(),
                Some(2),
                "Text".to_string(),
                true,
                false,
                true,
                "generated".to_string(),
            ),
        ],
        Vec::new(),
        Vec::new(),
    );

    let text =
        render_schema_check_report(&[EntitySchemaCheckDescription::new(generated, accepted)]);

    assert!(text.contains("status: mismatch"));
    assert!(
        text.contains("fix: default drift requires an explicit ALTER COLUMN SET/DROP DEFAULT flow")
    );
    assert!(text.contains(
        "fix: nullability drift requires an explicit ALTER COLUMN SET/DROP NOT NULL flow"
    ));
}

#[test]
fn schema_check_report_recommends_explicit_flows_for_generated_index_drift() {
    let generated = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        Vec::new(),
        vec![
            EntityIndexDescription::new(
                "idx_character__name".to_string(),
                false,
                vec!["name".to_string()],
                "generated".to_string(),
            ),
            EntityIndexDescription::new(
                "idx_character__class_name".to_string(),
                false,
                vec!["class_name".to_string()],
                "generated".to_string(),
            ),
        ],
        Vec::new(),
    );
    let accepted = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        Vec::new(),
        vec![
            EntityIndexDescription::new(
                "idx_character__name".to_string(),
                false,
                vec!["nickname".to_string()],
                "generated".to_string(),
            ),
            EntityIndexDescription::new(
                "idx_character__level".to_string(),
                false,
                vec!["level".to_string()],
                "generated".to_string(),
            ),
        ],
        Vec::new(),
    );

    let text =
        render_schema_check_report(&[EntitySchemaCheckDescription::new(generated, accepted)]);

    assert!(text.contains("status: mismatch"));
    assert!(text.contains("index"));
    assert!(text.contains("accepted-only generated index"));
    assert!(text.contains("generated-only index"));
    assert!(text.contains(
        "action: generated-only indexes need accepted index publication before planner parity"
    ));
    assert!(text.contains(
        "fix: accepted-only generated indexes require explicit index removal or generated schema restoration"
    ));
    assert!(text.contains(
        "fix: index contract drift requires explicit index replacement, not same-name mutation"
    ));
}

#[test]
fn schema_check_report_treats_generated_index_origin_drift_as_mismatch() {
    let generated = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        Vec::new(),
        vec![EntityIndexDescription::new(
            "idx_character__name".to_string(),
            false,
            vec!["name".to_string()],
            "ddl".to_string(),
        )],
        Vec::new(),
    );
    let accepted = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        Vec::new(),
        vec![EntityIndexDescription::new(
            "idx_character__name".to_string(),
            false,
            vec!["name".to_string()],
            "generated".to_string(),
        )],
        Vec::new(),
    );

    let text =
        render_schema_check_report(&[EntitySchemaCheckDescription::new(generated, accepted)]);

    assert!(text.contains("status: mismatch"));
    assert!(text.contains("idx_character__name:name:no:ddl"));
    assert!(text.contains("idx_character__name:name:no:generated"));
    assert!(text.contains(
        "fix: index contract drift requires explicit index replacement, not same-name mutation"
    ));
}

#[test]
fn schema_check_report_renders_readable_default_mismatch() {
    let generated = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![EntityFieldDescription::new(
            "level".to_string(),
            Some(1),
            "nat16 default=slot_payload(bytes=4, sha256=aaaaaaaaaaaaaaaa)".to_string(),
            false,
            false,
            true,
            "generated".to_string(),
        )],
        Vec::new(),
        Vec::new(),
    );
    let accepted = EntitySchemaDescription::new(
        "demo::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        vec![EntityFieldDescription::new(
            "level".to_string(),
            Some(1),
            "nat16 default=slot_payload(bytes=4, sha256=bbbbbbbbbbbbbbbb)".to_string(),
            false,
            false,
            true,
            "generated".to_string(),
        )],
        Vec::new(),
        Vec::new(),
    );

    let text =
        render_schema_check_report(&[EntitySchemaCheckDescription::new(generated, accepted)]);

    assert!(text.contains("status: mismatch"));
    assert!(text.contains("default=slot_payload(bytes=4, sha256=aaaaaaaaaaaaaaaa)"));
    assert!(text.contains("default=slot_payload(bytes=4, sha256=bbbbbbbbbbbbbbbb)"));
}
