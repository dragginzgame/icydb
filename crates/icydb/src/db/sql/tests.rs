//! Module: db::sql::tests
//!
//! Responsibility: module boundary tests.
//! Does not own: production implementation or public API ownership.
//! Boundary: verifies facade contracts through local module behavior.

use crate::db::{
    EntityCatalogCounts, EntityCatalogDescription, EntityFieldDescription, EntityIndexDescription,
    EntityRelationCardinality, EntityRelationDescription, EntitySchemaDescription,
    MemoryCatalogDescription, RowProjectionOutput, SqlColumnDefault, SqlColumnExtra, SqlColumnKey,
    SqlDescribeOutput, SqlShowColumnsOutput, SqlShowRelationsOutput, StoreCatalogDescription,
    sql::{
        SqlGroupedRowsOutput, SqlQueryResult, render_describe_lines, render_describe_output_lines,
        render_show_columns_lines, render_show_constraints_lines, render_show_entities_lines,
        render_show_entities_verbose_lines, render_show_indexes_lines, render_show_memory_lines,
        render_show_relations_lines, render_show_stores_lines, render_show_stores_verbose_lines,
        sql_query_result_from_statement,
    },
};
use crate::value::OutputValue;
use std::{
    io::Write as _,
    process::{Command, Stdio},
};

use candid::{CandidType, Decode, Encode};
use icydb_core::db::{GroupedRow, SqlStatementResult};
use icydb_core::types::{Decimal, Float32, Float64};
use serde::Deserialize;

const PRE_0_224_VERBOSE_DOSSIER_CANDID_BYTES: usize = 2_063;
const PRE_0_224_VERBOSE_DOSSIER_CANDID_SHA256: &str =
    "8456e5335bc0456b7d10b2ba8b344c8d8bd09326cba18ab0e4a881dca2f890c8";
const CURRENT_VERBOSE_DOSSIER_CANDID_BYTES: usize = 2_074;
const CURRENT_VERBOSE_DOSSIER_CANDID_SHA256: &str =
    "7e6bca5053e8cba781c2cc49bd7a3d8d7e0c51602906fa89f146b2652459674d";
const PRE_0_224_RELATION_CONTROL_CANDID_BYTES: usize = 242;
const PRE_0_224_RELATION_CONTROL_CANDID_SHA256: &str =
    "4a25beaeb6f7fb4f0665e713ecefeb06dea781fb448da21b5c610ae33341b4ea";
const PRE_0_224_VERBOSE_DOSSIER_LINES: &str =
    include_str!("fixtures/pre_0_224_verbose_dossier.lines");

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct Pre0224EntitySchemaDescription {
    entity_path: String,
    entity_name: String,
    entity_tag: u64,
    accepted_schema_fingerprint_method: u8,
    accepted_schema_fingerprint: [u8; 16],
    primary_key: String,
    primary_key_fields: Vec<String>,
    identity: Option<Box<Pre0224EntityIdentityDescription>>,
    fields: Vec<EntityFieldDescription>,
    indexes: Vec<EntityIndexDescription>,
    relations: Vec<EntityRelationDescription>,
    constraints: Vec<Pre0224EntityConstraintDescription>,
    row_layout_current: u32,
    row_layout_history_floor: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct Pre0224EntityIdentityDescription {
    field: String,
    generator: String,
    accepted_kind: String,
    minimum: u128,
    maximum: u128,
    high_water: u128,
    remaining: u128,
    exhausted: bool,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct Pre0224EntityConstraintDescription {
    id: u32,
    name: String,
    kind: String,
    origin: String,
    validation_state: String,
    validation_progress: Option<Pre0224ConstraintValidationProgressDescription>,
    field_id: Option<u32>,
    index_id: Option<u32>,
    relation_id: Option<u32>,
    fields: Vec<String>,
    index: Option<String>,
    relation: Option<String>,
    target_entity: Option<String>,
    action: Option<String>,
    semantics: String,
    check_sql: Option<String>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct Pre0224ConstraintValidationProgressDescription {
    phase: String,
    rows_scanned: u64,
    findings_seen: u64,
    restarts: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
enum SqlColumnKeyContract {
    Primary,
    Unique,
    Multiple,
    None,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
enum SqlColumnDefaultContract {
    Auto,
    Null,
    Literal { text: String },
    Required,
    NotApplicable,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
enum SqlColumnExtraContract {
    Identity,
    Generated,
    Relation,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlColumnSummaryContract {
    name: String,
    field_type: String,
    nullable: bool,
    key: SqlColumnKeyContract,
    default: SqlColumnDefaultContract,
    extra: Vec<SqlColumnExtraContract>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
enum SqlDescribeOutputContract {
    Compact {
        entity: String,
        columns: Vec<SqlColumnSummaryContract>,
    },
    Verbose {
        description: EntitySchemaDescription,
    },
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
enum SqlShowColumnsOutputContract {
    Compact {
        entity: String,
        columns: Vec<SqlColumnSummaryContract>,
    },
    Verbose {
        entity: String,
        columns: Vec<EntityFieldDescription>,
    },
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlShowRelationsOutputContract {
    entity: String,
    relations: Vec<EntityRelationDescription>,
}

fn text(value: &str) -> OutputValue {
    OutputValue::Text(value.to_string())
}

fn required_field(name: &str, slot: u16, kind: &str, primary_key: bool) -> EntityFieldDescription {
    EntityFieldDescription::new(
        name.to_string(),
        Some(slot),
        kind.to_string(),
        false,
        primary_key,
        true,
        "generated".to_string(),
        Some("required".to_string()),
        None,
        None,
        None,
        Some(1),
        Some("reject".to_string()),
        None,
        None,
    )
}

fn pre_0_224_field(
    name: &str,
    slot: Option<u16>,
    kind: &str,
    nullable: bool,
    primary_key: bool,
    queryable: bool,
    origin: &str,
) -> EntityFieldDescription {
    EntityFieldDescription::new(
        name.to_string(),
        slot,
        kind.to_string(),
        nullable,
        primary_key,
        queryable,
        origin.to_string(),
        slot.map(|_| {
            if nullable {
                "null".to_string()
            } else {
                "required".to_string()
            }
        }),
        None,
        None,
        None,
        slot.map(|_| 1),
        slot.map(|_| "reject".to_string()),
        None,
        None,
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "the named golden keeps the complete pre-0.224 dossier visible in one fixture"
)]
fn pre_0_224_verbose_dossier_fixture() -> Pre0224EntitySchemaDescription {
    let mut fields = vec![
        EntityFieldDescription::new(
            "id".to_string(),
            Some(0),
            "Nat64".to_string(),
            false,
            true,
            true,
            "generated".to_string(),
            Some("generated".to_string()),
            None,
            None,
            None,
            Some(1),
            Some("reject".to_string()),
            None,
            None,
        ),
        pre_0_224_field(
            "display_name",
            Some(1),
            "Text(64)",
            false,
            false,
            true,
            "generated",
        ),
        pre_0_224_field(
            "profile",
            Some(2),
            "Composite(Profile)",
            true,
            false,
            true,
            "generated",
        ),
        pre_0_224_field(
            "profile.nickname",
            None,
            "Text(32)",
            true,
            false,
            true,
            "generated",
        ),
        pre_0_224_field(
            "profile.rank",
            None,
            "Nat16",
            true,
            false,
            true,
            "generated",
        ),
        pre_0_224_field(
            "friend_id",
            Some(3),
            "Nat64",
            true,
            false,
            true,
            "generated",
        ),
        pre_0_224_field(
            "member_ids",
            Some(4),
            "List<Nat64>(16)",
            false,
            false,
            true,
            "generated",
        ),
        pre_0_224_field(
            "watcher_ids",
            Some(5),
            "Set<Nat64>(16)",
            false,
            false,
            true,
            "generated",
        ),
    ];
    fields.push(EntityFieldDescription::new(
        "legacy_score".to_string(),
        Some(6),
        "Int32".to_string(),
        false,
        false,
        true,
        "ddl".to_string(),
        Some("default".to_string()),
        Some("7".to_string()),
        Some(5),
        Some("5e0f8f3b90b05d27".to_string()),
        Some(3),
        Some("7".to_string()),
        Some(5),
        Some("5e0f8f3b90b05d27".to_string()),
    ));

    Pre0224EntitySchemaDescription {
        entity_path: "fixtures::pre_0_224::Account".to_string(),
        entity_name: "Account".to_string(),
        entity_tag: 42,
        accepted_schema_fingerprint_method: 1,
        accepted_schema_fingerprint: [0x24; 16],
        primary_key: "id".to_string(),
        primary_key_fields: vec!["id".to_string()],
        identity: Some(Box::new(Pre0224EntityIdentityDescription {
            field: "id".to_string(),
            generator: "Identity::next".to_string(),
            accepted_kind: "Nat64".to_string(),
            minimum: 1,
            maximum: u128::from(u64::MAX),
            high_water: 17,
            remaining: u128::from(u64::MAX) - 17,
            exhausted: false,
        })),
        fields,
        indexes: vec![
            EntityIndexDescription::new(
                "account_pk".to_string(),
                true,
                vec!["id".to_string()],
                "generated".to_string(),
            ),
            EntityIndexDescription::new(
                "account_display_name_idx".to_string(),
                false,
                vec!["display_name".to_string()],
                "generated".to_string(),
            ),
            EntityIndexDescription::new(
                "account_score_name_idx".to_string(),
                true,
                vec!["legacy_score".to_string(), "display_name".to_string()],
                "ddl".to_string(),
            ),
        ],
        relations: vec![
            EntityRelationDescription::new(
                "friend_id".to_string(),
                "fixtures::pre_0_224::User".to_string(),
                "User".to_string(),
                "stores::accounts".to_string(),
                EntityRelationCardinality::Single,
            ),
            EntityRelationDescription::new(
                "member_ids".to_string(),
                "fixtures::pre_0_224::Group".to_string(),
                "Group".to_string(),
                "stores::accounts".to_string(),
                EntityRelationCardinality::List,
            ),
            EntityRelationDescription::new(
                "watcher_ids".to_string(),
                "fixtures::pre_0_224::User".to_string(),
                "User".to_string(),
                "stores::accounts".to_string(),
                EntityRelationCardinality::Set,
            ),
        ],
        constraints: vec![
            Pre0224EntityConstraintDescription {
                id: 0,
                name: "account_pk".to_string(),
                kind: "primary_key".to_string(),
                origin: "generated".to_string(),
                validation_state: "validated".to_string(),
                validation_progress: None,
                field_id: None,
                index_id: None,
                relation_id: None,
                fields: vec!["id".to_string()],
                index: None,
                relation: None,
                target_entity: None,
                action: None,
                semantics: "primary_key_v1".to_string(),
                check_sql: None,
            },
            Pre0224EntityConstraintDescription {
                id: 8,
                name: "account_score_name_unique".to_string(),
                kind: "unique".to_string(),
                origin: "sql_ddl".to_string(),
                validation_state: "validated".to_string(),
                validation_progress: None,
                field_id: None,
                index_id: Some(7),
                relation_id: None,
                fields: vec!["legacy_score".to_string(), "display_name".to_string()],
                index: Some("account_score_name_idx".to_string()),
                relation: None,
                target_entity: None,
                action: None,
                semantics: "unique_index_v1".to_string(),
                check_sql: None,
            },
            Pre0224EntityConstraintDescription {
                id: 9,
                name: "account_score_nonnegative".to_string(),
                kind: "check".to_string(),
                origin: "sql_ddl".to_string(),
                validation_state: "validated".to_string(),
                validation_progress: None,
                field_id: None,
                index_id: None,
                relation_id: None,
                fields: vec!["legacy_score".to_string()],
                index: None,
                relation: None,
                target_entity: None,
                action: None,
                semantics: "check_expr_v1".to_string(),
                check_sql: Some("legacy_score >= 0".to_string()),
            },
            Pre0224EntityConstraintDescription {
                id: 10,
                name: "account_friend_relation".to_string(),
                kind: "relation".to_string(),
                origin: "generated".to_string(),
                validation_state: "validated".to_string(),
                validation_progress: None,
                field_id: None,
                index_id: None,
                relation_id: Some(0),
                fields: vec!["friend_id".to_string()],
                index: None,
                relation: Some("account_friend".to_string()),
                target_entity: Some("fixtures::pre_0_224::User".to_string()),
                action: Some("restrict".to_string()),
                semantics: "relation_pk_restrict_v1".to_string(),
                check_sql: None,
            },
            Pre0224EntityConstraintDescription {
                id: 11,
                name: "account_display_name_not_null".to_string(),
                kind: "not_null".to_string(),
                origin: "sql_ddl".to_string(),
                validation_state: "validating".to_string(),
                validation_progress: Some(Pre0224ConstraintValidationProgressDescription {
                    phase: "forward".to_string(),
                    rows_scanned: 41,
                    findings_seen: 2,
                    restarts: 1,
                }),
                field_id: Some(1),
                index_id: None,
                relation_id: None,
                fields: vec!["display_name".to_string()],
                index: None,
                relation: None,
                target_entity: None,
                action: None,
                semantics: "not_null_v1".to_string(),
                check_sql: None,
            },
        ],
        row_layout_current: 3,
        row_layout_history_floor: 1,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut child = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("sha256sum should be available for the host-only golden test");
    child
        .stdin
        .take()
        .expect("sha256sum stdin should be piped")
        .write_all(bytes)
        .expect("golden Candid bytes should be writable to sha256sum");
    let output = child
        .wait_with_output()
        .expect("sha256sum should complete for the golden Candid bytes");
    assert!(output.status.success(), "sha256sum should succeed");
    String::from_utf8(output.stdout)
        .expect("sha256sum output should be UTF-8")
        .split_whitespace()
        .next()
        .expect("sha256sum output should contain a digest")
        .to_string()
}

#[test]
fn pre_0_224_verbose_dossier_golden() {
    let expected = pre_0_224_verbose_dossier_fixture();
    let expected_bytes = Encode!(&expected).expect("golden mirror should encode");
    let description = Decode!(&expected_bytes, EntitySchemaDescription)
        .expect("golden mirror should decode as the maintained dossier");
    let actual_bytes = Encode!(&description).expect("maintained dossier should encode");
    let actual = Decode!(&actual_bytes, Pre0224EntitySchemaDescription)
        .expect("maintained dossier should decode as the complete golden mirror");
    let relation_bytes = Encode!(&description.relations().to_vec())
        .expect("maintained relation projection should encode");
    let lines = render_describe_lines(&description);
    let verbose_lines = render_describe_output_lines(&SqlDescribeOutput::Verbose { description });

    assert_eq!(
        actual, expected,
        "the complete typed dossier must remain exact"
    );
    assert_eq!(expected_bytes.len(), PRE_0_224_VERBOSE_DOSSIER_CANDID_BYTES);
    assert_eq!(
        sha256_hex(&expected_bytes),
        PRE_0_224_VERBOSE_DOSSIER_CANDID_SHA256
    );
    assert_ne!(
        actual_bytes, expected_bytes,
        "the maintained dossier intentionally adds optional predicate metadata"
    );
    assert_eq!(actual_bytes.len(), CURRENT_VERBOSE_DOSSIER_CANDID_BYTES);
    assert_eq!(
        sha256_hex(&actual_bytes),
        CURRENT_VERBOSE_DOSSIER_CANDID_SHA256
    );
    assert_eq!(
        relation_bytes.len(),
        PRE_0_224_RELATION_CONTROL_CANDID_BYTES
    );
    assert_eq!(
        sha256_hex(&relation_bytes),
        PRE_0_224_RELATION_CONTROL_CANDID_SHA256
    );
    assert_eq!(
        lines.join("\n"),
        PRE_0_224_VERBOSE_DOSSIER_LINES.trim_end(),
        "verbose shell lines must remain exact",
    );
    assert_eq!(
        verbose_lines, lines,
        "the explicit VERBOSE envelope must preserve the named dossier golden",
    );
}

#[test]
fn sql_introspection_0_224_candid_envelopes_are_exact() {
    let column = SqlColumnSummaryContract {
        name: "profile.name".to_string(),
        field_type: "text(max_len=64)".to_string(),
        nullable: true,
        key: SqlColumnKeyContract::Unique,
        default: SqlColumnDefaultContract::Literal {
            text: "'guest'".to_string(),
        },
        extra: vec![
            SqlColumnExtraContract::Identity,
            SqlColumnExtraContract::Generated,
            SqlColumnExtraContract::Relation,
        ],
    };
    let describe_contract = SqlDescribeOutputContract::Compact {
        entity: "Account".to_string(),
        columns: vec![column.clone()],
    };
    let describe_bytes = Encode!(&describe_contract).expect("describe contract should encode");
    let describe = Decode!(&describe_bytes, SqlDescribeOutput)
        .expect("describe contract should decode through the maintained envelope");
    assert_eq!(
        Encode!(&describe).expect("maintained describe output should encode"),
        describe_bytes,
        "DESCRIBE labels, variants, fields, and nesting must remain exact",
    );
    assert_eq!(
        render_describe_output_lines(&describe),
        vec![
            "+--------------+------------------+----------+-----+---------+-------------------------------+".to_string(),
            "| name         | type             | nullable | key | default | extra                         |".to_string(),
            "+--------------+------------------+----------+-----+---------+-------------------------------+".to_string(),
            "| profile.name | text(max_len=64) | yes      | UNI | 'guest' | identity, generated, relation |".to_string(),
            "+--------------+------------------+----------+-----+---------+-------------------------------+".to_string(),
        ],
    );

    let columns_contract = SqlShowColumnsOutputContract::Compact {
        entity: "Account".to_string(),
        columns: vec![column],
    };
    let columns_bytes = Encode!(&columns_contract).expect("show-columns contract should encode");
    let columns = Decode!(&columns_bytes, SqlShowColumnsOutput)
        .expect("show-columns contract should decode through the maintained envelope");
    assert_eq!(
        Encode!(&columns).expect("maintained show-columns output should encode"),
        columns_bytes,
        "SHOW COLUMNS labels, variants, fields, and nesting must remain exact",
    );

    let relation = EntityRelationDescription::new(
        "owner_id".to_string(),
        "entities::Owner".to_string(),
        "Owner".to_string(),
        "stores::Owner".to_string(),
        EntityRelationCardinality::Single,
    );
    let relations_contract = SqlShowRelationsOutputContract {
        entity: "Account".to_string(),
        relations: vec![relation],
    };
    let relations_bytes =
        Encode!(&relations_contract).expect("show-relations contract should encode");
    let relations = Decode!(&relations_bytes, SqlShowRelationsOutput)
        .expect("show-relations contract should decode through the maintained envelope");
    assert_eq!(
        Encode!(&relations).expect("maintained show-relations output should encode"),
        relations_bytes,
        "SHOW RELATIONS fields and nesting must remain exact",
    );
    assert_eq!(
        render_show_relations_lines(&relations),
        vec![
            "+----------+-----------------+-------------+".to_string(),
            "| field    | target          | cardinality |".to_string(),
            "+----------+-----------------+-------------+".to_string(),
            "| owner_id | entities::Owner | Single      |".to_string(),
            "+----------+-----------------+-------------+".to_string(),
        ],
    );

    let SqlDescribeOutput::Compact { columns, .. } = describe else {
        panic!("decoded DESCRIBE contract must retain its explicit compact mode");
    };
    assert_eq!(columns[0].key(), SqlColumnKey::Unique);
    assert_eq!(
        columns[0].default(),
        &SqlColumnDefault::Literal {
            text: "'guest'".to_string(),
        }
    );
    assert_eq!(
        columns[0].extra(),
        &[
            SqlColumnExtra::Identity,
            SqlColumnExtra::Generated,
            SqlColumnExtra::Relation,
        ]
    );
}

#[test]
fn render_describe_lines_output_contract_vector_is_stable() {
    let description = EntitySchemaDescription::new(
        "schema.public.ExampleEntity".to_string(),
        "ExampleEntity".to_string(),
        9,
        1,
        [0x33; 16],
        "id".to_string(),
        vec!["id".to_string()],
        vec![
            required_field("id", 0, "Ulid", true),
            required_field("name", 1, "Text", false),
        ],
        vec![
            EntityIndexDescription::new(
                "example_entity_name_idx".to_string(),
                false,
                vec!["name".to_string()],
                "ddl".to_string(),
            ),
            EntityIndexDescription::new(
                "example_entity_pk".to_string(),
                true,
                vec!["id".to_string()],
                "generated".to_string(),
            ),
        ],
        vec![EntityRelationDescription::new(
            "mentor_id".to_string(),
            "schema.public.User".to_string(),
            "User".to_string(),
            "user_store".to_string(),
            EntityRelationCardinality::Single,
        )],
        Vec::new(),
        1,
        1,
    );

    assert_eq!(
        render_describe_lines(&description),
        vec![
            "entity: ExampleEntity".to_string(),
            "path: schema.public.ExampleEntity".to_string(),
            "row layout: current=1 history_floor=1".to_string(),
            "identity: none".to_string(),
            String::new(),
            "fields:".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+-----------------+----------------+---------------+--------------+-------------------+-----------------+------------+-----------+".to_string(),
            "| name | slot | type | nullable | pk  | queryable | origin    | insert omission | insert default | default bytes | default hash | introduced layout | historical fill | fill bytes | fill hash |".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+-----------------+----------------+---------------+--------------+-------------------+-----------------+------------+-----------+".to_string(),
            "| id   | 0    | Ulid | no       | yes | yes       | generated | required        | -              | -             | -            | 1                 | reject          | -          | -         |".to_string(),
            "| name | 1    | Text | no       | no  | yes       | generated | required        | -              | -             | -            | 1                 | reject          | -          | -         |".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+-----------------+----------------+---------------+--------------+-------------------+-----------------+------------+-----------+".to_string(),
            String::new(),
            "indexes:".to_string(),
            "+-------------------------+--------+--------+-----------+".to_string(),
            "| name                    | fields | unique | origin    |".to_string(),
            "+-------------------------+--------+--------+-----------+".to_string(),
            "| example_entity_name_idx | name   | no     | ddl       |".to_string(),
            "| example_entity_pk       | id     | yes    | generated |".to_string(),
            "+-------------------------+--------+--------+-----------+".to_string(),
            String::new(),
            "relations:".to_string(),
            "+-----------+--------+-------------+".to_string(),
            "| field     | target | cardinality |".to_string(),
            "+-----------+--------+-------------+".to_string(),
            "| mentor_id | User   | Single      |".to_string(),
            "+-----------+--------+-------------+".to_string(),
            String::new(),
            "constraints: []".to_string(),
        ],
        "describe shell output must remain contract-stable across release lines",
    );
}

#[test]
fn render_show_constraints_lines_empty_contract_is_stable() {
    assert_eq!(
        render_show_constraints_lines("ExampleEntity", &[]),
        vec![
            "entity: ExampleEntity".to_string(),
            String::new(),
            "constraints: []".to_string(),
        ],
    );
}

#[test]
fn sql_query_result_from_statement_preserves_show_constraints_entity() {
    assert_eq!(
        sql_query_result_from_statement(
            SqlStatementResult::ShowConstraints(Vec::new()),
            "ExampleEntity".to_string(),
        ),
        SqlQueryResult::ShowConstraints {
            entity: "ExampleEntity".to_string(),
            constraints: Vec::new(),
        },
    );
}

#[test]
fn render_show_indexes_lines_output_contract_vector_is_stable() {
    let indexes = vec![
        "PRIMARY KEY (id)".to_string(),
        "INDEX example_entity_name_idx(name)".to_string(),
    ];

    assert_eq!(
        render_show_indexes_lines("ExampleEntity", indexes.as_slice()),
        vec![
            "surface=indexes entity=ExampleEntity index_count=2".to_string(),
            "PRIMARY KEY (id)".to_string(),
            "INDEX example_entity_name_idx(name)".to_string(),
        ],
        "show-indexes shell output must remain contract-stable across release lines",
    );
}

#[test]
fn render_show_columns_lines_output_contract_vector_is_stable() {
    let columns = vec![
        required_field("id", 0, "Ulid", true),
        required_field("name", 1, "Text", false),
    ];

    assert_eq!(
        render_show_columns_lines(&SqlShowColumnsOutput::Verbose {
            entity: "ExampleEntity".to_string(),
            columns,
        }),
        vec![
            "entity: ExampleEntity".to_string(),
            String::new(),
            "fields:".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+-----------------+----------------+---------------+--------------+-------------------+-----------------+------------+-----------+".to_string(),
            "| name | slot | type | nullable | pk  | queryable | origin    | insert omission | insert default | default bytes | default hash | introduced layout | historical fill | fill bytes | fill hash |".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+-----------------+----------------+---------------+--------------+-------------------+-----------------+------------+-----------+".to_string(),
            "| id   | 0    | Ulid | no       | yes | yes       | generated | required        | -              | -             | -            | 1                 | reject          | -          | -         |".to_string(),
            "| name | 1    | Text | no       | no  | yes       | generated | required        | -              | -             | -            | 1                 | reject          | -          | -         |".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+-----------------+----------------+---------------+--------------+-------------------+-----------------+------------+-----------+".to_string(),
        ],
        "show-columns shell output must remain contract-stable across release lines",
    );
}

#[test]
fn render_show_entities_lines_output_contract_vector_is_stable() {
    let entities = vec![
        EntityCatalogDescription::new(
            "ExampleEntity".to_string(),
            "schema.public.ExampleEntity".to_string(),
            "stores::main".to_string(),
            "stable".to_string(),
            EntityCatalogCounts::new(2, 1, 0, 1),
        ),
        EntityCatalogDescription::new(
            "Order".to_string(),
            "schema.public.Order".to_string(),
            "stores::sales".to_string(),
            "stable".to_string(),
            EntityCatalogCounts::new(5, 2, 1, 3),
        ),
        EntityCatalogDescription::new(
            "User".to_string(),
            "schema.public.User".to_string(),
            "stores::main".to_string(),
            "journaled".to_string(),
            EntityCatalogCounts::new(4, 0, 2, 4),
        ),
    ];

    assert_eq!(
        render_show_entities_lines(entities.as_slice()),
        vec![
            "+---------------+-------+-----------+------+---------+-----------+----+".to_string(),
            "| name          | store | storage   | cols | indexes | relations | sv |".to_string(),
            "+---------------+-------+-----------+------+---------+-----------+----+".to_string(),
            "| ExampleEntity | main  | stable    | 2    | 1       | 0         | 1  |".to_string(),
            "| Order         | sales | stable    | 5    | 2       | 1         | 3  |".to_string(),
            "| User          | main  | journaled | 4    | 0       | 2         | 4  |".to_string(),
            "+---------------+-------+-----------+------+---------+-----------+----+".to_string(),
            String::new(),
            "3 entities,".to_string(),
        ],
        "show-entities shell output must remain contract-stable across release lines",
    );
}

#[test]
fn render_show_entities_verbose_lines_output_contract_vector_is_stable() {
    let entities = vec![EntityCatalogDescription::new(
        "ExampleEntity".to_string(),
        "schema.public.ExampleEntity".to_string(),
        "stores::main".to_string(),
        "stable".to_string(),
        EntityCatalogCounts::new(2, 1, 0, 1),
    )];

    assert_eq!(
        render_show_entities_verbose_lines(entities.as_slice()),
        vec![
            "+---------------+-----------------------------+--------------+---------+------+---------+-----------+----+".to_string(),
            "| name          | path                        | store        | storage | cols | indexes | relations | sv |".to_string(),
            "+---------------+-----------------------------+--------------+---------+------+---------+-----------+----+".to_string(),
            "| ExampleEntity | schema.public.ExampleEntity | stores::main | stable  | 2    | 1       | 0         | 1  |".to_string(),
            "+---------------+-----------------------------+--------------+---------+------+---------+-----------+----+".to_string(),
            String::new(),
            "1 entity,".to_string(),
        ],
        "verbose show-entities output should keep full paths behind an explicit surface",
    );
}

#[test]
fn render_show_stores_lines_output_contract_vector_is_stable() {
    let stores = vec![
        StoreCatalogDescription::new("stores::main".to_string(), "stable".to_string()),
        StoreCatalogDescription::new("stores::scratch".to_string(), "heap".to_string()),
        StoreCatalogDescription::new("stores::journaled".to_string(), "journaled".to_string()),
    ];

    assert_eq!(
        render_show_stores_lines(stores.as_slice()),
        vec![
            "+-----------+-----------+".to_string(),
            "| store     | storage   |".to_string(),
            "+-----------+-----------+".to_string(),
            "| main      | stable    |".to_string(),
            "| scratch   | heap      |".to_string(),
            "| journaled | journaled |".to_string(),
            "+-----------+-----------+".to_string(),
            String::new(),
            "3 stores,".to_string(),
        ],
        "show-stores shell output must remain contract-stable across release lines",
    );
}

#[test]
fn render_show_stores_verbose_lines_output_contract_vector_is_stable() {
    let stores = vec![StoreCatalogDescription::new(
        "stores::journaled".to_string(),
        "journaled".to_string(),
    )];

    assert_eq!(
        render_show_stores_verbose_lines(stores.as_slice()),
        vec![
            "+-------------------+-----------+".to_string(),
            "| path              | storage   |".to_string(),
            "+-------------------+-----------+".to_string(),
            "| stores::journaled | journaled |".to_string(),
            "+-------------------+-----------+".to_string(),
            String::new(),
            "1 store,".to_string(),
        ],
        "verbose show-stores output should keep full paths behind an explicit surface",
    );
}

#[test]
fn render_show_memory_lines_output_contract_vector_is_stable() {
    let memory = vec![
        MemoryCatalogDescription::new(
            "icydb.demo.main.data.v1".to_string(),
            100,
            "stores::main".to_string(),
        ),
        MemoryCatalogDescription::new(
            "icydb.demo.main.index.v1".to_string(),
            101,
            "stores::main".to_string(),
        ),
    ];

    assert_eq!(
        render_show_memory_lines(memory.as_slice()),
        vec![
            "+--------------------------+-----------+-------+".to_string(),
            "| tag                      | memory_id | store |".to_string(),
            "+--------------------------+-----------+-------+".to_string(),
            "| icydb.demo.main.data.v1  | 100       | main  |".to_string(),
            "| icydb.demo.main.index.v1 | 101       | main  |".to_string(),
            "+--------------------------+-----------+-------+".to_string(),
            String::new(),
            "2 memories,".to_string(),
        ],
        "show-memory shell output should expose stable keys, memory ids, and owning stores",
    );
}

#[test]
fn sql_query_result_projection_render_lines_output_contract_vector_is_stable() {
    let projection = RowProjectionOutput {
        entity: "User".to_string(),
        columns: vec!["name".to_string()],
        rows: vec![vec![text("alice")]],
        row_count: 1,
    };
    let result = SqlQueryResult::Projection(projection);

    assert_eq!(
        result.render_lines(),
        vec![
            "+-------+".to_string(),
            "| name  |".to_string(),
            "+-------+".to_string(),
            "| alice |".to_string(),
            "+-------+".to_string(),
            String::new(),
            "1 row,".to_string(),
        ],
        "projection query-result rendering must remain contract-stable across release lines",
    );
}

#[test]
fn sql_query_result_projection_render_lines_empty_table_omits_trailing_separator() {
    let projection = RowProjectionOutput {
        entity: "User".to_string(),
        columns: vec![
            "name".to_string(),
            "hit_points".to_string(),
            "strength".to_string(),
        ],
        rows: Vec::new(),
        row_count: 0,
    };
    let result = SqlQueryResult::Projection(projection);

    assert_eq!(
        result.render_lines(),
        vec![
            "+------+------------+----------+".to_string(),
            "| name | hit_points | strength |".to_string(),
            "+------+------------+----------+".to_string(),
            String::new(),
            "0 rows,".to_string(),
        ],
        "empty projection tables should stop after the header separator instead of rendering a duplicate closing border",
    );
}

#[test]
fn sql_query_result_grouped_render_lines_output_contract_vector_is_stable() {
    let grouped = SqlGroupedRowsOutput {
        entity: "User".to_string(),
        columns: vec!["age".to_string(), "count(*)".to_string()],
        rows: vec![
            vec!["24".to_string(), "1".to_string()],
            vec!["31".to_string(), "2".to_string()],
        ],
        row_count: 2,
        next_cursor: Some("cursor:age:31".to_string()),
    };
    let result = SqlQueryResult::Grouped(grouped);

    assert_eq!(
        result.render_lines(),
        vec![
            "next_cursor=cursor:age:31".to_string(),
            "+-----+----------+".to_string(),
            "| age | count(*) |".to_string(),
            "+-----+----------+".to_string(),
            "| 24  | 1        |".to_string(),
            "| 31  | 2        |".to_string(),
            "+-----+----------+".to_string(),
            String::new(),
            "2 rows,".to_string(),
        ],
        "grouped query-result rendering must remain contract-stable across release lines",
    );
}

#[test]
fn sql_query_result_row_count_footer_uses_grouped_decimal_formatting() {
    let projection = RowProjectionOutput {
        entity: "User".to_string(),
        columns: vec!["name".to_string()],
        rows: Vec::new(),
        row_count: 1_234,
    };
    let result = SqlQueryResult::Projection(projection);

    assert_eq!(
        result.render_lines().last(),
        Some(&"1,234 rows,".to_string()),
        "row-count footers should use grouped decimal formatting for large result sets",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_count_entity_and_row_count() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Count { row_count: 3 },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Count {
            entity: "User".to_string(),
            row_count: 3,
        },
        "public SQL packaging must preserve outward count payload identity",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_text_projection_values() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["lower(name)".to_string()],
            fixed_scales: vec![None],
            rows: vec![vec![text("alice")], vec![text("bob")]],
            row_count: 2,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "User".to_string(),
            columns: vec!["lower(name)".to_string()],
            rows: vec![vec![text("alice")], vec![text("bob")]],
            row_count: 2,
        }),
        "public SQL packaging must preserve text projection values as semantic output values",
    );
}

#[test]
fn sql_query_result_from_statement_keeps_blob_projection_typed_until_rendering() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["thumbnail".to_string()],
            fixed_scales: vec![None],
            rows: vec![vec![OutputValue::Blob(vec![0xab, 0xcd])]],
            row_count: 1,
        },
        "Blob".to_string(),
    );

    let SqlQueryResult::Projection(rows) = result else {
        panic!("blob projection should remain a projection payload");
    };
    assert_eq!(
        rows.rows,
        vec![vec![OutputValue::Blob(vec![0xab, 0xcd])]],
        "SQL projection packaging should not pre-render blob payloads as hex text",
    );
    assert_eq!(
        rows.rendered_rows(),
        vec![vec!["0xabcd".to_string()]],
        "display rendering should still expose the stable hex representation when explicitly requested",
    );
}

#[test]
fn sql_query_result_blob_projection_candid_payload_stays_binary_not_hex() {
    let blob = vec![0xab; 4_096];
    let typed = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "Blob".to_string(),
        columns: vec!["thumbnail".to_string()],
        rows: vec![vec![OutputValue::Blob(blob.clone())]],
        row_count: 1,
    });
    let rendered = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "Blob".to_string(),
        columns: vec!["thumbnail".to_string()],
        rows: vec![vec![text(
            format!("0x{}", "ab".repeat(blob.len())).as_str(),
        )]],
        row_count: 1,
    });

    let typed_len = Encode!(&typed)
        .expect("typed blob projection should encode")
        .len();
    let rendered_len = Encode!(&rendered)
        .expect("rendered blob projection should encode")
        .len();

    assert!(
        rendered_len.saturating_sub(typed_len) >= blob.len(),
        "binary blob projections should avoid the old hex-text payload expansion: typed={typed_len}, rendered={rendered_len}"
    );
}

#[test]
fn sql_query_result_from_statement_preserves_semantic_projection_value_variants() {
    let float32 = Float32::try_new(1.25).expect("finite f32");
    let float64 = Float64::try_new(2.5).expect("finite f64");
    let decimal = Decimal::new(1234, 2);
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec![
                "nat_value".to_string(),
                "int_value".to_string(),
                "decimal_value".to_string(),
                "float32_value".to_string(),
                "float64_value".to_string(),
                "optional_value".to_string(),
            ],
            fixed_scales: vec![None, None, None, None, None, None],
            rows: vec![vec![
                OutputValue::Nat64(7),
                OutputValue::Int64(-3),
                OutputValue::Decimal(decimal),
                OutputValue::Float32(float32),
                OutputValue::Float64(float64),
                OutputValue::Null,
            ]],
            row_count: 1,
        },
        "Scalar".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "Scalar".to_string(),
            columns: vec![
                "nat_value".to_string(),
                "int_value".to_string(),
                "decimal_value".to_string(),
                "float32_value".to_string(),
                "float64_value".to_string(),
                "optional_value".to_string(),
            ],
            rows: vec![vec![
                OutputValue::Nat64(7),
                OutputValue::Int64(-3),
                OutputValue::Decimal(decimal),
                OutputValue::Float32(float32),
                OutputValue::Float64(float64),
                OutputValue::Null,
            ]],
            row_count: 1,
        }),
        "public SQL projection packaging should preserve semantic output value variants until explicit display rendering",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_scalar_arithmetic_and_round_projection_rows() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["age - 1".to_string(), "ROUND(age / 3, 2)".to_string()],
            fixed_scales: vec![None, Some(2)],
            rows: vec![
                vec![
                    OutputValue::Decimal(Decimal::from_i128(23).expect("23 decimal")),
                    OutputValue::Decimal(Decimal::new(800, 2)),
                ],
                vec![
                    OutputValue::Decimal(Decimal::from_i128(30).expect("30 decimal")),
                    OutputValue::Decimal(Decimal::new(1033, 2)),
                ],
            ],
            row_count: 2,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "User".to_string(),
            columns: vec!["age - 1".to_string(), "ROUND(age / 3, 2)".to_string()],
            rows: vec![
                vec![
                    OutputValue::Decimal(Decimal::from_i128(23).expect("23 decimal")),
                    text("8.00"),
                ],
                vec![
                    OutputValue::Decimal(Decimal::from_i128(30).expect("30 decimal")),
                    text("10.33"),
                ],
            ],
            row_count: 2,
        }),
        "public SQL packaging must preserve arithmetic and ROUND projection labels and rendered decimal rows",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_fixed_scale_for_zero_round_projection_rows() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["ROUND(age / 10, 3)".to_string()],
            fixed_scales: vec![Some(3)],
            rows: vec![vec![OutputValue::Decimal(Decimal::ZERO)]],
            row_count: 1,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "User".to_string(),
            columns: vec!["ROUND(age / 10, 3)".to_string()],
            rows: vec![vec![text("0.000")]],
            row_count: 1,
        }),
        "public SQL packaging must keep ROUND projection scale even for zero values",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_fixed_scale_for_aliased_round_projection_rows() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["dextrisma".to_string()],
            fixed_scales: vec![Some(3)],
            rows: vec![vec![OutputValue::Decimal(
                Decimal::from_i128(16).expect("16 decimal"),
            )]],
            row_count: 1,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "User".to_string(),
            columns: vec!["dextrisma".to_string()],
            rows: vec![vec![text("16.000")]],
            row_count: 1,
        }),
        "public SQL packaging must preserve aliased ROUND projection scale even when the outward label no longer exposes ROUND(..., scale)",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_fixed_scale_for_grouped_round_rows() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Grouped {
            columns: vec!["age".to_string(), "ROUND(AVG(age), 4)".to_string()],
            fixed_scales: vec![None, Some(4)],
            rows: vec![
                GroupedRow::new(
                    vec![OutputValue::Nat64(12)],
                    vec![OutputValue::Decimal(
                        Decimal::from_i128(12).expect("12 decimal"),
                    )],
                ),
                GroupedRow::new(
                    vec![OutputValue::Nat64(14)],
                    vec![OutputValue::Decimal(Decimal::new(142_000, 4))],
                ),
            ],
            row_count: 2,
            next_cursor: None,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Grouped(SqlGroupedRowsOutput {
            entity: "User".to_string(),
            columns: vec!["age".to_string(), "ROUND(AVG(age), 4)".to_string()],
            rows: vec![
                vec!["12".to_string(), "12.0000".to_string()],
                vec!["14".to_string(), "14.2000".to_string()],
            ],
            row_count: 2,
            next_cursor: None,
        }),
        "public grouped SQL packaging must preserve fixed ROUND projection scale for grouped rows",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_grouped_rows_and_cursor() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Grouped {
            columns: vec!["age".to_string(), "count(*)".to_string()],
            fixed_scales: vec![None, None],
            rows: vec![
                GroupedRow::new(vec![OutputValue::Nat64(24)], vec![OutputValue::Nat64(1)]),
                GroupedRow::new(vec![OutputValue::Nat64(31)], vec![OutputValue::Nat64(2)]),
            ],
            row_count: 2,
            next_cursor: Some("cursor:age:31".to_string()),
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Grouped(SqlGroupedRowsOutput {
            entity: "User".to_string(),
            columns: vec!["age".to_string(), "count(*)".to_string()],
            rows: vec![
                vec!["24".to_string(), "1".to_string()],
                vec!["31".to_string(), "2".to_string()],
            ],
            row_count: 2,
            next_cursor: Some("cursor:age:31".to_string()),
        }),
        "public SQL packaging must preserve grouped rows and outward continuation cursor",
    );
}

#[test]
fn sql_query_result_renders_ddl_publication_payload() {
    let result = SqlQueryResult::Ddl {
        entity: "User".to_string(),
        mutation_kind: "add_field_path_index".to_string(),
        target_index: "user_age_idx".to_string(),
        target_store: "test::User::user_age_idx".to_string(),
        field_path: vec!["age".to_string()],
        status: "published".to_string(),
        rows_scanned: 3,
        index_keys_written: 3,
        constraint_validation: None,
    };

    assert_eq!(
        result.render_lines(),
        vec![
            "surface=ddl entity=User mutation_kind=add_field_path_index target_index=user_age_idx target_store=test::User::user_age_idx field_path=age status=published rows_scanned=3 index_keys_written=3".to_string()
        ],
        "public SQL DDL payloads should render a stable developer diagnostic line",
    );
}
