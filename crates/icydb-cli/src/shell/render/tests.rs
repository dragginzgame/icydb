//! SQL terminal reports over native database DTOs.
use super::reports::*;
use crate::shell::render::render_lines;
use icydb::db::sql::{SqlGroupedRowsOutput, SqlQueryResult};
use icydb::db::{
    EntityCatalogCounts, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription,
};
use icydb::db::{
    EntityCatalogDescription, EntityFieldDescription, EntitySchemaDescription,
    MemoryCatalogDescription, RowProjectionOutput, SqlColumnDefault, SqlColumnExtra, SqlColumnKey,
    SqlDescribeOutput, SqlShowColumnsOutput, SqlShowRelationsOutput, StoreCatalogDescription,
};
use icydb::value::OutputValue;
use serde::Deserialize;

fn text(value: &str) -> OutputValue {
    OutputValue::text(value.to_string())
}
const CURRENT_VERBOSE_DOSSIER_WIRE_BYTES: usize = 1_522;
const CURRENT_VERBOSE_DOSSIER_WIRE_SHA256: &str =
    "babba037099bc776e1425d9ce00210975cb92a2b1d28e76340ae77ae01f5113d";
const CURRENT_RELATION_CONTROL_WIRE_BYTES: usize = 180;
const CURRENT_RELATION_CONTROL_WIRE_SHA256: &str =
    "046895bf23b5bb0f97d7e658d1c808f96e66afafb2e5ab7208ecf0abec2425b4";
const CURRENT_VERBOSE_DOSSIER_LINES: &str = include_str!("fixtures/verbose_dossier.lines");

// Mirror the complete current public wire, including fields without public constructors.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
struct EntitySchemaDescriptionContract {
    entity_path: String,
    entity_name: String,
    entity_tag: u64,
    accepted_schema_fingerprint_method: u8,
    accepted_schema_fingerprint: [u8; 16],
    primary_key: String,
    primary_key_fields: Vec<String>,
    identity: Option<Box<EntityIdentityDescriptionContract>>,
    fields: Vec<EntityFieldDescription>,
    indexes: Vec<EntityIndexDescription>,
    relations: Vec<EntityRelationDescription>,
    constraints: Vec<EntityConstraintDescriptionContract>,
    row_layout_current: u32,
    row_layout_history_floor: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
struct EntityIdentityDescriptionContract {
    field: String,
    generator: String,
    accepted_kind: String,
    minimum: u128,
    maximum: u128,
    high_water: u128,
    remaining: u128,
    exhausted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
struct EntityConstraintDescriptionContract {
    id: u32,
    name: String,
    kind: String,
    origin: String,
    validation_state: String,
    validation_progress: Option<ConstraintValidationProgressContract>,
    field_id: Option<u32>,
    index_id: Option<u32>,
    relation_id: Option<u32>,
    fields: Vec<String>,
    index: Option<String>,
    predicate_sql: Option<String>,
    relation: Option<String>,
    target_entity: Option<String>,
    action: Option<String>,
    semantics: String,
    check_sql: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
struct ConstraintValidationProgressContract {
    phase: String,
    rows_scanned: u64,
    findings_seen: u64,
    restarts: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
enum SqlColumnKeyContract {
    Primary,
    Unique,
    Multiple,
    None,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
enum SqlColumnDefaultContract {
    Auto,
    Null,
    Literal { text: String },
    Required,
    NotApplicable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
enum SqlColumnExtraContract {
    Identity,
    Generated,
    Relation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
struct SqlColumnSummaryContract {
    name: String,
    field_type: String,
    nullable: bool,
    key: SqlColumnKeyContract,
    default: SqlColumnDefaultContract,
    extra: Vec<SqlColumnExtraContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
enum SqlDescribeOutputContract {
    Compact {
        entity: String,
        columns: Vec<SqlColumnSummaryContract>,
    },
    Verbose {
        description: EntitySchemaDescription,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
struct SqlShowRelationsOutputContract {
    entity: String,
    relations: Vec<EntityRelationDescription>,
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

fn dossier_field(
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
    reason = "the named golden keeps the complete current dossier visible in one fixture"
)]
fn verbose_dossier_fixture() -> EntitySchemaDescriptionContract {
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
        dossier_field(
            "display_name",
            Some(1),
            "Text(64)",
            false,
            false,
            true,
            "generated",
        ),
        dossier_field(
            "profile",
            Some(2),
            "Composite(Profile)",
            true,
            false,
            true,
            "generated",
        ),
        dossier_field(
            "profile.nickname",
            None,
            "Text(32)",
            true,
            false,
            true,
            "generated",
        ),
        dossier_field(
            "profile.rank",
            None,
            "Nat16",
            true,
            false,
            true,
            "generated",
        ),
        dossier_field(
            "friend_id",
            Some(3),
            "Nat64",
            true,
            false,
            true,
            "generated",
        ),
        dossier_field(
            "member_ids",
            Some(4),
            "List<Nat64>(16)",
            false,
            false,
            true,
            "generated",
        ),
        dossier_field(
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

    EntitySchemaDescriptionContract {
        entity_path: "fixtures::describe::Account".to_string(),
        entity_name: "Account".to_string(),
        entity_tag: 42,
        accepted_schema_fingerprint_method: 1,
        accepted_schema_fingerprint: [0x24; 16],
        primary_key: "id".to_string(),
        primary_key_fields: vec!["id".to_string()],
        identity: Some(Box::new(EntityIdentityDescriptionContract {
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
                "fixtures::describe::User".to_string(),
                "User".to_string(),
                "stores::accounts".to_string(),
                EntityRelationCardinality::Single,
            ),
            EntityRelationDescription::new(
                "member_ids".to_string(),
                "fixtures::describe::Group".to_string(),
                "Group".to_string(),
                "stores::accounts".to_string(),
                EntityRelationCardinality::List,
            ),
            EntityRelationDescription::new(
                "watcher_ids".to_string(),
                "fixtures::describe::User".to_string(),
                "User".to_string(),
                "stores::accounts".to_string(),
                EntityRelationCardinality::Set,
            ),
        ],
        constraints: vec![
            EntityConstraintDescriptionContract {
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
                predicate_sql: None,
                relation: None,
                target_entity: None,
                action: None,
                semantics: "primary_key_v1".to_string(),
                check_sql: None,
            },
            EntityConstraintDescriptionContract {
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
                predicate_sql: None,
                relation: None,
                target_entity: None,
                action: None,
                semantics: "unique_index_v1".to_string(),
                check_sql: None,
            },
            EntityConstraintDescriptionContract {
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
                predicate_sql: None,
                relation: None,
                target_entity: None,
                action: None,
                semantics: "check_expr_v1".to_string(),
                check_sql: Some("legacy_score >= 0".to_string()),
            },
            EntityConstraintDescriptionContract {
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
                predicate_sql: None,
                relation: Some("account_friend".to_string()),
                target_entity: Some("fixtures::describe::User".to_string()),
                action: Some("restrict".to_string()),
                semantics: "relation_pk_restrict_v1".to_string(),
                check_sql: None,
            },
            EntityConstraintDescriptionContract {
                id: 11,
                name: "account_display_name_not_null".to_string(),
                kind: "not_null".to_string(),
                origin: "sql_ddl".to_string(),
                validation_state: "validating".to_string(),
                validation_progress: Some(ConstraintValidationProgressContract {
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
                predicate_sql: None,
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
    ic_host_artifacts::artifact::Sha256Digest::compute(bytes).to_string()
}

#[test]
fn verbose_dossier_golden() {
    let expected = verbose_dossier_fixture();
    let expected_bytes =
        icydb_schema::wire::encode_one(&expected).expect("golden mirror should encode");
    let description = icydb_schema::wire::decode_one::<EntitySchemaDescription>(&expected_bytes)
        .expect("golden mirror should decode as the maintained dossier");
    let actual_bytes =
        icydb_schema::wire::encode_one(&description).expect("maintained dossier should encode");
    let actual = icydb_schema::wire::decode_one::<EntitySchemaDescriptionContract>(&actual_bytes)
        .expect("maintained dossier should decode as the complete golden mirror");
    let relation_bytes = icydb_schema::wire::encode_one(description.relations())
        .expect("maintained relation projection should encode");
    let lines = render_describe_lines(&description);
    let constraint_lines =
        render_show_constraints_lines(description.entity_name(), description.constraints());
    let verbose_lines = render_describe_output_lines(&SqlDescribeOutput::Verbose { description });

    assert_eq!(
        actual, expected,
        "the complete typed dossier must remain exact"
    );
    assert_eq!(
        actual_bytes, expected_bytes,
        "current contract encoding must be exact"
    );
    assert_eq!(actual_bytes.len(), CURRENT_VERBOSE_DOSSIER_WIRE_BYTES);
    assert_eq!(
        sha256_hex(&actual_bytes),
        CURRENT_VERBOSE_DOSSIER_WIRE_SHA256
    );
    assert_eq!(relation_bytes.len(), CURRENT_RELATION_CONTROL_WIRE_BYTES);
    assert_eq!(
        sha256_hex(&relation_bytes),
        CURRENT_RELATION_CONTROL_WIRE_SHA256
    );
    assert_eq!(
        lines.join("\n"),
        CURRENT_VERBOSE_DOSSIER_LINES.trim_end(),
        "verbose shell lines must remain exact",
    );
    assert_eq!(
        verbose_lines, lines,
        "the explicit VERBOSE envelope must preserve the named dossier golden",
    );
    let (_, constraint_section) = CURRENT_VERBOSE_DOSSIER_LINES
        .trim_end()
        .split_once("constraints:\n")
        .expect("the dossier golden should contain populated constraints");
    assert_eq!(
        constraint_lines[2..].join("\n"),
        format!("constraints:\n{constraint_section}"),
        "SHOW CONSTRAINTS must preserve the complete current constraint table",
    );
}

#[test]
fn sql_introspection_wire_envelopes_are_exact() {
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
    let describe_bytes = icydb_schema::wire::encode_one(&describe_contract)
        .expect("describe contract should encode");
    let describe = icydb_schema::wire::decode_one::<SqlDescribeOutput>(&describe_bytes)
        .expect("describe contract should decode through the maintained envelope");
    assert_eq!(
        icydb_schema::wire::encode_one(&describe)
            .expect("maintained describe output should encode"),
        describe_bytes,
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
    let columns_bytes = icydb_schema::wire::encode_one(&columns_contract)
        .expect("show-columns contract should encode");
    let columns = icydb_schema::wire::decode_one::<SqlShowColumnsOutput>(&columns_bytes)
        .expect("show-columns contract should decode through the maintained envelope");
    assert_eq!(
        icydb_schema::wire::encode_one(&columns)
            .expect("maintained show-columns output should encode"),
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
    let relations_bytes = icydb_schema::wire::encode_one(&relations_contract)
        .expect("show-relations contract should encode");
    let relations = icydb_schema::wire::decode_one::<SqlShowRelationsOutput>(&relations_bytes)
        .expect("show-relations contract should decode through the maintained envelope");
    assert_eq!(
        icydb_schema::wire::encode_one(&relations)
            .expect("maintained show-relations output should encode"),
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
        render_lines(&result),
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
        render_lines(&result),
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
        render_lines(&result),
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
        render_lines(&result).last(),
        Some(&"1,234 rows,".to_string()),
        "row-count footers should use grouped decimal formatting for large result sets",
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
        render_lines(&result),
        vec![
            "surface=ddl entity=User mutation_kind=add_field_path_index target_index=user_age_idx target_store=test::User::user_age_idx field_path=age status=published rows_scanned=3 index_keys_written=3".to_string()
        ],
        "public SQL DDL payloads should render a stable developer diagnostic line",
    );
}

#[test]
fn sql_tables_escape_cells_before_display_width_alignment() {
    use crate::shell::render::render_text;
    let result = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "Entry".into(),
        columns: vec!["name\n".into(), "value".into()],
        rows: vec![
            vec![text("界"), text("e\u{301}")],
            vec![text("x\x1b]0;title\x07\r\t"), text("\u{85}\u{202e}")],
        ],
        row_count: 2,
    });
    let before = result.clone();
    let rendered = render_text(&result);
    assert_eq!(result, before, "presentation must preserve native DTOs");
    assert!(!rendered.contains(['\x1b', '\x07', '\r', '\t', '\u{85}', '\u{202e}']));
    assert!(rendered.contains("name\\n"));
    assert!(rendered.contains("x\\u{1b}]0;title\\u{7}\\r\\t"));
    let rows = rendered
        .lines()
        .filter(|line| line.starts_with('|'))
        .collect::<Vec<_>>();
    assert_eq!(
        rows.len(),
        3,
        "embedded controls cannot create terminal rows"
    );
    assert!(rows[1].contains("界"));
    assert!(rows[1].contains("e\u{301}"));
}

#[test]
fn grouped_terminal_display_keeps_sql_cells_and_cursor_bytes_opaque() {
    use crate::shell::render::render_text;
    let grouped = SqlGroupedRowsOutput {
        entity: "Entry".into(),
        columns: vec!["key".into(), "value".into()],
        rows: vec![
            vec!["NULL".into(), "'null'".into()],
            vec!["x\ny".into(), "0.000".into()],
        ],
        row_count: 2,
        next_cursor: Some("opaque-authenticated-cursor".into()),
    };
    let result = SqlQueryResult::Grouped(grouped.clone());
    let output = render_text(&result);
    assert!(output.starts_with("next_cursor=opaque-authenticated-cursor\n"));
    assert!(output.contains("| NULL"));
    assert!(output.contains("'null'"));
    assert!(output.contains("x\\ny"));
    assert!(output.contains("0.000"));
    assert_eq!(result, SqlQueryResult::Grouped(grouped));
}
