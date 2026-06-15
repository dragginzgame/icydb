use candid::Encode;
use icydb_core::db::{GroupedRow, SqlStatementResult};
use icydb_core::types::{Decimal, Float32, Float64};

use crate::__macro::Value;
use crate::db::sql::{
    SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput, render_describe_lines,
    render_show_columns_lines, render_show_entities_lines, render_show_entities_verbose_lines,
    render_show_indexes_lines, render_show_memory_lines, render_show_stores_lines,
    render_show_stores_verbose_lines, sql_query_result_from_statement,
};
use crate::db::{
    EntityCatalogCounts, EntityCatalogDescription, EntityFieldDescription, EntityIndexDescription,
    EntityRelationCardinality, EntityRelationDescription, EntityRelationStrength,
    EntitySchemaDescription, MemoryCatalogDescription, StoreCatalogDescription,
};
use crate::value::OutputValue;

fn text(value: &str) -> OutputValue {
    OutputValue::Text(value.to_string())
}

#[test]
fn render_describe_lines_output_contract_vector_is_stable() {
    let description = EntitySchemaDescription::new(
        "schema.public.ExampleEntity".to_string(),
        "ExampleEntity".to_string(),
        "id".to_string(),
        vec![
            EntityFieldDescription::new(
                "id".to_string(),
                Some(0),
                "Ulid".to_string(),
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
            EntityRelationStrength::Strong,
            EntityRelationCardinality::Single,
        )],
    );

    assert_eq!(
        render_describe_lines(&description),
        vec![
            "entity: ExampleEntity".to_string(),
            "path: schema.public.ExampleEntity".to_string(),
            String::new(),
            "fields:".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+".to_string(),
            "| name | slot | type | nullable | pk  | queryable | origin    |".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+".to_string(),
            "| id   | 0    | Ulid | no       | yes | yes       | generated |".to_string(),
            "| name | 1    | Text | no       | no  | yes       | generated |".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+".to_string(),
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
            "+-----------+--------+----------+-------------+".to_string(),
            "| field     | target | strength | cardinality |".to_string(),
            "+-----------+--------+----------+-------------+".to_string(),
            "| mentor_id | User   | Strong   | Single      |".to_string(),
            "+-----------+--------+----------+-------------+".to_string(),
        ],
        "describe shell output must remain contract-stable across release lines",
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
        EntityFieldDescription::new(
            "id".to_string(),
            Some(0),
            "Ulid".to_string(),
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
    ];

    assert_eq!(
        render_show_columns_lines("ExampleEntity", columns.as_slice()),
        vec![
            "entity: ExampleEntity".to_string(),
            String::new(),
            "fields:".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+".to_string(),
            "| name | slot | type | nullable | pk  | queryable | origin    |".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+".to_string(),
            "| id   | 0    | Ulid | no       | yes | yes       | generated |".to_string(),
            "| name | 1    | Text | no       | no  | yes       | generated |".to_string(),
            "+------+------+------+----------+-----+-----------+-----------+".to_string(),
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
    let projection = SqlQueryRowsOutput {
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
    let projection = SqlQueryRowsOutput {
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
    let projection = SqlQueryRowsOutput {
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
        SqlQueryResult::Projection(SqlQueryRowsOutput {
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
    let typed = SqlQueryResult::Projection(SqlQueryRowsOutput {
        entity: "Blob".to_string(),
        columns: vec!["thumbnail".to_string()],
        rows: vec![vec![OutputValue::Blob(blob.clone())]],
        row_count: 1,
    });
    let rendered = SqlQueryResult::Projection(SqlQueryRowsOutput {
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
                Value::Nat64(7).into(),
                Value::Int64(-3).into(),
                Value::Decimal(decimal).into(),
                Value::Float32(float32).into(),
                Value::Float64(float64).into(),
                Value::Null.into(),
            ]],
            row_count: 1,
        },
        "Scalar".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(SqlQueryRowsOutput {
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
                    Value::Decimal(Decimal::from_i128(23).expect("23 decimal")).into(),
                    Value::Decimal(Decimal::new(800, 2)).into(),
                ],
                vec![
                    Value::Decimal(Decimal::from_i128(30).expect("30 decimal")).into(),
                    Value::Decimal(Decimal::new(1033, 2)).into(),
                ],
            ],
            row_count: 2,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(SqlQueryRowsOutput {
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
            rows: vec![vec![Value::Decimal(Decimal::ZERO).into()]],
            row_count: 1,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(SqlQueryRowsOutput {
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
            rows: vec![vec![
                Value::Decimal(Decimal::from_i128(16).expect("16 decimal")).into(),
            ]],
            row_count: 1,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(SqlQueryRowsOutput {
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
                    vec![Value::Nat64(12)],
                    vec![Value::Decimal(Decimal::from_i128(12).expect("12 decimal"))],
                ),
                GroupedRow::new(
                    vec![Value::Nat64(14)],
                    vec![Value::Decimal(Decimal::new(142_000, 4))],
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
                GroupedRow::new(vec![Value::Nat64(24)], vec![Value::Nat64(1)]),
                GroupedRow::new(vec![Value::Nat64(31)], vec![Value::Nat64(2)]),
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
    };

    assert_eq!(
        result.render_lines(),
        vec![
            "surface=ddl entity=User mutation_kind=add_field_path_index target_index=user_age_idx target_store=test::User::user_age_idx field_path=age status=published rows_scanned=3 index_keys_written=3".to_string()
        ],
        "public SQL DDL payloads should render a stable developer diagnostic line",
    );
}
