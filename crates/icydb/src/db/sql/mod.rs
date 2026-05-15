//! Defines the public SQL text/result payload types exposed by the facade crate.
//!
//! This module consumes already-executed SQL outputs and renders stable
//! endpoint-friendly row payloads; parsing and execution stay in `icydb-core`.

mod convert;
mod table_render;
mod types;
mod value_render;

pub(crate) use convert::sql_query_result_from_statement;
pub use table_render::{
    render_count_lines, render_describe_lines, render_explain_lines, render_grouped_lines,
    render_projection_lines, render_show_columns_lines, render_show_entities_lines,
    render_show_indexes_lines,
};
pub use types::{SqlGroupedRowsOutput, SqlProjectionRows, SqlQueryResult, SqlQueryRowsOutput};
pub use value_render::render_value_text;

//
// TESTS
//

#[cfg(test)]
mod tests {
    use icydb_core::db::{GroupedRow, SqlStatementResult};
    use icydb_core::types::Decimal;

    use crate::__macro::Value;
    use crate::db::sql::{
        SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput, render_describe_lines,
        render_show_columns_lines, render_show_entities_lines, render_show_indexes_lines,
        sql_query_result_from_statement,
    };
    use crate::db::{
        EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
        EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
    };

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
                    true,
                    true,
                ),
                EntityFieldDescription::new(
                    "name".to_string(),
                    Some(1),
                    "Text".to_string(),
                    false,
                    true,
                ),
            ],
            vec![
                EntityIndexDescription::new(
                    "example_entity_name_idx".to_string(),
                    false,
                    vec!["name".to_string()],
                ),
                EntityIndexDescription::new(
                    "example_entity_pk".to_string(),
                    true,
                    vec!["id".to_string()],
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
                "+------+------+------+-----+-----------+".to_string(),
                "| name | slot | type | pk  | queryable |".to_string(),
                "+------+------+------+-----+-----------+".to_string(),
                "| id   | 0    | Ulid | yes | yes       |".to_string(),
                "| name | 1    | Text | no  | yes       |".to_string(),
                "+------+------+------+-----+-----------+".to_string(),
                String::new(),
                "indexes:".to_string(),
                "+-------------------------+--------+--------+".to_string(),
                "| name                    | fields | unique |".to_string(),
                "+-------------------------+--------+--------+".to_string(),
                "| example_entity_name_idx | name   | no     |".to_string(),
                "| example_entity_pk       | id     | yes    |".to_string(),
                "+-------------------------+--------+--------+".to_string(),
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
            EntityFieldDescription::new("id".to_string(), Some(0), "Ulid".to_string(), true, true),
            EntityFieldDescription::new(
                "name".to_string(),
                Some(1),
                "Text".to_string(),
                false,
                true,
            ),
        ];

        assert_eq!(
            render_show_columns_lines("ExampleEntity", columns.as_slice()),
            vec![
                "entity: ExampleEntity".to_string(),
                String::new(),
                "fields:".to_string(),
                "+------+------+------+-----+-----------+".to_string(),
                "| name | slot | type | pk  | queryable |".to_string(),
                "+------+------+------+-----+-----------+".to_string(),
                "| id   | 0    | Ulid | yes | yes       |".to_string(),
                "| name | 1    | Text | no  | yes       |".to_string(),
                "+------+------+------+-----+-----------+".to_string(),
            ],
            "show-columns shell output must remain contract-stable across release lines",
        );
    }

    #[test]
    fn render_show_entities_lines_output_contract_vector_is_stable() {
        let entities = vec![
            "ExampleEntity".to_string(),
            "Order".to_string(),
            "User".to_string(),
        ];

        assert_eq!(
            render_show_entities_lines(entities.as_slice()),
            vec![
                "tables:".to_string(),
                "+---------------+".to_string(),
                "| name          |".to_string(),
                "+---------------+".to_string(),
                "| ExampleEntity |".to_string(),
                "| Order         |".to_string(),
                "| User          |".to_string(),
                "+---------------+".to_string(),
                String::new(),
                "3 tables,".to_string(),
            ],
            "show-entities shell output must remain contract-stable across release lines",
        );
    }

    #[test]
    fn sql_query_result_projection_render_lines_output_contract_vector_is_stable() {
        let projection = SqlQueryRowsOutput {
            entity: "User".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["alice".to_string()]],
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
    fn sql_query_result_from_statement_preserves_projection_text_rows() {
        let result = sql_query_result_from_statement(
            SqlStatementResult::ProjectionText {
                columns: vec!["lower(name)".to_string()],
                rows: vec![vec!["alice".to_string()], vec!["bob".to_string()]],
                row_count: 2,
            },
            "User".to_string(),
        );

        assert_eq!(
            result,
            SqlQueryResult::Projection(SqlQueryRowsOutput {
                entity: "User".to_string(),
                columns: vec!["lower(name)".to_string()],
                rows: vec![vec!["alice".to_string()], vec!["bob".to_string()]],
                row_count: 2,
            }),
            "public SQL packaging must preserve text projection payloads verbatim",
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
                    vec!["23".to_string(), "8.00".to_string()],
                    vec!["30".to_string(), "10.33".to_string()],
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
                rows: vec![vec!["0.000".to_string()]],
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
                rows: vec![vec!["16.000".to_string()]],
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
                        vec![Value::Nat(12)],
                        vec![Value::Decimal(Decimal::from_i128(12).expect("12 decimal"))],
                    ),
                    GroupedRow::new(
                        vec![Value::Nat(14)],
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
                    GroupedRow::new(vec![Value::Nat(24)], vec![Value::Nat(1)]),
                    GroupedRow::new(vec![Value::Nat(31)], vec![Value::Nat(2)]),
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
            mutation_kind: "add_non_unique_field_path_index".to_string(),
            target_index: "user_age_idx".to_string(),
            target_store: "test::User::user_age_idx".to_string(),
            field_path: vec!["age".to_string()],
            status: "published".to_string(),
        };

        assert_eq!(
            result.render_lines(),
            vec![
                "surface=ddl entity=User mutation_kind=add_non_unique_field_path_index target_index=user_age_idx target_store=test::User::user_age_idx field_path=age status=published".to_string()
            ],
            "public SQL DDL payloads should render a stable developer diagnostic line",
        );
    }
}
