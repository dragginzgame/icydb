use super::*;
use crate::value::Value;

fn literal(value: i64) -> SqlExpr {
    SqlExpr::Literal(Value::Int64(value))
}

fn field(name: &str) -> SqlExpr {
    SqlExpr::Field(name.to_string())
}

fn aliased_field(alias: &str, name: &str) -> SqlExpr {
    SqlExpr::FieldPath {
        root: alias.to_string(),
        segments: vec![name.to_string()],
    }
}

fn equals(left: SqlExpr, right: SqlExpr) -> SqlExpr {
    SqlExpr::Binary {
        op: SqlExprBinaryOp::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn and(left: SqlExpr, right: SqlExpr) -> SqlExpr {
    SqlExpr::Binary {
        op: SqlExprBinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
    }
}

#[test]
fn classify_write_order_proof_requires_full_canonical_primary_key_order() {
    let asc_id = SqlOrderTerm {
        field: SqlExpr::Field("id".to_string()),
        direction: SqlOrderDirection::Asc,
    };
    let desc_id = SqlOrderTerm {
        field: SqlExpr::Field("id".to_string()),
        direction: SqlOrderDirection::Desc,
    };
    let asc_other = SqlOrderTerm {
        field: SqlExpr::Field("name".to_string()),
        direction: SqlOrderDirection::Asc,
    };

    assert_eq!(
        classify_write_order_proof(&[], "Token", None, &["id"]),
        SqlWriteOrderProof::Missing,
    );
    assert_eq!(
        classify_write_order_proof(std::slice::from_ref(&asc_id), "Token", None, &["id"]),
        SqlWriteOrderProof::CanonicalPrimaryKey,
    );
    assert_eq!(
        classify_write_order_proof(std::slice::from_ref(&desc_id), "Token", None, &["id"]),
        SqlWriteOrderProof::DescendingPrimaryKey,
    );
    assert_eq!(
        classify_write_order_proof(std::slice::from_ref(&asc_other), "Token", None, &["id"]),
        SqlWriteOrderProof::Other,
    );
    assert_eq!(
        classify_write_order_proof(&[asc_id], "Token", None, &["id", "version"]),
        SqlWriteOrderProof::Other,
    );
}

#[test]
fn classify_write_where_proof_requires_complete_primary_key_literal_equality() {
    let complete = and(
        equals(field("id"), literal(1)),
        equals(field("version"), literal(2)),
    );
    assert_eq!(
        classify_write_where_proof(Some(&complete), "Token", None, &["id", "version"]),
        SqlWriteWhereProof::PrimaryKeyEquality,
    );

    let alias_complete = and(
        equals(aliased_field("t", "id"), literal(1)),
        equals(literal(2), aliased_field("t", "version")),
    );
    assert_eq!(
        classify_write_where_proof(
            Some(&alias_complete),
            "Token",
            Some("t"),
            &["id", "version"],
        ),
        SqlWriteWhereProof::PrimaryKeyEquality,
    );

    let partial = equals(field("id"), literal(1));
    assert_eq!(
        classify_write_where_proof(Some(&partial), "Token", None, &["id", "version"]),
        SqlWriteWhereProof::Other,
    );

    let duplicate = and(
        equals(field("id"), literal(1)),
        equals(field("id"), literal(2)),
    );
    assert_eq!(
        classify_write_where_proof(Some(&duplicate), "Token", None, &["id", "version"]),
        SqlWriteWhereProof::Other,
    );

    let field_to_field = equals(field("id"), field("version"));
    assert_eq!(
        classify_write_where_proof(Some(&field_to_field), "Token", None, &["id"]),
        SqlWriteWhereProof::Other,
    );
    assert_eq!(
        classify_write_where_proof(None, "Token", None, &["id"]),
        SqlWriteWhereProof::Missing,
    );
}
