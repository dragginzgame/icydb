use super::{
    SqlReturningFieldProjection, sql_returning_field_indices, sql_returning_field_selection,
};
use crate::{db::QueryError, value::Value};
use icydb_diagnostic_code::DiagnosticFactTag;

#[test]
fn borrowed_returning_projection_preserves_selection_order_and_alignment() {
    let columns = ["id", "blob", "label"].map(str::to_string);
    let row = vec![
        Value::Nat64(7),
        Value::Blob(vec![3; 64 * 1024]),
        Value::Text("name".into()),
    ];
    for (fields, expected) in [
        (vec!["id"], vec![Value::Nat64(7)]),
        (
            vec!["id", "label"],
            vec![Value::Nat64(7), Value::Text("name".into())],
        ),
        (
            vec!["label", "id"],
            vec![Value::Text("name".into()), Value::Nat64(7)],
        ),
        (vec![], vec![]),
    ] {
        let fields = fields.into_iter().map(str::to_string).collect::<Vec<_>>();
        let projection = SqlReturningFieldProjection::from_fields(&columns, &fields).unwrap();
        assert_eq!(projection.project_borrowed_row(&row).unwrap(), expected);
        assert_eq!(projection.project_owned_row(row.clone()).unwrap(), expected);
        let short = projection.project_borrowed_row(&row[..1]);
        let owned_short = projection.project_owned_row(row[..1].to_vec());
        if fields.iter().any(|field| field == "label") {
            assert_eq!(
                short.unwrap_err().diagnostic(),
                QueryError::invariant().diagnostic()
            );
            assert_eq!(
                owned_short.unwrap_err().diagnostic(),
                QueryError::invariant().diagnostic()
            );
        } else {
            assert_eq!(short.unwrap(), owned_short.unwrap());
        }
    }
    assert_eq!(row[1], Value::Blob(vec![3; 64 * 1024]));
}

#[test]
fn unknown_returning_field_retains_projection_index() {
    let error = sql_returning_field_indices(
        &["id".to_string()],
        &["id".to_string(), "missing".to_string()],
    )
    .expect_err("unknown returning field should reject");

    assert_eq!(
        error.diagnostic_facts(),
        vec![(DiagnosticFactTag::ProjectionIndex, 1)],
    );
}

#[test]
fn duplicate_returning_field_retains_duplicate_projection_index() {
    let error = sql_returning_field_selection(&[0, 1, 0])
        .expect_err("duplicate returning field should reject");

    assert_eq!(
        error.diagnostic_facts(),
        vec![(DiagnosticFactTag::ProjectionIndex, 2)],
    );
}
