use super::*;
use crate::{
    db::query::intent::QueryExecutionError,
    error::{ErrorDetail, QueryErrorDetail},
};

const fn enum_kind() -> AcceptedFieldKind {
    AcceptedFieldKind::Enum {
        type_id: crate::value::EnumTypeId::new(1).expect("test enum type ID should be valid"),
    }
}

fn assert_invalid_enum_sql_literal(error: QueryError) {
    let QueryError::Execute(QueryExecutionError::Unsupported(internal)) = error else {
        panic!("expected unsupported SQL write boundary error");
    };

    assert!(matches!(
        internal.detail(),
        Some(ErrorDetail::Query(QueryErrorDetail::SqlWriteBoundary { boundary }))
            if *boundary == SqlWriteBoundaryCode::InvalidFieldLiteral
    ));
}

#[test]
fn sql_enum_string_literal_remains_unresolved_until_accepted_patch_admission() {
    let input = sql_write_input_for_accepted_kind(&enum_kind(), &Value::Text("Active".to_string()))
        .expect("target-typed enum string should become authored input");

    assert_eq!(input, InputValue::loose_enum("Active"));
}

#[test]
fn sql_enum_target_rejects_non_label_scalar_literals() {
    let err = sql_write_input_for_accepted_kind(&enum_kind(), &Value::Nat64(7))
        .expect_err("numeric literal must not author an enum label");

    assert_invalid_enum_sql_literal(err);
}

#[test]
fn sql_enum_target_defers_label_validation_to_accepted_patch_admission() {
    for variant in ["Missing", "Loaded"] {
        let input =
            sql_write_input_for_accepted_kind(&enum_kind(), &Value::Text(variant.to_string()))
                .expect("enum text should remain unresolved until catalog admission");

        assert_eq!(input, InputValue::loose_enum(variant));
    }
}
