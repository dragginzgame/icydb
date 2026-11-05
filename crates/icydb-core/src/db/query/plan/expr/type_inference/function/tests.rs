use super::ExprType;
use crate::db::query::plan::expr::Function;

#[test]
fn null_function_arguments_preserve_unknown_and_sibling_rejection() {
    for function in [Function::Lower, Function::Abs, Function::OctetLength] {
        let shape = function.type_inference_shape();
        assert!(
            shape
                .infer_function_result_type(function, &[ExprType::Null])
                .is_ok()
        );
        assert!(
            shape
                .infer_function_result_type(function, &[ExprType::Unknown])
                .is_err()
        );
    }
    let round = Function::Round.type_inference_shape();
    let integer = ExprType::Numeric(super::NumericSubtype::Integer);
    assert!(
        round
            .infer_function_result_type(Function::Round, &[ExprType::Null, integer])
            .is_ok()
    );
    assert!(
        round
            .infer_function_result_type(Function::Round, &[ExprType::Null, ExprType::Text])
            .is_err()
    );
    assert!(
        Function::Substring
            .type_inference_shape()
            .infer_function_result_type(Function::Substring, &[ExprType::Null, ExprType::Text],)
            .is_err()
    );
    assert_eq!(
        Function::Coalesce
            .type_inference_shape()
            .infer_function_result_type(Function::Coalesce, &[ExprType::Null, ExprType::Null],)
            .expect("all-NULL coalesce"),
        ExprType::Null
    );
    assert_eq!(
        Function::Coalesce
            .type_inference_shape()
            .infer_function_result_type(Function::Coalesce, &[ExprType::Unknown, ExprType::Null],)
            .expect("unresolved input stays unresolved"),
        ExprType::Unknown
    );
}

#[test]
fn mod_admits_only_exact_u256_operands() {
    let shape = Function::Mod.type_inference_shape();

    assert!(matches!(
        shape
            .infer_function_result_type(Function::Mod, &[ExprType::U256, ExprType::U256])
            .expect("MOD should admit exact U256 operands"),
        ExprType::U256,
    ));
    assert!(
        shape
            .infer_function_result_type(
                Function::Mod,
                &[
                    ExprType::U256,
                    ExprType::Numeric(super::NumericSubtype::Integer),
                ],
            )
            .is_err()
    );
}
