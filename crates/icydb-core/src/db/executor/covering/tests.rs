use super::*;
use crate::{
    error::{ErrorClass, ErrorOrigin},
    value::ValueTag,
};

#[test]
fn decode_single_covering_projection_value_rejects_multiple_components() {
    let components: IndexComponentValues = Arc::from(vec![
        vec![ValueTag::Bool.to_u8(), 1],
        vec![ValueTag::Bool.to_u8(), 0],
    ]);

    let error = decode_single_covering_projection_value(&components)
        .expect_err("multi-component vectors must violate the single-component invariant");

    assert_eq!(error.class(), ErrorClass::InvariantViolation);
    assert_eq!(error.origin(), ErrorOrigin::Query);
}
