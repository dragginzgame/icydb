//! Consumer policy for recursive schema-depth values inside endpoint wrappers.

use crate::wire::{decode_one, encode_one};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
enum Nested {
    Leaf(u128),
    Children(Vec<Self>),
}

#[test]
fn schema_depth_values_fit_inside_endpoint_record_wrappers() {
    let mut value = Nested::Leaf(u128::MAX);
    for _ in 0..crate::MAX_SCHEMA_FIELD_TYPE_DEPTH {
        value = Nested::Children(vec![value]);
    }
    let reply: Result<Vec<(String, Nested)>, u16> = Ok(vec![("value".into(), value)]);
    let bytes = encode_one(&reply).unwrap();
    assert_eq!(
        decode_one::<Result<Vec<(String, Nested)>, u16>>(&bytes).unwrap(),
        reply
    );
}
