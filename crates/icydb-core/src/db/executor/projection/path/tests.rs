use super::*;
use crate::db::data::{decode_canonical_value_storage_bytes, encode_canonical_value_storage_bytes};

#[test]
fn canonical_traversal_paths_preserve_null_missing_and_corruption() {
    let segments: Box<[Box<[u8]>]> = [
        b"branch".to_vec().into_boxed_slice(),
        b"leaf".to_vec().into_boxed_slice(),
    ]
    .into();
    for (root, expected) in [
        (Value::Null, None),
        (Value::Map(vec![]), None),
        (
            Value::Map(vec![(Value::Text("branch".into()), Value::Null)]),
            None,
        ),
        (
            Value::Map(vec![(Value::Text("branch".into()), Value::Map(vec![]))]),
            None,
        ),
        (
            Value::Map(vec![(
                Value::Text("branch".into()),
                Value::Map(vec![(Value::Text("leaf".into()), Value::Null)]),
            )]),
            Some(Value::Null),
        ),
        (
            Value::Map(vec![(
                Value::Text("branch".into()),
                Value::Map(vec![(
                    Value::Text("leaf".into()),
                    Value::Text("payload".repeat(1024)),
                )]),
            )]),
            Some(Value::Text("payload".repeat(1024))),
        ),
        (
            Value::Map(vec![
                (Value::Text("branch".into()), Value::Map(vec![])),
                (
                    Value::Text("branch".into()),
                    Value::Map(vec![(Value::Text("leaf".into()), Value::Nat64(9))]),
                ),
            ]),
            None,
        ),
    ] {
        let bytes = encode_canonical_value_storage_bytes(&root).unwrap();
        for _ in 0..2 {
            let leaf = resolve_path_segments(&bytes, &segments).unwrap();
            assert_eq!(
                leaf.map(|raw| decode_canonical_value_storage_bytes(raw).unwrap()),
                expected
            );
            assert_eq!(
                resolve_value_field_path(&root, &["branch", "leaf"])
                    .unwrap()
                    .cloned(),
                expected
            );
        }
    }
    let scalar_ancestor = Value::Map(vec![(Value::Text("branch".into()), Value::Nat64(9))]);
    let bytes = encode_canonical_value_storage_bytes(&scalar_ancestor).unwrap();
    assert!(resolve_path_segments(&bytes, &segments).is_err());
    assert!(resolve_value_field_path(&scalar_ancestor, &["branch", "leaf"]).is_err());
    assert!(resolve_path_segments(&[0xff], &segments).is_err());
}
