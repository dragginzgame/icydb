//! Module: db::executor::projection::path
//! Responsibility: projection-local nested value-storage path resolution.
//! Does not own: planner path lowering, predicate evaluation, or index access.
//! Boundary: hides `ValueStorageView` behind an executor projection helper.

use crate::{
    db::{
        data::{FieldDecodeError, ValueStorageView},
        query::plan::expr::ProjectionEvalError,
    },
    error::InternalError,
    value::Value,
};

/// Walk one already-materialized record path without cloning nested maps.
/// Missing members and null ancestors both project as an absent scalar leaf;
/// a non-map ancestor is persisted-row corruption.
pub(in crate::db::executor) fn resolve_value_field_path<'value>(
    root: &'value Value,
    segments: &[String],
) -> Result<Option<&'value Value>, ProjectionEvalError> {
    let mut current = root;
    for segment in segments {
        if matches!(current, Value::Null) {
            return Ok(None);
        }
        let entries = current.as_map().ok_or_else(|| {
            let err = InternalError::persisted_row_decode_corruption();
            ProjectionEvalError::FieldPathEvaluationFailed {
                class: err.class(),
                origin: err.origin(),
            }
        })?;
        let Some((_, value)) = entries
            .iter()
            .find(|(key, _)| matches!(key, Value::Text(text) if text == segment))
        else {
            return Ok(None);
        };
        current = value;
    }

    Ok(Some(current))
}

/// Resolve one nested map path using already-encoded segment bytes.
pub(in crate::db::executor) fn resolve_path_segments<'a>(
    raw_bytes: &'a [u8],
    segment_bytes: &[Box<[u8]>],
) -> Result<Option<&'a [u8]>, FieldDecodeError> {
    let mut current = ValueStorageView::from_raw_validated(raw_bytes)?;

    // The caller has already resolved the root field to a persisted slot
    // payload. Traversal therefore starts at the first nested segment rather
    // than attempting to treat the raw row as a value-storage map.
    for segment in segment_bytes {
        if current.is_null() {
            return Ok(None);
        }
        current = match current.map_text_key_bytes(segment)? {
            Some(next) => next,
            None => return Ok(None),
        };
    }

    Ok(Some(current.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::data::{
        decode_canonical_value_storage_bytes, encode_canonical_value_storage_bytes,
    };

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
        ] {
            let bytes = encode_canonical_value_storage_bytes(&root).unwrap();
            for _ in 0..2 {
                let leaf = resolve_path_segments(&bytes, &segments).unwrap();
                assert_eq!(
                    leaf.map(|raw| decode_canonical_value_storage_bytes(raw).unwrap()),
                    expected
                );
                assert_eq!(
                    resolve_value_field_path(&root, &["branch".into(), "leaf".into()])
                        .unwrap()
                        .cloned(),
                    expected
                );
            }
        }
        let scalar_ancestor = Value::Map(vec![(Value::Text("branch".into()), Value::Nat64(9))]);
        let bytes = encode_canonical_value_storage_bytes(&scalar_ancestor).unwrap();
        assert!(resolve_path_segments(&bytes, &segments).is_err());
        assert!(
            resolve_value_field_path(&scalar_ancestor, &["branch".into(), "leaf".into()]).is_err()
        );
        assert!(resolve_path_segments(&[0xff], &segments).is_err());
    }
}
