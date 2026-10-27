//! Exclusive direct-projection handoff retains one lazy decoding authority.

use super::*;

fn backing(value: &Value) -> (*const u8, *const u8) {
    match value {
        Value::Blob(bytes) => (bytes.as_ptr(), bytes.as_ptr()),
        Value::List(items) => {
            let Value::Text(text) = &items[0] else {
                panic!("text item")
            };
            (items.as_ptr().cast(), text.as_ptr())
        }
        _ => panic!("heap-owning fixture"),
    }
}

#[test]
fn direct_projection_take_moves_heap_backing_and_keeps_later_reads_valid() {
    let nested = payload_contract(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Text { max_len: None })),
        LeafCodec::Structural,
        false,
    );
    for (contract, payload) in [
        (selective_payload_contract(), Value::Blob(vec![9; 4096])),
        (
            nested,
            Value::List(vec![Value::Text("payload".repeat(128))]),
        ),
    ] {
        let values = [Value::Nat64(7), Value::Bool(true), payload];
        let row =
            canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .unwrap()
            .into_raw_row();
        let mut reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
        let before = backing(reader.required_cached_value(2).unwrap());
        let output = reader.take_direct_projection_value(2).unwrap();
        assert_eq!(output, values[2]);
        assert_eq!(backing(&output), before);
        // The moved output is still live while the same reader is read again.
        // Refilling its existing empty cell cannot alias the caller's ownership.
        let again = reader.required_cached_value(2).unwrap();
        assert_eq!(again, &output);
        assert_ne!(backing(again), before);
        assert_eq!(reader.take_direct_projection_value(2).unwrap(), output);
        assert_eq!(reader.required_cached_value(0).unwrap(), &values[0]);
    }
}

#[test]
fn direct_catalog_scalars_and_invalid_slots_do_not_allocate_value_cache() {
    for (kind, value) in [
        (AcceptedFieldKind::Bool, Value::Bool(true)),
        (AcceptedFieldKind::Nat64, Value::Nat64(u64::MAX)),
        (AcceptedFieldKind::Int64, Value::Int64(i64::MIN)),
        (
            AcceptedFieldKind::Text { max_len: None },
            Value::Text("héllo".into()),
        ),
        (
            AcceptedFieldKind::Text { max_len: None },
            Value::Text(String::new()),
        ),
        (
            AcceptedFieldKind::Blob { max_len: None },
            Value::Blob(vec![0, 255]),
        ),
        (
            AcceptedFieldKind::Blob { max_len: None },
            Value::Blob(Vec::new()),
        ),
    ] {
        let contract = payload_contract_with_storage(
            kind,
            LeafCodec::Structural,
            true,
            FieldStorageDecode::CatalogValue,
        );
        for payload in [value, Value::Null] {
            let values = [Value::Nat64(7), Value::Bool(true), payload];
            let row =
                canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                    Ok(Cow::Borrowed(&values[slot]))
                })
                .unwrap()
                .into_raw_row();
            let mut reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
            for invalid in [contract.field_count(), usize::MAX] {
                let error = reader.required_cached_value(invalid).unwrap_err();
                let expected = crate::error::InternalError::index_invariant();
                assert_eq!(error.class(), expected.class());
                assert_eq!(error.origin(), expected.origin());
                assert!(reader.cached_values.get().is_none());
            }
            for _ in 0..2 {
                assert_eq!(reader.take_direct_projection_value(2).unwrap(), values[2]);
                assert!(reader.cached_values.get().is_none());
            }
            // Ordinary cached access remains valid after direct reads and refusals.
            assert_eq!(reader.required_cached_value(2).unwrap(), &values[2]);
            assert!(reader.cached_values.get().is_some());
            assert_eq!(reader.take_required_value(2).unwrap(), values[2]);
            assert_eq!(reader.required_cached_value(2).unwrap(), &values[2]);
        }
    }
}
