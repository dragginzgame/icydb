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
