use icydb_core::{
    db::encode_persisted_slot_payload_by_kind,
    model::FieldKind,
    traits::RuntimeValueEncode,
    value::Value,
};

#[derive(Clone, Debug, Default)]
struct CloneDebugOnly;

impl RuntimeValueEncode for CloneDebugOnly {
    fn to_value(&self) -> Value {
        Value::Text("placeholder".to_string())
    }
}

fn main() {
    let value = CloneDebugOnly;
    let _ = encode_persisted_slot_payload_by_kind(&value, FieldKind::Text, "broken");
}
