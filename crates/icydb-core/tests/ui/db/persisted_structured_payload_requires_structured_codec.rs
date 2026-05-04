use icydb_core::{
    db::encode_persisted_structured_slot_payload,
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
    let _ = encode_persisted_structured_slot_payload(&value, "broken");
}
