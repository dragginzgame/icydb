use icydb_core::{traits::PersistedFieldSlotCodec, value::Value};

fn assert_slot_codec<T: PersistedFieldSlotCodec>() {}

fn main() {
    assert_slot_codec::<Vec<Value>>();
}
