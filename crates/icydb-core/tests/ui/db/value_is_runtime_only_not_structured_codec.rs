use icydb_core::{db::PersistedStructuralValueCodec, value::Value};

fn assert_structured_codec<T: PersistedStructuralValueCodec>() {}

fn main() {
    assert_structured_codec::<Value>();
}
