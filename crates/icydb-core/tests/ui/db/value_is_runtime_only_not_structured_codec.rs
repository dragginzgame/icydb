use icydb_core::{traits::PersistedStructuredFieldCodec, value::Value};

fn assert_structured_codec<T: PersistedStructuredFieldCodec>() {}

fn main() {
    assert_structured_codec::<Value>();
}
