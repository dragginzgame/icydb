use icydb_core::value::RuntimeValueDecode;

fn assert_runtime_decode<T: RuntimeValueDecode>() {}

fn main() {
    assert_runtime_decode::<&'static str>();
}
