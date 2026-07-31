use icydb_core::db::KeyValueCodec;

fn assert_key_codec<T: KeyValueCodec>() {}

fn main() {
    assert_key_codec::<Vec<u64>>();
}
