#![allow(clippy::cast_possible_truncation)]
use bencher::*;
use icydb_core::{
    serialize::{deserialize, serialize},
    types::Blob,
};

benchmark_group!(
    benchmarks,
    serialize_blob_round_trip,
    serialize_blob,
    deserialize_blob
);

const BYTES: usize = 4096;

fn serialize_blob_round_trip(b: &mut Bencher) {
    // Prepare deterministic bytes (no RNG overhead in the hot loop)
    let bytes: Vec<u8> = (0..BYTES as u32).map(|i| i as u8).collect();
    let blob = Blob::from(bytes);

    b.iter(|| {
        // serialize -> deserialize
        let buf = serialize(&blob).expect("serialize Blob");
        let out: Blob = deserialize(&buf).expect("deserialize Blob");

        // prevent optimization
        std::hint::black_box(&buf);
        std::hint::black_box(out);
    });
}

fn serialize_blob(b: &mut Bencher) {
    let bytes: Vec<u8> = (0..BYTES as u32).map(|i| i as u8).collect();
    let blob = Blob::from(bytes);

    b.iter(|| {
        let buf = serialize(&blob).expect("serialize Blob");
        std::hint::black_box(buf);
    });
}

fn deserialize_blob(b: &mut Bencher) {
    let bytes: Vec<u8> = (0..BYTES as u32).map(|i| i as u8).collect();
    let blob = Blob::from(bytes);
    let encoded = serialize(&blob).expect("serialize Blob");

    b.iter(|| {
        let out: Blob = deserialize(&encoded).expect("deserialize Blob");
        std::hint::black_box(out);
    });
}
