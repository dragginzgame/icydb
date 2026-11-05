use crate::{
    db::EntityKey,
    types::{Id, Ulid},
};

struct VectorEntity;
impl EntityKey for VectorEntity {
    type Key = Ulid;
}

struct OtherEntity;
impl EntityKey for OtherEntity {
    type Key = Ulid;
}

#[test]
fn projection_is_deterministic_for_known_vector() {
    let id = Id::<VectorEntity>::from_key(Ulid::from_bytes([
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]));

    let projected = id.project().expect("valid ULID key should project");
    assert_eq!(
        projected.to_string(),
        "2da507e94f2d3dcb66fad5f74338d3fdf31f7b8a0e56e5888078f0c91a5b643f"
    );
}

#[test]
fn projection_is_stable_across_entities_for_same_key_bytes() {
    let key = Ulid::from_bytes([0x42; 16]);
    let a = Id::<VectorEntity>::from_key(key)
        .project()
        .expect("valid ULID key should project");
    let b = Id::<OtherEntity>::from_key(key)
        .project()
        .expect("valid ULID key should project");

    assert_eq!(a, b);
}
