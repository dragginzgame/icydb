use crate::schema::{AuthoredPackageNames, icydb::Money};
use runtime_api::wire;

#[test]
fn renamed_runtime_preserves_authored_fields_paths_and_wire_values() {
    let value = AuthoredPackageNames {
        icydb: Money::from(7_u64),
        icydb_model: 11,
    };
    let bytes = wire::encode_one(&value).unwrap();
    assert_eq!(
        wire::decode_one::<AuthoredPackageNames>(&bytes).unwrap(),
        value
    );
    assert_eq!(AuthoredPackageNames::ICYDB.as_str(), "icydb");
    assert_eq!(AuthoredPackageNames::ICYDB_MODEL.as_str(), "icydb_model");
}
