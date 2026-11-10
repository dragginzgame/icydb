use super::{AuthoredPackageNames, icydb::Money};

#[test]
fn renamed_model_and_override_preserve_authored_names_and_literals() {
    let value = AuthoredPackageNames {
        icydb: Money::from(7_u64),
        icydb_model: "authored".to_owned(),
    };
    let _: Money = value.icydb;
    assert_eq!(value.icydb_model, "authored");
}
