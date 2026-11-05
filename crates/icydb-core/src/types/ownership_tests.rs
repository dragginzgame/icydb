use super::*;

#[test]
fn core_scalar_surface_is_the_exact_schema_owned_type_identity() {
    let _: Account = icydb_schema::Account::from(icydb_schema::Principal::anonymous());
    let _: Blob = icydb_schema::Blob::default();
    let _: Date = icydb_schema::Date::EPOCH;
    let _: Decimal = icydb_schema::Decimal::default();
    let _: Duration = icydb_schema::Duration::ZERO;
    let _: Float32 = icydb_schema::Float32::default();
    let _: Float64 = icydb_schema::Float64::default();
    let _: U256 = icydb_schema::U256::ZERO;
    let _: Principal = icydb_schema::Principal::anonymous();
    let _: Subaccount = icydb_schema::Subaccount::MIN;
    let _: Timestamp = icydb_schema::Timestamp::EPOCH;
    let _: Ulid = icydb_schema::Ulid::nil();
    let _: Unit = icydb_schema::Unit;
}
