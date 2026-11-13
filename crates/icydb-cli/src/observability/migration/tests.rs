use icydb::__macro::RpcMethod;
use icydb::db::{ExpectedAcceptedHead, SchemaMigrationCommand, TargetDatabaseIdentity};
use icydb_schema::{ExpectedSchemaFingerprint, SchemaMigrationPlanDigest};

#[test]
fn migration_commands_decode_without_canister_type_metadata() {
    for revision in [1, u64::MAX] {
        for head in [
            ExpectedAcceptedHead::Empty,
            ExpectedAcceptedHead::Exact {
                revision,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
            },
        ] {
            let database = TargetDatabaseIdentity::from_bytes([1; 32]);
            let plan = SchemaMigrationPlanDigest::from_bytes([2; 32]);
            for command in [
                SchemaMigrationCommand::Adopt {
                    expected_database: database,
                    expected_head: head.clone(),
                },
                SchemaMigrationCommand::Abort {
                    expected_database: database,
                    expected_head: head.clone(),
                    expected_plan: plan,
                },
                SchemaMigrationCommand::Advance {
                    expected_database: database,
                    expected_head: head,
                    expected_plan: plan,
                },
            ] {
                // The argument carries its own types, without service metadata.
                let bytes = RpcMethod::SchemaMigrate
                    .encode_request(
                        &command,
                        icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
                    )
                    .unwrap();
                assert_eq!(
                    icydb::__macro::RpcMethod::SchemaMigrate
                        .decode_request::<SchemaMigrationCommand>(
                            &bytes,
                            icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)
                        )
                        .unwrap(),
                    command
                );
            }
        }
    }
}
