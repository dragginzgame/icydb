use super::dynamic_rows_reply;
use ic_wire::native;
use icydb_testing_wasm_helpers::wire::DynamicRowsQuery;
use std::cell::Cell;

#[test]
fn wire_admission_precedes_query_execution() {
    let request =
        native::encode_request(DynamicRowsQuery::ID, &(), DynamicRowsQuery::REQUEST_LIMITS)
            .unwrap();
    let mut invalid = (0..request.as_slice().len())
        .map(|length| request.as_slice()[..length].to_vec())
        .collect::<Vec<_>>();
    invalid.push(b"DIDL\0\0".to_vec());
    let mut wrong_identity = request.as_slice().to_vec();
    wrong_identity[2] ^= 1;
    invalid.push(wrong_identity);
    let mut wrong_direction = request.as_slice().to_vec();
    wrong_direction[10] = 1;
    invalid.push(wrong_direction);
    let mut trailing = request.as_slice().to_vec();
    trailing.push(0);
    invalid.push(trailing);

    let executions = Cell::new(0);
    for bytes in invalid {
        assert!(
            dynamic_rows_reply(&bytes, || {
                executions.set(executions.get() + 1);
                7
            })
            .is_err()
        );
    }
    assert_eq!(executions.get(), 0);

    let reply = dynamic_rows_reply(request.as_slice(), || {
        executions.set(executions.get() + 1);
        7
    })
    .unwrap();
    assert_eq!(
        native::decode_response::<u32>(
            DynamicRowsQuery::ID,
            reply.as_slice(),
            DynamicRowsQuery::RESPONSE_LIMITS
        ),
        Ok(7)
    );
    assert_eq!(executions.get(), 1);
}
