//! Generated upgrade fixture. Never deploy this test actor with application data.
//! Timer delivery is paused so tests choose the precise pre-upgrade journal state.

mod schema;

use ic_memory::{
    RuntimeMemory, committed_allocations,
    ic_stable_structures::{DefaultMemoryImpl, Memory},
    open_default_memory_manager_memory_by_key,
};
use icydb::{
    db::{DatabaseStartupState, StartupFailure, TypedWriteAdapter as _, WriteCell},
    types::Id,
};

icydb::ic_memory_range!(
    authority = "icydb.logical_fixture",
    start = 100,
    end = 140,
    mode = Allowed
);
icydb::start! {
    init() => pause_delivery;
    post_upgrade() => pause_delivery;
}

// Pause the IC delivery mechanism, not the generated driver or its budgets.
fn pause_delivery() {
    ic_cdk::api::global_timer_set(0);
}

#[cfg(feature = "test-admin-api")]
#[ic_cdk::update]
fn step() -> Result<bool, icydb::Error> {
    let result = icydb::db::with_request_execution(
        __icydb_generated::__icydb_startup_driver_attempt_for_tests,
    );
    pause_delivery();
    result
}

#[ic_cdk::query]
fn state() -> Result<DatabaseStartupState, StartupFailure> {
    startup_state()
}

#[ic_cdk::query]
fn allocations() -> Vec<(String, u8)> {
    committed_allocations()
        .unwrap()
        .declarations()
        .iter()
        .map(|allocation| {
            (
                allocation.stable_key().to_string(),
                allocation.slot().memory_manager_id().unwrap(),
            )
        })
        .collect()
}

#[ic_cdk::query]
fn keep_row_exists() -> Result<bool, String> {
    icydb::db::with_request_execution(|| {
        db()?
            .get::<schema::KeepRow>(Id::from_key(7_u64))
            .map(|row| row.is_some())
    })
    .map_err(|error| error.to_string())
}

#[ic_cdk::update]
fn insert_keep() -> Vec<u8> {
    icydb::db::with_request_execution(|| {
        let session = db().unwrap();
        let binding = schema::KeepRow::typed_binding(&session).unwrap();
        let write = schema::KeepRowInsert {
            id: WriteCell::Value(Id::from_key(7_u64)),
        }
        .encode_write(&binding)
        .unwrap();
        session.execute_trusted_typed_write_row(write).unwrap();
    });
    pause_delivery();
    capture_last_marker()
}

#[cfg(not(feature = "omit-retiring-store"))]
#[ic_cdk::query]
fn retiring_row_exists() -> Result<bool, String> {
    icydb::db::with_request_execution(|| {
        db()?
            .get::<schema::RetiringRow>(Id::from_key(9_u64))
            .map(|row| row.is_some())
    })
    .map_err(|error| error.to_string())
}

#[cfg(not(feature = "omit-retiring-store"))]
#[ic_cdk::update]
fn insert_retiring() {
    icydb::db::with_request_execution(|| {
        let session = db().unwrap();
        let binding = schema::RetiringRow::typed_binding(&session).unwrap();
        let write = schema::RetiringRowInsert {
            id: WriteCell::Value(Id::from_key(9_u64)),
        }
        .encode_write(&binding)
        .unwrap();
        session.execute_trusted_typed_write_row(write).unwrap();
    });
    pause_delivery();
}

const CONTROL_KEY: &str = "icydb.logical_fixture.commit.control.v1";
const FRAME_OFFSET: u64 = 15;
const FRAME_BYTES: usize = 13;
const PAYLOAD_OFFSET: u64 = FRAME_OFFSET + FRAME_BYTES as u64;
const MAX_FIXTURE_CONTROL_BYTES: usize = 64 * 1024;

fn control() -> RuntimeMemory<DefaultMemoryImpl> {
    open_default_memory_manager_memory_by_key(CONTROL_KEY).unwrap()
}

#[ic_cdk::query]
fn control_frame() -> Vec<u8> {
    let memory = control();
    let mut header = [0; FRAME_BYTES];
    memory.read(FRAME_OFFSET, &mut header);
    assert_eq!(&header[..5], b"IDCS\x01");
    let length = u32::from_be_bytes(header[5..9].try_into().unwrap()) as usize;
    assert!(length <= MAX_FIXTURE_CONTROL_BYTES);
    let mut frame = vec![0; FRAME_BYTES + length];
    memory.read(FRAME_OFFSET, &mut frame);
    frame
}

// Normal commit clearing shortens the frame and zeroes its marker length; it
// leaves the prior payload beyond the frame. Capture that exact production
// encoding immediately after the write, before any later write can reuse it.
// No marker payload codec is reimplemented here. An unchanged-actor upgrade
// must successfully replay the restored frame before it is used as a barrier test.
fn capture_last_marker() -> Vec<u8> {
    let empty = control_frame();
    assert_eq!(&empty[empty.len() - 4..], &[0; 4]);
    let payload_length = empty.len() - FRAME_BYTES;
    let memory = control();
    let mut marker_header = [0; 5];
    memory.read(PAYLOAD_OFFSET + payload_length as u64, &mut marker_header);
    assert_eq!(marker_header[0], 1);
    let marker_length = 5 + u32::from_le_bytes(marker_header[1..].try_into().unwrap()) as usize;
    assert!(payload_length + marker_length <= MAX_FIXTURE_CONTROL_BYTES);
    let mut frame = empty;
    let marker_offset = frame.len();
    frame.resize(marker_offset + marker_length, 0);
    memory.read(
        PAYLOAD_OFFSET + payload_length as u64,
        &mut frame[marker_offset..],
    );
    frame[marker_offset - 4..marker_offset]
        .copy_from_slice(&u32::try_from(marker_length).unwrap().to_le_bytes());
    frame[5..9].copy_from_slice(
        &u32::try_from(payload_length + marker_length)
            .unwrap()
            .to_be_bytes(),
    );
    let checksum = crc32c(&frame[FRAME_BYTES..]);
    frame[9..13].copy_from_slice(&checksum.to_be_bytes());
    frame
}

// Test-only raw frame restoration through the committed control capability.
// The next operation is an upgrade, which discards all in-memory marker hints.
#[ic_cdk::update]
fn restore_control_frame(frame: Vec<u8>) {
    assert!((FRAME_BYTES..=MAX_FIXTURE_CONTROL_BYTES).contains(&frame.len()));
    assert_eq!(&frame[..5], b"IDCS\x01");
    assert_eq!(
        u32::from_be_bytes(frame[5..9].try_into().unwrap()) as usize,
        frame.len() - FRAME_BYTES
    );
    assert_eq!(
        u32::from_be_bytes(frame[9..13].try_into().unwrap()),
        crc32c(&frame[FRAME_BYTES..])
    );
    control().write(FRAME_OFFSET, &frame);
    pause_delivery();
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
