use candid::CandidType;
use ic_testkit::{
    pic::{InstallSpec, StandaloneCanisterFixture},
    pocket_ic::PocketIc,
};
use serde::{Deserialize, Serialize};
use std::{env, fs, time::Duration};

#[derive(CandidType, Debug, Deserialize, Serialize)]
struct Allocation {
    physical: u64,
    virtual_bytes: u64,
    slack: u64,
    buckets: u16,
    slots: Vec<(u8, u64, u64)>,
}

fn main() {
    let args = env::args().collect::<Vec<_>>();
    let wasm = fs::read(&args[1]).unwrap();
    for pages in args[2].split(',').map(|n| n.parse::<u16>().unwrap()) {
        let fixture = StandaloneCanisterFixture::install(
            PocketIc::new(),
            InstallSpec::new(
                wasm.clone(),
                candid::encode_args((pages,)).unwrap(),
                100_000_000_000_000,
            )
            .label("bucket-experiment"),
        );
        let mut ready = false;
        for _ in 0..70 {
            fixture.pocket_ic().advance_time(Duration::from_secs(1));
            for _ in 0..8 {
                fixture.pocket_ic().tick();
            }
            let result: Result<(), icydb::Error> = fixture.update_candid("ready", ()).unwrap();
            if result.is_ok() {
                ready = true;
                break;
            }
            assert_eq!(
                result.unwrap_err().code(),
                icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
            );
        }
        assert!(ready);
        let allocation: Allocation = fixture.query_candid("allocation", ()).unwrap();
        println!(
            "{}",
            serde_json::json!({"pages":pages,"phase":"empty","allocation":allocation})
        );
        let mut first = 0_u32;
        for (phase, count, bytes) in [
            ("small", 64, 64_u32),
            ("medium", 2048, 1024),
            ("larger", 2048, 1024),
            ("wide", 512, 2048),
        ] {
            let phase_first = first;
            let mut total = 0_u64;
            let mut max = 0_u64;
            for _ in 0..count / 32 {
                let result: Result<u64, icydb::Error> = fixture
                    .update_candid("insert", (first, 32_u32, bytes))
                    .unwrap();
                let instructions = result.unwrap_or_else(|error| {
                    panic!("pages={pages} phase={phase} first={first}: {error:?}")
                });
                total += instructions;
                max = max.max(instructions);
                first += 32;
                // Deliver normal timer work between write batches. This advances
                // simulated IC time, never a wall-clock performance measurement.
                fixture.pocket_ic().advance_time(Duration::from_secs(1));
                for _ in 0..8 {
                    fixture.pocket_ic().tick();
                }
            }
            let allocation: Allocation = fixture.query_candid("allocation", ()).unwrap();
            let mut read_instructions = Vec::new();
            let positions = [phase_first, first - 32, phase_first + count / 2];
            for position in positions {
                let result: Result<(u64, u32), icydb::Error> = fixture
                    .query_candid("read", (position, 32_u32, bytes))
                    .unwrap();
                let (instructions, found) = result.unwrap();
                assert_eq!(found, 32);
                read_instructions.push(instructions);
            }
            let retained: Result<(u64, u32), icydb::Error> = fixture
                .query_candid("read", (0_u32, 32_u32, 64_u32))
                .unwrap();
            assert_eq!(retained.unwrap().1, 32);
            println!(
                "{}",
                serde_json::json!({"pages":pages,"phase":phase,"rows_added":count,"text_bytes":bytes,
                "write_instructions":total,"max_batch_instructions":max,"read_instructions":read_instructions,"allocation":allocation})
            );
        }
    }
}
