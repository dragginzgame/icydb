//! Native failure attribution only: RSS is not an IC performance metric.

use std::{fs, sync::mpsc, thread};

fn allocation_report() -> (u64, u64, u64, u64) {
    let report = ic_memory::default_memory_manager_memory_allocations().unwrap();
    for memory in &report.memories {
        if memory.virtual_extent.bytes != 0 {
            println!(
                "id={} binding={:?} virtual_bytes={} allocated_bytes={}",
                memory.memory_manager_id,
                memory.binding,
                memory.virtual_extent.bytes,
                memory.allocated_bytes,
            );
        }
    }
    (
        report.physical_extent.bytes,
        report.virtual_extent.bytes,
        report.bucket_size_bytes,
        report.bucket_slack_bytes,
    )
}

fn report_process_memory(phase: &str) {
    let status = fs::read_to_string("/proc/self/status").unwrap();
    for line in status.lines() {
        if line.starts_with("VmRSS:") || line.starts_with("VmHWM:") {
            println!("phase={phase} {line}");
        }
    }
}

#[test]
#[ignore = "native allocation attribution; run separately with ICYDB_MEMORY_WORKERS=1,2,4"]
fn native_memory_attribution() {
    let workers = std::env::var("ICYDB_MEMORY_WORKERS")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()
        .unwrap();
    assert!((1..=4).contains(&workers));
    report_process_memory("baseline");
    // Each worker owns an isolated thread-local database. Keep every backing
    // allocation alive at both observations; disconnected release channels
    // let workers exit if the observer or another worker fails.
    thread::scope(|scope| {
        let controls: Vec<_> = (0..workers)
            .map(|_| {
                let (report_tx, report_rx) = mpsc::channel();
                let (release_tx, release_rx) = mpsc::channel();
                scope.spawn(move || {
                    crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
                    report_tx.send(allocation_report()).unwrap();
                    if release_rx.recv().is_err() {
                        return;
                    }
                    super::insert_profile(19).unwrap();
                    assert_eq!(super::profile_rank().unwrap(), Some(19));
                    report_tx.send(allocation_report()).unwrap();
                    let _ = release_rx.recv();
                });
                (release_tx, report_rx)
            })
            .collect();
        for phase in ["initialized", "one_row"] {
            for (worker, (_, receiver)) in controls.iter().enumerate() {
                let (physical, virtual_bytes, bucket, slack) = receiver.recv().unwrap();
                println!(
                    "workers={workers} worker={worker} phase={phase} physical_bytes={physical} virtual_bytes={virtual_bytes} bucket_bytes={bucket} slack_bytes={slack}"
                );
            }
            report_process_memory(phase);
            if phase == "initialized" {
                for (sender, _) in &controls {
                    sender.send(()).unwrap();
                }
            }
        }
    });
}
