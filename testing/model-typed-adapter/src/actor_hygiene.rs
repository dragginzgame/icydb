//! Compile actual actor output with caller-defined standard names and macros.

// No endpoint adapter is needed to compile private actor wiring. Most generated
// helpers are deliberately unused by this native compile contract.
#[expect(
    dead_code,
    reason = "qualify private actor wiring without an endpoint host"
)]
mod generated {
    use std::{
        cmp::Ordering::{Equal as None, Less as Ok},
        collections::VecDeque as Vec,
        ffi::OsString as String,
        io::Result,
        ops::ControlFlow::{Break as Err, Continue as Some},
    };

    const _: std::cmp::Ordering = None;
    const _: std::cmp::Ordering = Ok;
    const _: std::ops::ControlFlow<(), ()> = Err(());
    const _: std::ops::ControlFlow<(), ()> = Some(());
    const _: Result<()> = std::result::Result::Ok(());
    const _: std::option::Option<Vec<String>> = std::option::Option::None;

    macro_rules! thread_local {
        ($($input:tt)*) => {
            const _: () = ();
        };
    }
    macro_rules! vec {
        ($($input:tt)*) => {
            0_u8
        };
    }
    macro_rules! concat {
        ($($input:tt)*) => {
            0_u8
        };
    }
    thread_local!();
    const _: u8 = vec![];
    const _: u8 = concat!();

    ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/actor.rs"));
}

pub(super) fn icydb_memory_pool() -> std::result::Result<
    runtime_api::db::MemoryAllocationPool,
    runtime_api::db::MemoryAllocationPoolError,
> {
    runtime_api::db::MemoryAllocationPool::new(
        vec![runtime_api::db::MemoryAuthority::new(
            "icydb.actor_hygiene",
            "icydb.actor_hygiene.",
        )?],
        vec![],
    )
}

#[test]
fn actor_requests_keep_the_declared_memory_authority() {
    let requests = generated::icydb_memory_requests().expect("generated requests should validate");
    assert_eq!(requests.len(), 7);
    assert!(requests.iter().all(|request| {
        request.authority() == "icydb.actor_hygiene"
            && request
                .stable_key()
                .as_str()
                .starts_with("icydb.actor_hygiene.")
    }));
    let _ = icydb_memory_pool().expect("fixture authority should validate");
}
