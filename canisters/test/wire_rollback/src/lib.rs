//! Installed fault injection through IcyDB's maintained raw Wire adapter.

use std::cell::Cell;

use icydb_testing_wasm_helpers::fixture_wire::WireRollbackAction;
use serde::{Serialize, Serializer, ser::Error};

thread_local! {
    static COMMITS: Cell<u64> = const { Cell::new(0) };
}

// The admitted operation runs before serialization. A failure here must undo
// its heap mutation, whereas an application error is a normal encoded result.
struct Reply {
    value: Result<u64, u8>,
    fail_encoding: bool,
}

impl Serialize for Reply {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.fail_encoding {
            return Err(S::Error::custom("injected reply encoding failure"));
        }
        self.value.serialize(serializer)
    }
}

fn apply(action: WireRollbackAction) -> Reply {
    if action == WireRollbackAction::ApplicationError {
        return Reply {
            value: Err(7),
            fail_encoding: false,
        };
    }
    let committed = COMMITS.with(|counter| {
        let next = counter.get().saturating_add(1);
        counter.set(next);
        next
    });
    Reply {
        value: Ok(committed),
        fail_encoding: action == WireRollbackAction::EncodingFailure,
    }
}
icydb_testing_canic_adapter::export_wire!(update, "wire_rollback_apply", WireRollbackApply, apply, (action: WireRollbackAction), Reply);

fn state() -> u64 {
    COMMITS.with(Cell::get)
}
icydb_testing_canic_adapter::export_wire!(
    query,
    "wire_rollback_state",
    WireRollbackState,
    state,
    (),
    u64
);
