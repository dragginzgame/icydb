extern crate self as icydb;

pub mod db {
    pub use icydb_core::db::{SlotReader, SlotWriter};
    pub use icydb_core::error::InternalError;

    pub trait PersistedRow: Sized {
        fn materialize_from_slots(slots: &mut dyn SlotReader) -> Result<Self, InternalError>;

        fn write_slots(&self, out: &mut dyn SlotWriter) -> Result<(), InternalError>;
    }
}

pub mod __macro {
    pub use icydb_core::__macro::PersistedFieldSlotCodec;
}

use icydb_core::value::Value;
use icydb_derive::PersistedRow;

#[derive(PersistedRow)]
struct RuntimeValueListEntity {
    values: Vec<Value>,
}

fn main() {}
