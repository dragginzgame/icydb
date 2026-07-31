//! Engine operations for the canonical schema-owned `Duration` atom.

pub use icydb_schema::Duration;

use crate::traits::Repr;

impl Repr for Duration {
    type Inner = u64;

    fn repr(&self) -> Self::Inner {
        self.as_millis()
    }

    fn from_repr(inner: Self::Inner) -> Self {
        Self::from_millis(inner)
    }
}
