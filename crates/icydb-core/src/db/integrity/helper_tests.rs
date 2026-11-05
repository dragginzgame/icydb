//! Test-only constructors and adapters for the parent module.

use super::*;

impl DatabaseIncarnationId {
    pub(crate) const fn for_tests(fill: u8) -> Self {
        let mut bytes = [fill; 16];
        if fill == 0 {
            bytes[15] = 1;
        }
        Self(bytes)
    }
}
