//! Test-only state and helpers for the parent module.

use super::*;

impl CratePaths {
    pub(crate) fn test() -> Self {
        Self {
            model: quote!(::icydb_model),
            runtime: quote!(::icydb),
        }
    }
}
