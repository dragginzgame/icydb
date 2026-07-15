//! Module: db::bootstrap
//!
//! Responsibility: typed generated-database bootstrap failure.
//! Does not own: memory allocation policy or generated store initialization.
//! Boundary: preserves the `ic-memory` cause until an interface chooses a
//! compact public error projection.

use std::{convert::Infallible, fmt, sync::Arc};

use ic_memory::RuntimeBootstrapError;

/// Failure to bootstrap the generated database's stable-memory authority.
///
/// Cloning this error is cheap and preserves the original typed `ic-memory`
/// cause cached by generated database wiring.
#[derive(Clone, Debug)]
pub struct DatabaseBootstrapError {
    source: Arc<RuntimeBootstrapError<Infallible>>,
}

impl DatabaseBootstrapError {
    /// Borrow the authoritative `ic-memory` bootstrap failure.
    #[must_use]
    pub fn cause(&self) -> &RuntimeBootstrapError<Infallible> {
        &self.source
    }
}

impl From<RuntimeBootstrapError<Infallible>> for DatabaseBootstrapError {
    fn from(source: RuntimeBootstrapError<Infallible>) -> Self {
        Self {
            source: Arc::new(source),
        }
    }
}

impl fmt::Display for DatabaseBootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}

impl std::error::Error for DatabaseBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
