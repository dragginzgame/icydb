//! Bind IcyDB storage to one host-owned, bootstrapped memory runtime.
//! The runtime owns allocation policy, persistence and opening authority.

///
/// TESTS
///
#[cfg(test)]
mod tests;

use ic_memory::{DefaultMemoryImpl, MemoryRuntime, RuntimeMemory, RuntimeOpenError};
use std::{cell::OnceCell, rc::Rc};

thread_local! {
    static MEMORY_RUNTIME: OnceCell<Rc<MemoryRuntime<DefaultMemoryImpl>>> = const { OnceCell::new() };
}

/// Failure to bind the host's committed memory authority to IcyDB.
#[derive(Clone, Copy, Debug, Eq, thiserror::Error, PartialEq)]
pub enum MemoryRuntimeBindingError {
    /// Allocation bootstrap must succeed before publishing the runtime.
    #[error("memory runtime has not completed bootstrap")]
    NotBootstrapped,
    /// Another runtime already supplies IcyDB's storage authority.
    #[error("IcyDB is already bound to a different memory runtime")]
    AlreadyBound,
}

/// Bind a bootstrapped host runtime before opening IcyDB stores.
///
/// Composed hosts bootstrap their complete declarations and retain the same
/// `Rc` for their other consumers. Rebinding that exact instance is idempotent;
/// replacing it is rejected so existing handles cannot acquire another authority.
pub fn bind_memory_runtime(
    runtime: Rc<MemoryRuntime<DefaultMemoryImpl>>,
) -> Result<(), MemoryRuntimeBindingError> {
    if !runtime.is_bootstrapped() {
        return Err(MemoryRuntimeBindingError::NotBootstrapped);
    }
    MEMORY_RUNTIME.with(|bound| {
        if let Some(existing) = bound.get() {
            return if Rc::ptr_eq(existing, &runtime) {
                Ok(())
            } else {
                Err(MemoryRuntimeBindingError::AlreadyBound)
            };
        }
        bound
            .set(runtime)
            .map_err(|_| MemoryRuntimeBindingError::AlreadyBound)
    })
}

/// Return the bound host runtime without constructing or bootstrapping memory.
#[must_use]
pub fn memory_runtime() -> Option<Rc<MemoryRuntime<DefaultMemoryImpl>>> {
    MEMORY_RUNTIME.with(|bound| bound.get().cloned())
}

/// Open a declared key through IcyDB's bound host runtime.
#[doc(hidden)]
pub fn open_memory(stable_key: &str) -> Result<RuntimeMemory<DefaultMemoryImpl>, RuntimeOpenError> {
    memory_runtime()
        .ok_or(RuntimeOpenError::NotBootstrapped)?
        .open_memory(stable_key)
}
