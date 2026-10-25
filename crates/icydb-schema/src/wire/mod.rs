//! Native RPC resource policy; IC Wire owns the codec and envelopes.

#[cfg(not(target_family = "wasm"))]
mod metadata;

use ic_wire::Limits;
use serde::{Serialize, de::DeserializeOwned};

pub use ic_wire::Error;

/// Maximum admitted payload traversal nodes.
pub const MAX_NODES: usize = 1_048_576;
/// Maximum admitted Serde nesting, including endpoint record/container boundaries.
/// Allows the 64-level schema contract plus enum, collection and reply wrappers.
pub const MAX_DEPTH: usize = 256;
/// Browser traversal allowance for bounded interactive views.
///
/// Intentionally below native policy; large replies can require smaller pages.
/// This preserves the existing browser contract; it does not bound JS heap use.
pub const BROWSER_MAX_NODES: usize = 32_768;
/// Browser nesting allowance, intentionally below the native schema allowance.
///
/// Deep native values remain unsupported by the browser view; callers receive a
/// limit refusal rather than truncated or partially decoded output.
pub const BROWSER_MAX_DEPTH: usize = 32;

/// Conservative allocation allowance for decoded native payloads.
pub const MAX_HEAP_BYTES: usize = 256 * 1024 * 1024;
/// Maximum public reply envelope bytes.
pub const MAX_REPLY_BYTES: usize = 3 * 1024 * 1024;
/// Maximum owned endpoint request envelope bytes.
pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

/// Admission allowances for payload or complete frame bytes.
#[must_use]
pub const fn limits(bytes: usize) -> Limits {
    Limits {
        bytes,
        allocation: MAX_HEAP_BYTES.saturating_add(bytes),
        nodes: MAX_NODES,
        depth: MAX_DEPTH,
    }
}

/// Allocation-free sizing policy retaining the same traversal heap allowance.
///
/// A size may exceed delivery limits so precommit callers can return the typed
/// oversized-reply error. No output buffer is allocated under this allowance.
#[must_use]
pub const fn sizing_limits() -> Limits {
    limits(usize::MAX - MAX_HEAP_BYTES)
}

/// Encode a native payload through IC Wire's bounded codec.
pub fn encode_one<T: Serialize>(value: T) -> Result<Vec<u8>, Error> {
    ic_wire::native::encode(&value, limits(MAX_REPLY_BYTES))
}

/// Measure a reply payload under the same resource limits as [`encode_one`].
///
/// This allocates no output buffer. Unlike [`encoded_len`], it refuses payloads
/// above the reply byte ceiling. Pass the same owned or borrowed value shape
/// as encoding so IC Wire applies identical traversal admission.
pub fn encoded_reply_len<T: Serialize>(value: T) -> Result<usize, Error> {
    ic_wire::native::encoded_len(&value, limits(MAX_REPLY_BYTES))
}

/// Decode one complete payload through IC Wire's bounded codec.
///
/// Maps, sets and custom visitors may normalize input; successful decoding does
/// not establish canonical ordering or uniqueness. Retain the original bytes
/// for exact retries, signatures or hashes instead of re-encoding the value.
pub fn decode_one<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    ic_wire::native::decode(bytes, limits(MAX_REPLY_BYTES))
}

/// Measure native payload bytes without allocating output storage.
pub fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<usize, Error> {
    ic_wire::native::encoded_len(value, sizing_limits())
}

#[cfg(test)]
mod tests;
