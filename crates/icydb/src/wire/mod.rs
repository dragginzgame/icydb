//! Native payload encoding and database RPC resource policy.
//!
//! Native method contracts are shared by clients and host adapters through the
//! internal wiring surface. Adapters own IC exports, authorization and lifecycle.
//! Accepted database payloads retain their schema-owned codec admission.

pub(crate) mod method;

pub use icydb_schema::wire::{
    Error, MAX_REPLY_BYTES, MAX_REQUEST_BYTES, decode_one, encode_one, encoded_len, limits,
};

#[cfg(test)]
mod tests;
