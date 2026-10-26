//! Native payload encoding and database RPC resource policy.
//!
//! Canic's IcyDB adapter owns endpoint identities and lifecycle envelopes.
//! Accepted database payloads retain their schema-owned codec admission.

pub use icydb_schema::wire::{
    Error, MAX_REPLY_BYTES, MAX_REQUEST_BYTES, decode_one, encode_one, encoded_len, limits,
};

#[cfg(test)]
mod tests;
