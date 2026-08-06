//! Module: cursor::token
//! Responsibility: typed continuation token wire contracts for scalar/grouped cursor paths.
//! Does not own: higher-level cursor validation, ordering policy, or
//! resume-bound semantics.
//! Boundary: defines the current token payloads consumed by cursor
//! encode/decode boundaries.

mod bytes;
mod codec;
mod error;
mod grouped;
mod scalar;
mod value;

pub(in crate::db::cursor) use codec::MAX_CURSOR_TOKEN_BYTES;
pub(in crate::db::cursor::token) use codec::{
    decode_grouped_token, decode_scalar_token, encode_grouped_token, encode_scalar_token,
};
pub(in crate::db) use error::TokenWireError;
pub(in crate::db) use grouped::GroupedContinuationToken;
pub(in crate::db) use scalar::{
    ScalarOrderTermContract, ScalarPageMode, ScalarPageToken, ScalarPageTokenAuthority,
    ScalarPageTokenProgress, ScalarPageTokenWindow,
};
