//! Module: cursor::token::grouped
//! Responsibility: grouped continuation token domain type and wire conversion helpers.
//! Does not own: scalar continuation token policy or planner continuation semantics.
//! Boundary: maps grouped cursor tokens between runtime domain and bounded wire payloads.

use crate::{
    db::{cursor::ContinuationSignature, direction::Direction},
    value::Value,
};

use crate::db::cursor::token::{TokenWireError, decode_grouped_token, encode_grouped_token};

///
/// GroupedContinuationToken
///
/// Dedicated continuation payload for grouped result pagination.
/// Sealed through the shared cursor envelope before crossing a request boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedContinuationToken {
    signature: ContinuationSignature,
    last_group_key: Vec<Value>,
    direction: Direction,
    initial_offset: u32,
}

impl GroupedContinuationToken {
    pub(in crate::db) const fn new_with_direction(
        signature: ContinuationSignature,
        last_group_key: Vec<Value>,
        direction: Direction,
        initial_offset: u32,
    ) -> Self {
        Self {
            signature,
            last_group_key,
            direction,
            initial_offset,
        }
    }

    pub(in crate::db) fn into_components(
        self,
    ) -> (ContinuationSignature, Vec<Value>, Direction, u32) {
        (
            self.signature,
            self.last_group_key,
            self.direction,
            self.initial_offset,
        )
    }

    pub(in crate::db) fn encode(&self, mac_key: &[u8; 32]) -> Result<Vec<u8>, TokenWireError> {
        encode_grouped_token(
            self.signature,
            self.last_group_key.as_slice(),
            self.direction,
            self.initial_offset,
            mac_key,
        )
    }

    pub(in crate::db) fn decode(bytes: &[u8], mac_key: &[u8; 32]) -> Result<Self, TokenWireError> {
        let payload = decode_grouped_token(bytes, mac_key)?;

        Ok(Self::new_with_direction(
            payload.signature,
            payload.last_group_key,
            payload.direction,
            payload.initial_offset,
        ))
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;
