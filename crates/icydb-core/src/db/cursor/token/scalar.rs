//! Module: cursor::token::scalar
//! Responsibility: authenticated current-form scalar page token domain.
//! Does not own: query planning, source-revision proofs, or public DTOs.
//! Boundary: validated scalar page state -> bounded MAC-protected wire bytes.

use crate::{
    db::{
        cursor::{ContinuationSignature, CursorBoundary, token::TokenWireError},
        query::plan::OrderDirection,
    },
    types::EntityTag,
};

use crate::db::cursor::token::{decode_scalar_token, encode_scalar_token};

/// Maximum number of explicit and hidden ordering terms in one scalar token.
pub(in crate::db::cursor) const MAX_SCALAR_CURSOR_ORDER_TERMS: usize = 32;

/// Maximum combined binary encoding of the logical ordering boundary.
pub(in crate::db::cursor) const MAX_SCALAR_CURSOR_LOGICAL_BOUNDARY_BYTES: usize = 4 * 1024;

/// Scalar traversal semantics selected at the outward API boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarPageMode {
    Live,
    Exhaustive,
}

/// One canonical explicit or planner-appended ordering term.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarOrderTermContract {
    identity: String,
    direction: OrderDirection,
}

impl ScalarOrderTermContract {
    #[must_use]
    pub(in crate::db) fn new(identity: impl Into<String>, direction: OrderDirection) -> Self {
        Self {
            identity: identity.into(),
            direction,
        }
    }

    #[must_use]
    pub(in crate::db) const fn identity(&self) -> &str {
        self.identity.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn direction(&self) -> OrderDirection {
        self.direction
    }
}

/// Runtime and accepted-schema authority bound into every scalar token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarPageTokenAuthority {
    database_incarnation: [u8; 16],
    accepted_root_revision: u64,
    accepted_root_fingerprint_method: u8,
    accepted_root_fingerprint: [u8; 32],
    accepted_entity_fingerprint: [u8; 16],
    entity_tag: EntityTag,
}

impl ScalarPageTokenAuthority {
    #[must_use]
    pub(in crate::db) const fn new(
        database_incarnation: [u8; 16],
        accepted_root_revision: u64,
        accepted_root_fingerprint_method: u8,
        accepted_root_fingerprint: [u8; 32],
        accepted_entity_fingerprint: [u8; 16],
        entity_tag: EntityTag,
    ) -> Self {
        Self {
            database_incarnation,
            accepted_root_revision,
            accepted_root_fingerprint_method,
            accepted_root_fingerprint,
            accepted_entity_fingerprint,
            entity_tag,
        }
    }

    #[must_use]
    pub(in crate::db) const fn database_incarnation(self) -> [u8; 16] {
        self.database_incarnation
    }

    #[must_use]
    pub(in crate::db) const fn accepted_root_revision(self) -> u64 {
        self.accepted_root_revision
    }

    #[must_use]
    pub(in crate::db) const fn accepted_root_fingerprint_method(self) -> u8 {
        self.accepted_root_fingerprint_method
    }

    #[must_use]
    pub(in crate::db) const fn accepted_root_fingerprint(self) -> [u8; 32] {
        self.accepted_root_fingerprint
    }

    #[must_use]
    pub(in crate::db) const fn accepted_entity_fingerprint(self) -> [u8; 16] {
        self.accepted_entity_fingerprint
    }

    #[must_use]
    pub(in crate::db) const fn entity_tag(self) -> EntityTag {
        self.entity_tag
    }
}

/// Immutable total LIMIT/OFFSET and page-envelope profile identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarPageTokenWindow {
    initial_offset: u32,
    total_limit: Option<u32>,
    envelope_profile_identity: u64,
}

impl ScalarPageTokenWindow {
    #[must_use]
    pub(in crate::db) const fn new(
        initial_offset: u32,
        total_limit: Option<u32>,
        envelope_profile_identity: u64,
    ) -> Self {
        Self {
            initial_offset,
            total_limit,
            envelope_profile_identity,
        }
    }

    #[must_use]
    pub(in crate::db) const fn initial_offset(self) -> u32 {
        self.initial_offset
    }

    #[must_use]
    pub(in crate::db) const fn total_limit(self) -> Option<u32> {
        self.total_limit
    }

    #[must_use]
    pub(in crate::db) const fn envelope_profile_identity(self) -> u64 {
        self.envelope_profile_identity
    }
}

/// Logical and physical progress carried between scalar pages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarPageTokenProgress {
    last_emitted_logical: Option<CursorBoundary>,
    last_consumed_physical: Option<Vec<u8>>,
    unconsumed_lookahead: Option<Vec<u8>>,
    matching_rows_skipped: u64,
    rows_emitted: u64,
}

impl ScalarPageTokenProgress {
    #[must_use]
    pub(in crate::db) const fn new(
        last_emitted_logical: Option<CursorBoundary>,
        last_consumed_physical: Option<Vec<u8>>,
        unconsumed_lookahead: Option<Vec<u8>>,
        matching_rows_skipped: u64,
        rows_emitted: u64,
    ) -> Self {
        Self {
            last_emitted_logical,
            last_consumed_physical,
            unconsumed_lookahead,
            matching_rows_skipped,
            rows_emitted,
        }
    }

    #[must_use]
    pub(in crate::db) const fn last_emitted_logical(&self) -> Option<&CursorBoundary> {
        self.last_emitted_logical.as_ref()
    }

    #[must_use]
    pub(in crate::db) fn last_consumed_physical(&self) -> Option<&[u8]> {
        self.last_consumed_physical.as_deref()
    }

    #[must_use]
    pub(in crate::db) fn unconsumed_lookahead(&self) -> Option<&[u8]> {
        self.unconsumed_lookahead.as_deref()
    }

    #[must_use]
    pub(in crate::db) const fn matching_rows_skipped(&self) -> u64 {
        self.matching_rows_skipped
    }

    #[must_use]
    pub(in crate::db) const fn rows_emitted(&self) -> u64 {
        self.rows_emitted
    }
}

/// Complete authenticated current-form scalar continuation state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarPageToken {
    mode: ScalarPageMode,
    signature: ContinuationSignature,
    authority: ScalarPageTokenAuthority,
    window: ScalarPageTokenWindow,
    order_terms: Vec<ScalarOrderTermContract>,
    progress: ScalarPageTokenProgress,
}

impl ScalarPageToken {
    #[must_use]
    pub(in crate::db) const fn new(
        mode: ScalarPageMode,
        signature: ContinuationSignature,
        authority: ScalarPageTokenAuthority,
        window: ScalarPageTokenWindow,
        order_terms: Vec<ScalarOrderTermContract>,
        progress: ScalarPageTokenProgress,
    ) -> Self {
        Self {
            mode,
            signature,
            authority,
            window,
            order_terms,
            progress,
        }
    }

    pub(in crate::db) fn encode(&self, mac_key: &[u8; 32]) -> Result<Vec<u8>, TokenWireError> {
        encode_scalar_token(self, mac_key)
    }

    pub(in crate::db) fn decode(bytes: &[u8], mac_key: &[u8; 32]) -> Result<Self, TokenWireError> {
        decode_scalar_token(bytes, mac_key)
    }

    #[must_use]
    pub(in crate::db) const fn mode(&self) -> ScalarPageMode {
        self.mode
    }

    #[must_use]
    pub(in crate::db) const fn signature(&self) -> ContinuationSignature {
        self.signature
    }

    #[must_use]
    pub(in crate::db) const fn authority(&self) -> ScalarPageTokenAuthority {
        self.authority
    }

    #[must_use]
    pub(in crate::db) const fn window(&self) -> ScalarPageTokenWindow {
        self.window
    }

    #[must_use]
    pub(in crate::db) const fn order_terms(&self) -> &[ScalarOrderTermContract] {
        self.order_terms.as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn progress(&self) -> &ScalarPageTokenProgress {
        &self.progress
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{cursor::CursorBoundarySlot, query::plan::OrderDirection},
        value::Value,
    };

    fn token() -> ScalarPageToken {
        ScalarPageToken::new(
            ScalarPageMode::Live,
            ContinuationSignature::from_bytes([0x11; 32]),
            ScalarPageTokenAuthority::new(
                [0x22; 16],
                7,
                1,
                [0x33; 32],
                [0x44; 16],
                EntityTag::new(9),
            ),
            ScalarPageTokenWindow::new(0, Some(2_048), 0x55),
            vec![
                ScalarOrderTermContract::new("score", OrderDirection::Desc),
                ScalarOrderTermContract::new("nullable", OrderDirection::Asc),
                ScalarOrderTermContract::new("id", OrderDirection::Desc),
            ],
            ScalarPageTokenProgress::new(
                Some(CursorBoundary {
                    slots: vec![
                        CursorBoundarySlot::Present(Value::Nat64(10)),
                        CursorBoundarySlot::Missing,
                        CursorBoundarySlot::Present(Value::Text("id-7".to_string())),
                    ],
                }),
                Some(vec![1, 2, 3]),
                Some(vec![4, 5, 6]),
                3,
                1_024,
            ),
        )
    }

    #[test]
    fn authenticated_scalar_token_round_trips_complete_mixed_order_contract() {
        let key = [0x66; 32];
        let token = token();
        let encoded = token.encode(&key).expect("bounded token should encode");
        assert_eq!(encoded[4], 1);
        let decoded = ScalarPageToken::decode(encoded.as_slice(), &key)
            .expect("authenticated token should decode");

        assert_eq!(decoded, token);
    }

    #[test]
    fn authenticated_scalar_token_rejects_tampering_and_wrong_database_key() {
        let key = [0x66; 32];
        let mut encoded = token().encode(&key).expect("bounded token should encode");
        let payload_index = 12;
        encoded[payload_index] ^= 0x80;

        assert!(ScalarPageToken::decode(encoded.as_slice(), &key).is_err());
        assert!(
            ScalarPageToken::decode(
                token()
                    .encode(&key)
                    .expect("bounded token should encode")
                    .as_slice(),
                &[0x77; 32],
            )
            .is_err()
        );
    }

    #[test]
    fn authenticated_scalar_token_rejects_oversized_order_contracts() {
        let mut order_terms = Vec::new();
        for index in 0..=MAX_SCALAR_CURSOR_ORDER_TERMS {
            order_terms.push(ScalarOrderTermContract::new(
                format!("field_{index}"),
                OrderDirection::Asc,
            ));
        }
        let mut token = token();
        token.order_terms = order_terms;

        assert!(token.encode(&[0x66; 32]).is_err());
    }

    #[test]
    fn authenticated_scalar_token_rejects_oversized_logical_boundaries() {
        let mut token = token();
        token.progress.last_emitted_logical = Some(CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Text(
                "x".repeat(MAX_SCALAR_CURSOR_LOGICAL_BOUNDARY_BYTES + 1),
            ))],
        });

        assert!(token.encode(&[0x66; 32]).is_err());
    }
}
