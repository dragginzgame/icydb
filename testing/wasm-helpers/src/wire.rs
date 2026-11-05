//! Shared IC Wire contract for the dynamic row-count endpoint trial.
//! Transport, query execution and database formats remain consumer-owned.

use ic_wire::Limits;

/// Current contract for the existing dynamic row-count query.
///
/// Unit requests still carry the method/direction envelope. An incompatible
/// layout must receive a new identity and replace its callers together.
pub struct DynamicRowsQuery;

impl DynamicRowsQuery {
    /// Canonical method identity, unchanged across native-codec adoption.
    pub const ID: u64 = u64::from_le_bytes(*b"ICYDBDR1");
    /// Public IC query name.
    pub const NAME: &'static str = "query_one_entity_dynamic_rows";
    /// Unit request envelope; native traversal needs four visits and three layers.
    pub const REQUEST_LIMITS: Limits = Limits {
        bytes: 11,
        allocation: 11,
        nodes: 4,
        depth: 3,
    };
    /// Frame bytes plus two native value-size charges; the payload remains one u32.
    pub const RESPONSE_LIMITS: Limits = Limits {
        bytes: 16,
        allocation: 80, // 16 frame bytes plus two u32 charges (2 × 8 × 4).
        nodes: 4,
        depth: 3,
    };
}

#[cfg(test)]
mod tests;
