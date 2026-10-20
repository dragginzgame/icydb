//! Current raw request/reply contracts for generated IcyDB endpoints.
//!
//! Endpoint identities bind canonical IC Wire envelopes to the maintained native
//! payloads. Database storage codecs and host-owned RPC contracts are separate.

use icydb_schema::wire::limits;
use serde::{Serialize, de::DeserializeOwned};

pub use icydb_schema::wire::{
    Error, MAX_REPLY_BYTES, MAX_REQUEST_BYTES, decode_one, encode_one, encoded_len,
};

const ID_BASE: u64 = u64::from_le_bytes(*b"ICYDBW00");

/// One maintained generated endpoint and its current byte-contract identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Endpoint {
    /// The `icydb_query` method.
    Query,
    /// The `icydb_ddl` method.
    Ddl,
    /// The `icydb_update` method.
    Update,
    /// The `icydb_integrity` method.
    Integrity,
    /// The `icydb_fixtures_reset` method.
    FixturesReset,
    /// The `icydb_fixtures_load` method.
    FixturesLoad,
    /// The `icydb_metrics` method.
    Metrics,
    /// The `icydb_metrics_reset` method.
    MetricsReset,
    /// The `icydb_snapshot` method.
    Snapshot,
    /// The `icydb_schema` method.
    Schema,
    /// The `icydb_schema_migrate` method.
    SchemaMigrate,
    /// The `icydb_schema_migration` method.
    SchemaMigration,
}

impl Endpoint {
    /// Exact public IC method name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Query => "icydb_query",
            Self::Ddl => "icydb_ddl",
            Self::Update => "icydb_update",
            Self::Integrity => "icydb_integrity",
            Self::FixturesReset => "icydb_fixtures_reset",
            Self::FixturesLoad => "icydb_fixtures_load",
            Self::Metrics => "icydb_metrics",
            Self::MetricsReset => "icydb_metrics_reset",
            Self::Snapshot => "icydb_snapshot",
            Self::Schema => "icydb_schema",
            Self::SchemaMigrate => "icydb_schema_migrate",
            Self::SchemaMigration => "icydb_schema_migration",
        }
    }
    /// Current protocol identity; incompatible payload changes require a new ID.
    #[must_use]
    pub const fn id(self) -> u64 {
        ID_BASE
            + match self {
                Self::Query => 1,
                Self::Ddl => 2,
                Self::Update => 3,
                Self::Integrity => 4,
                Self::FixturesReset => 5,
                Self::FixturesLoad => 6,
                Self::Metrics => 7,
                Self::MetricsReset => 8,
                Self::Snapshot => 9,
                Self::Schema => 10,
                Self::SchemaMigrate => 11,
                Self::SchemaMigration => 12,
            }
    }
    /// Resolve a generated endpoint by its exact public name.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "icydb_query" => Some(Self::Query),
            "icydb_ddl" => Some(Self::Ddl),
            "icydb_update" => Some(Self::Update),
            "icydb_integrity" => Some(Self::Integrity),
            "icydb_fixtures_reset" => Some(Self::FixturesReset),
            "icydb_fixtures_load" => Some(Self::FixturesLoad),
            "icydb_metrics" => Some(Self::Metrics),
            "icydb_metrics_reset" => Some(Self::MetricsReset),
            "icydb_snapshot" => Some(Self::Snapshot),
            "icydb_schema" => Some(Self::Schema),
            "icydb_schema_migrate" => Some(Self::SchemaMigrate),
            "icydb_schema_migration" => Some(Self::SchemaMigration),
            _ => None,
        }
    }
    /// Resolve a declared current protocol identity.
    #[must_use]
    pub const fn from_id(id: u64) -> Option<Self> {
        match id.wrapping_sub(ID_BASE) {
            1 => Some(Self::Query),
            2 => Some(Self::Ddl),
            3 => Some(Self::Update),
            4 => Some(Self::Integrity),
            5 => Some(Self::FixturesReset),
            6 => Some(Self::FixturesLoad),
            7 => Some(Self::Metrics),
            8 => Some(Self::MetricsReset),
            9 => Some(Self::Snapshot),
            10 => Some(Self::Schema),
            11 => Some(Self::SchemaMigrate),
            12 => Some(Self::SchemaMigration),
            _ => None,
        }
    }
    /// Encode one canonical request for this endpoint.
    pub fn encode_request<T: Serialize>(self, value: T) -> Result<Vec<u8>, Error> {
        ic_wire::native::encode_request(self.id(), &value, limits(MAX_REQUEST_BYTES))
    }
    /// Admit identity, direction, resources and canonical request bytes.
    pub fn decode_request<T: Serialize + DeserializeOwned>(self, bytes: &[u8]) -> Result<T, Error> {
        ic_wire::native::decode_request(self.id(), bytes, limits(MAX_REQUEST_BYTES))
    }
    /// Encode one canonical response for this endpoint.
    pub fn encode_response<T: Serialize>(self, value: T) -> Result<Vec<u8>, Error> {
        ic_wire::native::encode_response(self.id(), &value, limits(MAX_REPLY_BYTES))
    }
    /// Admit identity, direction, resources and canonical response bytes.
    pub fn decode_response<T: Serialize + DeserializeOwned>(
        self,
        bytes: &[u8],
    ) -> Result<T, Error> {
        ic_wire::native::decode_response(self.id(), bytes, limits(MAX_REPLY_BYTES))
    }
}

/// Encode arguments for a generated composed init hook.
pub fn encode_init_arguments<T: Serialize>(arguments: T) -> Result<Vec<u8>, Error> {
    ic_wire::native::encode_request(ID_BASE + 13, &arguments, limits(MAX_REQUEST_BYTES))
}

/// Encode arguments for a generated composed post-upgrade hook.
pub fn encode_post_upgrade_arguments<T: Serialize>(arguments: T) -> Result<Vec<u8>, Error> {
    ic_wire::native::encode_request(ID_BASE + 14, &arguments, limits(MAX_REQUEST_BYTES))
}

/// Decode arguments for a generated composed init hook before bootstrap.
#[doc(hidden)]
pub fn decode_init_arguments<T: Serialize + DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    ic_wire::native::decode_request(ID_BASE + 13, bytes, limits(MAX_REQUEST_BYTES))
}

/// Decode arguments for a generated composed post-upgrade hook before bootstrap.
#[doc(hidden)]
pub fn decode_post_upgrade_arguments<T: Serialize + DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, Error> {
    ic_wire::native::decode_request(ID_BASE + 14, bytes, limits(MAX_REQUEST_BYTES))
}

///
/// TESTS
///
#[cfg(test)]
mod tests;
