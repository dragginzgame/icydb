//! Native database RPC contracts shared by clients and host adapters.
//!
//! This module carries method identities and delegates all framing/admission to
//! IC Wire. IC exports, authorization and lifecycle remain adapter responsibilities.

use ic_wire::{Error, Limits};
use serde::{Serialize, de::DeserializeOwned};

const ID_BASE: u64 = u64::from_le_bytes(*b"ICYDBW00");

// One declaration owns every projection of the fixed method identity roster.
macro_rules! define_methods {
    ($( $variant:ident => ($name:literal, $id:literal), )*) => {
        /// A database RPC method and its exact byte-contract identity.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum RpcMethod {
            $( #[doc = $name] $variant, )*
        }

        impl RpcMethod {
            /// Maintained native RPC methods, in declaration order.
            pub const ALL: &[Self] = &[$( Self::$variant, )*];

            /// Exact public IC method name.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self { $( Self::$variant => $name, )* }
            }

            /// Current protocol identity. Payload changes require explicit coordination.
            #[must_use]
            pub const fn id(self) -> u64 {
                ID_BASE + match self { $( Self::$variant => $id, )* }
            }

            /// Resolve one exact public method name.
            #[must_use]
            pub fn from_name(name: &str) -> Option<Self> {
                match name { $( $name => Some(Self::$variant), )* _ => None }
            }

            /// Resolve one declared protocol identity.
            #[must_use]
            pub const fn from_id(id: u64) -> Option<Self> {
                match id.wrapping_sub(ID_BASE) {
                    $( $id => Some(Self::$variant), )* _ => None,
                }
            }
        }
    };
}

define_methods! {
    Query => ("icydb_query", 1),
    Ddl => ("icydb_ddl", 2),
    Update => ("icydb_update", 3),
    Integrity => ("icydb_integrity", 4),
    FixturesReset => ("icydb_fixtures_reset", 5),
    FixturesLoad => ("icydb_fixtures_load", 6),
    Metrics => ("icydb_metrics", 7),
    MetricsReset => ("icydb_metrics_reset", 8),
    Snapshot => ("icydb_snapshot", 9),
    Schema => ("icydb_schema", 10),
    SchemaMigrate => ("icydb_schema_migrate", 11),
    SchemaMigration => ("icydb_schema_migration", 12),
}

impl RpcMethod {
    /// Encode one canonical request for this endpoint.
    pub fn encode_request<T: Serialize>(self, value: T, limits: Limits) -> Result<Vec<u8>, Error> {
        ic_wire::native::encode_request(self.id(), &value, limits)
    }
    /// Admit identity, direction, resources and request payload primitives.
    /// Collection ordering and uniqueness remain the caller's semantic contract.
    pub fn decode_request<T: DeserializeOwned>(
        self,
        bytes: &[u8],
        limits: Limits,
    ) -> Result<T, Error> {
        ic_wire::native::decode_request(self.id(), bytes, limits)
    }
    /// Encode one canonical response for this endpoint.
    pub fn encode_response<T: Serialize>(self, value: T, limits: Limits) -> Result<Vec<u8>, Error> {
        ic_wire::native::encode_response(self.id(), &value, limits)
    }
    /// Admit identity, direction, resources and response payload primitives.
    /// Retain received bytes when exact frame identity is needed after decoding.
    pub fn decode_response<T: DeserializeOwned>(
        self,
        bytes: &[u8],
        limits: Limits,
    ) -> Result<T, Error> {
        ic_wire::native::decode_response(self.id(), bytes, limits)
    }
}
