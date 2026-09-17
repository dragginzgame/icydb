//! Module: node::canister
//!
//! Responsibility: canister-level schema node metadata and memory allocation validation.
//! Does not own: ICP lifecycle management or runtime stable-memory implementation.
//! Boundary: validates permanent namespaces and store keys before runtime use.

#[cfg(test)]
mod tests;

use crate::node::{stable_memory_key, validate_stable_key, validate_stable_key_segment};
use crate::prelude::*;
use std::collections::BTreeMap;
use std::str::FromStr;

/// Build-time constructor for one source-declared coordinated migration plan.
pub type MigrationPlanConstructor =
    fn() -> Result<icydb_schema::SchemaMigrationPlan, icydb_schema::SchemaContractError>;

///
/// CanisterMemoryProfile
///
/// Build-time bucket sizing for IcyDB-owned shared memory bootstrap. This is
/// physical configuration, not accepted schema authority or allocation access.
/// A host that bootstraps first owns the effective setting instead.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanisterMemoryProfile {
    /// 256 KiB buckets; 8 GiB shared manager capacity before backing limits.
    Compact,
    /// 1 MiB buckets; 32 GiB shared manager capacity before backing limits.
    General,
    /// 8 MiB buckets; 256 GiB shared manager capacity before backing limits.
    HighHeadroom,
}

impl CanisterMemoryProfile {
    /// Return the immutable bucket size in 64 KiB Wasm pages.
    #[must_use]
    pub const fn bucket_size_pages(self) -> u16 {
        match self {
            Self::Compact => 4,
            Self::General => 16,
            Self::HighHeadroom => 128,
        }
    }
}

/// Parse one macro-validated textual migration literal into its exact public atom.
#[doc(hidden)]
pub fn migration_literal_from_text(
    kind: &str,
    value: &str,
) -> Result<icydb_schema::ScalarLiteral, icydb_schema::SchemaContractError> {
    use icydb_schema::{
        Account, Blob, Date, Decimal, Duration, IntBig, NatBig, Principal, ScalarLiteral,
        Subaccount, Timestamp, U256, Ulid,
    };

    let invalid = || icydb_schema::SchemaContractError::InvalidLiteral;
    match kind {
        "account" => Account::from_str(value)
            .map(ScalarLiteral::Account)
            .map_err(|_| invalid()),
        "blob" => decode_migration_hex(value)
            .map(Blob::from)
            .map(ScalarLiteral::Blob),
        "date" => Date::parse(value)
            .map(ScalarLiteral::Date)
            .ok_or_else(invalid),
        "decimal" => Decimal::from_str(value)
            .map(ScalarLiteral::Decimal)
            .map_err(|_| invalid()),
        "duration" => Duration::parse_flexible(value)
            .map(ScalarLiteral::Duration)
            .map_err(|_| invalid()),
        "int_big" => IntBig::from_str(value)
            .map(ScalarLiteral::IntBig)
            .map_err(|_| invalid()),
        "nat_big" => NatBig::from_str(value)
            .map(ScalarLiteral::NatBig)
            .map_err(|_| invalid()),
        "principal" => Principal::from_str(value)
            .map(ScalarLiteral::Principal)
            .map_err(|_| invalid()),
        "subaccount" => {
            let bytes = decode_migration_hex(value)?;
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| invalid())?;
            Ok(ScalarLiteral::Subaccount(Subaccount::from_array(bytes)))
        }
        "timestamp" => Timestamp::parse_flexible(value)
            .map(ScalarLiteral::Timestamp)
            .map_err(|_| invalid()),
        "u256" => U256::from_str(value)
            .map(ScalarLiteral::U256)
            .map_err(|_| invalid()),
        "ulid" => Ulid::from_str(value)
            .map(ScalarLiteral::Ulid)
            .map_err(|_| invalid()),
        _ => Err(invalid()),
    }
}

fn decode_migration_hex(value: &str) -> Result<Vec<u8>, icydb_schema::SchemaContractError> {
    if !value.len().is_multiple_of(2) {
        return Err(icydb_schema::SchemaContractError::InvalidLiteral);
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = decode_hex_nibble(pair[0])?;
            let low = decode_hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn decode_hex_nibble(value: u8) -> Result<u8, icydb_schema::SchemaContractError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(icydb_schema::SchemaContractError::InvalidLiteral),
    }
}

///
/// Canister
///

#[derive(Clone, Debug, Serialize)]
pub struct Canister {
    def: Def,
    memory_namespace: &'static str,
    memory_profile: CanisterMemoryProfile,
    #[serde(skip)]
    migration_plan: Option<MigrationPlanConstructor>,
}

impl Canister {
    #[must_use]
    pub const fn new(
        def: Def,
        memory_namespace: &'static str,
        migration_plan: Option<MigrationPlanConstructor>,
    ) -> Self {
        Self {
            def,
            memory_namespace,
            memory_profile: CanisterMemoryProfile::General,
            migration_plan,
        }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    #[must_use]
    pub const fn memory_namespace(&self) -> &'static str {
        self.memory_namespace
    }

    /// Select the profile used when IcyDB owns memory bootstrap.
    ///
    /// Existing memory must match its bucket size; this does not resize it.
    #[must_use]
    pub const fn with_memory_profile(mut self, profile: CanisterMemoryProfile) -> Self {
        self.memory_profile = profile;
        self
    }

    /// Return the configured profile; newly constructed nodes use `General`.
    #[must_use]
    pub const fn memory_profile(&self) -> CanisterMemoryProfile {
        self.memory_profile
    }

    /// Construct the optional source-declared migration plan.
    ///
    /// # Errors
    ///
    /// Returns the schema-contract error produced by the bounded declaration.
    pub fn migration_plan(
        &self,
    ) -> Result<Option<icydb_schema::SchemaMigrationPlan>, icydb_schema::SchemaContractError> {
        self.migration_plan
            .map(|constructor| constructor())
            .transpose()
    }

    #[must_use]
    pub fn commit_stable_key(&self) -> String {
        stable_memory_key(self.memory_namespace(), "commit", "control")
    }

    #[must_use]
    pub fn integrity_progress_stable_key(&self) -> String {
        stable_memory_key(self.memory_namespace(), "integrity", "progress")
    }

    #[must_use]
    pub fn startup_stable_key(&self) -> String {
        stable_memory_key(self.memory_namespace(), "startup", "control")
    }
}

impl MacroNode for Canister {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Canister {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_stable_key_segment(
            &mut errs,
            "canister memory_namespace",
            self.memory_namespace(),
        );
        for key in [
            self.commit_stable_key(),
            self.startup_stable_key(),
            self.integrity_progress_stable_key(),
        ] {
            validate_stable_key(&mut errs, "canister control key", &key);
        }

        let canister_path = self.def().path();
        {
            let schema = schema_read();
            let mut seen = BTreeMap::new();
            for (path, store) in
                schema.filter_nodes::<Store>(|store| store.canister() == canister_path)
            {
                if let Some(config) = store.journaled_memory_config()
                    && let Some(previous) = seen.insert(config.key(), path)
                {
                    err!(
                        errs,
                        "duplicate store key `{}` in canister `{}`: {} conflicts with {}",
                        config.key(),
                        canister_path,
                        previous,
                        path
                    );
                }
            }
        }
        errs.result()
    }
}

impl VisitableNode for Canister {
    fn route_key(&self) -> String {
        self.def().path()
    }
}
