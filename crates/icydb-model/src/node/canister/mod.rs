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

/// Parse one authored textual migration literal into its exact public atom.
#[doc(hidden)]
pub fn migration_literal_from_text(
    kind: &str,
    value: &str,
) -> Result<icydb_schema::ScalarLiteral, icydb_schema::SchemaContractError> {
    use icydb_schema::{
        Account, Blob, Date, Decimal, Duration, Principal, ScalarLiteral, Subaccount, Timestamp,
        U256, Ulid,
    };

    let invalid = || icydb_schema::SchemaContractError::InvalidLiteral;
    match kind {
        "account" => Account::from_str(value)
            .map(ScalarLiteral::Account)
            .map_err(|_| invalid()),
        "blob" => Blob::from_hex_literal(value).map(ScalarLiteral::Blob),
        "date" => Date::parse(value)
            .map(ScalarLiteral::Date)
            .ok_or_else(invalid),
        "decimal" => Decimal::from_str(value)
            .map(ScalarLiteral::Decimal)
            .map_err(|_| invalid()),
        "duration" => Duration::parse_flexible(value)
            .map(ScalarLiteral::Duration)
            .map_err(|_| invalid()),
        "principal" => Principal::from_str(value)
            .map(ScalarLiteral::Principal)
            .map_err(|_| invalid()),
        "subaccount" => {
            // Migration literals retain their exact unprefixed 32-byte grammar.
            if value.len() != 64 {
                return Err(invalid());
            }
            Subaccount::from_hex(value).map(ScalarLiteral::Subaccount)
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

///
/// Canister
///

#[derive(Clone, Debug, Serialize)]
pub struct Canister {
    def: Def,
    memory_namespace: &'static str,
    #[serde(skip)]
    migration_plan: Option<MigrationPlanConstructor>,
    migration_attempt: u64,
}

impl Canister {
    #[must_use]
    /// Declare a canister namespace and optional migration constructor, starting at attempt zero.
    pub const fn new(
        def: Def,
        memory_namespace: &'static str,
        migration_plan: Option<MigrationPlanConstructor>,
    ) -> Self {
        Self {
            def,
            memory_namespace,
            migration_plan,
            migration_attempt: 0,
        }
    }

    /// Set the explicit retry identity for this declaration's migration.
    #[must_use]
    pub const fn with_migration_attempt(mut self, attempt: u64) -> Self {
        self.migration_attempt = attempt;
        self
    }

    /// Return the operator-selected migration attempt; zero is the first attempt.
    #[must_use]
    pub const fn migration_attempt(&self) -> u64 {
        self.migration_attempt
    }

    #[must_use]
    /// Borrow the Rust definition identity used to register this declaration.
    pub const fn def(&self) -> &Def {
        &self.def
    }

    #[must_use]
    /// Return the explicit namespace used for durable memory keys.
    pub const fn memory_namespace(&self) -> &'static str {
        self.memory_namespace
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
    /// Construct the commit-control memory key in this canister namespace.
    pub fn commit_stable_key(&self) -> String {
        stable_memory_key(self.memory_namespace(), "commit", "control")
    }

    #[must_use]
    /// Construct the integrity-progress memory key in this canister namespace.
    pub fn integrity_progress_stable_key(&self) -> String {
        stable_memory_key(self.memory_namespace(), "integrity", "progress")
    }

    #[must_use]
    /// Construct the startup-control memory key in this canister namespace.
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
        if self.migration_attempt != 0 && self.migration_plan.is_none() {
            err!(errs, "migration_attempt requires a declared migration plan");
        }

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
