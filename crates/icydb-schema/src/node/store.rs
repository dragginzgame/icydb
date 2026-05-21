use crate::node::{
    validate_app_memory_id, validate_memory_id_in_range, validate_memory_id_not_reserved,
    validate_stable_key, validate_stable_key_segment,
};
use crate::prelude::*;

///
/// Store
///
/// Schema node describing stable IC BTreeMap memories that store:
/// - primary entity data
/// - all index data for that entity
/// - persisted schema metadata for that store
///

#[derive(Clone, Debug, Serialize)]
pub struct Store {
    def: Def,
    ident: &'static str,
    name: &'static str,
    canister: &'static str,
    data_memory_id: u8,
    index_memory_id: u8,
    schema_memory_id: u8,
}

impl Store {
    #[must_use]
    pub const fn new(
        def: Def,
        ident: &'static str,
        store_name: &'static str,
        canister: &'static str,
        data_memory_id: u8,
        index_memory_id: u8,
        schema_memory_id: u8,
    ) -> Self {
        Self {
            def,
            ident,
            name: store_name,
            canister,
            data_memory_id,
            index_memory_id,
            schema_memory_id,
        }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    #[must_use]
    pub const fn ident(&self) -> &'static str {
        self.ident
    }

    #[must_use]
    pub const fn store_name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn canister(&self) -> &'static str {
        self.canister
    }

    #[must_use]
    pub const fn data_memory_id(&self) -> u8 {
        self.data_memory_id
    }

    #[must_use]
    pub const fn index_memory_id(&self) -> u8 {
        self.index_memory_id
    }

    #[must_use]
    pub const fn schema_memory_id(&self) -> u8 {
        self.schema_memory_id
    }

    #[must_use]
    pub fn data_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.allocation(memory_namespace, StoreMemoryRole::Data)
    }

    #[must_use]
    pub fn index_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.allocation(memory_namespace, StoreMemoryRole::Index)
    }

    #[must_use]
    pub fn schema_allocation(&self, memory_namespace: &str) -> StableMemoryAllocation {
        self.allocation(memory_namespace, StoreMemoryRole::Schema)
    }

    #[must_use]
    pub fn allocation(
        &self,
        memory_namespace: &str,
        role: StoreMemoryRole,
    ) -> StableMemoryAllocation {
        let memory_id = match role {
            StoreMemoryRole::Data => self.data_memory_id,
            StoreMemoryRole::Index => self.index_memory_id,
            StoreMemoryRole::Schema => self.schema_memory_id,
        };

        StableMemoryAllocation::new(
            memory_id,
            stable_memory_key(memory_namespace, self.store_name(), role.as_str()),
            None,
            None,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreMemoryRole {
    Data,
    Index,
    Schema,
}

impl StoreMemoryRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Index => "index",
            Self::Schema => "schema",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StableMemoryAllocation {
    memory_id: u8,
    stable_key: String,
    schema_version: Option<u32>,
    schema_fingerprint: Option<String>,
}

impl StableMemoryAllocation {
    #[must_use]
    pub const fn new(
        memory_id: u8,
        stable_key: String,
        schema_version: Option<u32>,
        schema_fingerprint: Option<String>,
    ) -> Self {
        Self {
            memory_id,
            stable_key,
            schema_version,
            schema_fingerprint,
        }
    }

    #[must_use]
    pub const fn memory_id(&self) -> u8 {
        self.memory_id
    }

    #[must_use]
    pub const fn stable_key(&self) -> &str {
        self.stable_key.as_str()
    }

    #[must_use]
    pub const fn schema_version(&self) -> Option<u32> {
        self.schema_version
    }

    #[must_use]
    pub const fn schema_fingerprint(&self) -> Option<&str> {
        match &self.schema_fingerprint {
            Some(value) => Some(value.as_str()),
            None => None,
        }
    }

    #[must_use]
    pub fn same_identity_as(&self, other: &Self) -> bool {
        self.memory_id == other.memory_id && self.stable_key == other.stable_key
    }
}

#[must_use]
pub fn stable_memory_key(memory_namespace: &str, store_name: &str, role: &str) -> String {
    format!("icydb.{memory_namespace}.{store_name}.{role}.v1")
}

impl MacroNode for Store {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Store {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        match schema.cast_node::<Canister>(self.canister()) {
            Ok(canister) => {
                validate_stable_key_segment(&mut errs, "store store_name", self.store_name());

                // Validate data memory ID
                validate_memory_id_in_range(
                    &mut errs,
                    "data_memory_id",
                    self.data_memory_id(),
                    canister.memory_min(),
                    canister.memory_max(),
                );
                validate_app_memory_id(&mut errs, "data_memory_id", self.data_memory_id());
                validate_memory_id_not_reserved(&mut errs, "data_memory_id", self.data_memory_id());
                validate_stable_key(
                    &mut errs,
                    "data stable key",
                    self.data_allocation(canister.memory_namespace())
                        .stable_key(),
                );

                // Validate index memory ID
                validate_memory_id_in_range(
                    &mut errs,
                    "index_memory_id",
                    self.index_memory_id(),
                    canister.memory_min(),
                    canister.memory_max(),
                );
                validate_app_memory_id(&mut errs, "index_memory_id", self.index_memory_id());
                validate_memory_id_not_reserved(
                    &mut errs,
                    "index_memory_id",
                    self.index_memory_id(),
                );
                validate_stable_key(
                    &mut errs,
                    "index stable key",
                    self.index_allocation(canister.memory_namespace())
                        .stable_key(),
                );

                // Validate schema memory ID
                validate_memory_id_in_range(
                    &mut errs,
                    "schema_memory_id",
                    self.schema_memory_id(),
                    canister.memory_min(),
                    canister.memory_max(),
                );
                validate_app_memory_id(&mut errs, "schema_memory_id", self.schema_memory_id());
                validate_memory_id_not_reserved(
                    &mut errs,
                    "schema_memory_id",
                    self.schema_memory_id(),
                );
                validate_stable_key(
                    &mut errs,
                    "schema stable key",
                    self.schema_allocation(canister.memory_namespace())
                        .stable_key(),
                );

                // Ensure the per-store memories are distinct.
                if self.data_memory_id() == self.index_memory_id() {
                    err!(
                        errs,
                        "data_memory_id and index_memory_id must differ (both are {})",
                        self.data_memory_id(),
                    );
                }
                if self.data_memory_id() == self.schema_memory_id() {
                    err!(
                        errs,
                        "data_memory_id and schema_memory_id must differ (both are {})",
                        self.data_memory_id(),
                    );
                }
                if self.index_memory_id() == self.schema_memory_id() {
                    err!(
                        errs,
                        "index_memory_id and schema_memory_id must differ (both are {})",
                        self.index_memory_id(),
                    );
                }
            }
            Err(e) => errs.add(e),
        }

        errs.result()
    }
}

impl VisitableNode for Store {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_stable_keys_use_durable_icydb_shape() {
        let store = Store::new(
            Def::new("demo::rpg", "CharacterStore"),
            "CHARACTER_STORE",
            "characters",
            "demo::rpg::Canister",
            110,
            111,
            112,
        );

        assert_eq!(
            store.data_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.data.v1",
        );
        assert_eq!(
            store.index_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.index.v1",
        );
        assert_eq!(
            store.schema_allocation("demo_rpg").stable_key(),
            "icydb.demo_rpg.characters.schema.v1",
        );
    }
}
