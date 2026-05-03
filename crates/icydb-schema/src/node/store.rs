use crate::node::{validate_memory_id_in_range, validate_memory_id_not_reserved};
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
        canister: &'static str,
        data_memory_id: u8,
        index_memory_id: u8,
        schema_memory_id: u8,
    ) -> Self {
        Self {
            def,
            ident,
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
                // Validate data memory ID
                validate_memory_id_in_range(
                    &mut errs,
                    "data_memory_id",
                    self.data_memory_id(),
                    canister.memory_min(),
                    canister.memory_max(),
                );
                validate_memory_id_not_reserved(&mut errs, "data_memory_id", self.data_memory_id());

                // Validate index memory ID
                validate_memory_id_in_range(
                    &mut errs,
                    "index_memory_id",
                    self.index_memory_id(),
                    canister.memory_min(),
                    canister.memory_max(),
                );
                validate_memory_id_not_reserved(
                    &mut errs,
                    "index_memory_id",
                    self.index_memory_id(),
                );

                // Validate schema memory ID
                validate_memory_id_in_range(
                    &mut errs,
                    "schema_memory_id",
                    self.schema_memory_id(),
                    canister.memory_min(),
                    canister.memory_max(),
                );
                validate_memory_id_not_reserved(
                    &mut errs,
                    "schema_memory_id",
                    self.schema_memory_id(),
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
