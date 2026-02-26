use crate::node::{validate_memory_id_in_range, validate_memory_id_not_reserved};
use crate::prelude::*;

///
/// Store
///
/// Schema node describing a stable IC BTreeMap pair that stores:
/// - primary entity data
/// - all index data for that entity
///

#[derive(Clone, Debug, Serialize)]
pub struct Store {
    pub def: Def,
    pub ident: &'static str,
    pub canister: &'static str,
    pub data_memory_id: u8,
    pub index_memory_id: u8,
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

        match schema.cast_node::<Canister>(self.canister) {
            Ok(canister) => {
                // Validate data memory ID
                validate_memory_id_in_range(
                    &mut errs,
                    "data_memory_id",
                    self.data_memory_id,
                    canister.memory_min,
                    canister.memory_max,
                );
                validate_memory_id_not_reserved(&mut errs, "data_memory_id", self.data_memory_id);

                // Validate index memory ID
                validate_memory_id_in_range(
                    &mut errs,
                    "index_memory_id",
                    self.index_memory_id,
                    canister.memory_min,
                    canister.memory_max,
                );
                validate_memory_id_not_reserved(&mut errs, "index_memory_id", self.index_memory_id);

                // Ensure they are not the same
                if self.data_memory_id == self.index_memory_id {
                    err!(
                        errs,
                        "data_memory_id and index_memory_id must differ (both are {})",
                        self.data_memory_id,
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
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
    }
}
