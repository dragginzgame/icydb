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
                if self.data_memory_id < canister.memory_min
                    || self.data_memory_id > canister.memory_max
                {
                    err!(
                        errs,
                        "data_memory_id {} outside of range {}-{}",
                        self.data_memory_id,
                        canister.memory_min,
                        canister.memory_max,
                    );
                }

                // Validate index memory ID
                if self.index_memory_id < canister.memory_min
                    || self.index_memory_id > canister.memory_max
                {
                    err!(
                        errs,
                        "index_memory_id {} outside of range {}-{}",
                        self.index_memory_id,
                        canister.memory_min,
                        canister.memory_max,
                    );
                }

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
