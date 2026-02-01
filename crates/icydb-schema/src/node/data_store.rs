use crate::prelude::*;

///
/// DataStore
///
/// Schema node describing a stable IC BTreeMap that stores entity data.
/// This is the authoritative row store for an entity.
///

#[derive(Clone, Debug, Serialize)]
pub struct DataStore {
    pub def: Def,
    pub ident: &'static str,
    pub canister: &'static str,
    pub memory_id: u8,
}

impl MacroNode for DataStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for DataStore {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // canister
        match schema.cast_node::<Canister>(self.canister) {
            Ok(canister) => {
                if self.memory_id < canister.memory_min || self.memory_id > canister.memory_max {
                    err!(
                        errs,
                        "memory_id {} outside of range {}-{}",
                        self.memory_id,
                        canister.memory_min,
                        canister.memory_max,
                    );
                }
            }
            Err(e) => errs.add(e),
        }

        errs.result()
    }
}

impl VisitableNode for DataStore {
    fn route_key(&self) -> String {
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
    }
}
