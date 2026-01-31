use crate::prelude::*;
use canic_utils::case::{Case, Casing};

///
/// IndexStore
///

#[derive(Clone, Debug, Serialize)]
pub struct IndexStore {
    pub def: Def,
    pub ident: &'static str,
    pub canister: &'static str,
    pub entry_memory_id: u8,
    pub fingerprint_memory_id: u8,
}

impl MacroNode for IndexStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for IndexStore {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        match schema.cast_node::<Canister>(self.canister) {
            Ok(canister) => {
                let min = canister.memory_min;
                let max = canister.memory_max;

                if !(min..=max).contains(&self.entry_memory_id) {
                    err!(
                        errs,
                        "entry_memory_id {} outside of range {}-{}",
                        self.entry_memory_id,
                        min,
                        max
                    );
                }

                if !(min..=max).contains(&self.fingerprint_memory_id) {
                    err!(
                        errs,
                        "fingerprint_memory_id {} outside of range {}-{}",
                        self.fingerprint_memory_id,
                        min,
                        max
                    );
                }

                if self.entry_memory_id == self.fingerprint_memory_id {
                    err!(
                        errs,
                        "entry_memory_id and fingerprint_memory_id must be distinct (both = {})",
                        self.entry_memory_id
                    );
                }
            }
            Err(e) => errs.add(e),
        }

        if !self.ident.is_case(Case::UpperSnake) {
            err!(errs, "ident '{}' must be UPPER_SNAKE_CASE", self.ident);
        }

        errs.result()
    }
}

impl VisitableNode for IndexStore {
    fn route_key(&self) -> String {
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
    }
}
