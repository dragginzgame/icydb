use crate::prelude::*;
use canic_utils::case::{Case, Casing};

///
/// Store
///
/// Schema node describing a stable IC BTreeMap that stores entity data.
///

#[derive(Clone, Debug, Serialize)]
pub struct Store {
    pub def: Def,
    pub ident: &'static str,
    pub ty: StoreType,
    pub canister: &'static str,
    pub memory_id: u8,
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

        // ident
        if !self.ident.is_case(Case::UpperSnake) {
            err!(errs, "ident '{}' must be UPPER_SNAKE_CASE", self.ident);
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
