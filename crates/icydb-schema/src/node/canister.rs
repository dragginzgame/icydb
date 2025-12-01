use crate::prelude::*;

///
/// Canister
///

#[derive(CandidType, Clone, Debug, Serialize)]
pub struct Canister {
    pub def: Def,
    pub memory_min: u8,
    pub memory_max: u8,
}

impl MacroNode for Canister {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Canister {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // Check for duplicate memory IDs among stores for this canister
        let canister_path = self.def.path();
        let mut seen_ids = std::collections::HashSet::new();
        for (_, store) in schema.filter_nodes::<Store>(|node| node.canister == canister_path) {
            let memory_id = store.memory_id;
            if !seen_ids.insert(memory_id) {
                err!(
                    errs,
                    "duplicate memory_id `{}` used in canister `{}`",
                    memory_id,
                    canister_path
                );
            }
        }

        // store
        if self.memory_min > self.memory_max {
            err!(errs, "memory_min must be equal to or less than memory_max");
        }

        errs.result()
    }
}

impl VisitableNode for Canister {
    fn route_key(&self) -> String {
        self.def.path()
    }
}
