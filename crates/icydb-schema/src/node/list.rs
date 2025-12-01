use crate::prelude::*;

///
/// List
///

#[derive(Clone, Debug, Serialize)]
pub struct List {
    pub def: Def,
    pub item: Item,
    pub ty: Type,
}

impl MacroNode for List {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for List {
    fn ty(&self) -> &Type {
        &self.ty
    }
}

impl ValidateNode for List {}

impl VisitableNode for List {
    fn route_key(&self) -> String {
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
        self.item.accept(v);
        self.ty.accept(v);
    }
}
