use crate::prelude::*;

///
/// Map
///

#[derive(Clone, Debug, Serialize)]
pub struct Map {
    pub def: Def,
    pub key: Item,
    pub value: Value,
    pub ty: Type,
}

impl MacroNode for Map {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for Map {
    fn ty(&self) -> &Type {
        &self.ty
    }
}

impl ValidateNode for Map {}

impl VisitableNode for Map {
    fn route_key(&self) -> String {
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
        self.key.accept(v);
        self.value.accept(v);
        self.ty.accept(v);
    }
}
