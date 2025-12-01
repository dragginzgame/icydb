use crate::prelude::*;

///
/// Newtype
///

#[derive(Clone, Debug, Serialize)]
pub struct Newtype {
    pub def: Def,
    pub item: Item,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Arg>,

    pub ty: Type,
}

impl MacroNode for Newtype {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for Newtype {
    fn ty(&self) -> &Type {
        &self.ty
    }
}

impl ValidateNode for Newtype {}

impl VisitableNode for Newtype {
    fn route_key(&self) -> String {
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
        self.item.accept(v);
        if let Some(node) = &self.default {
            node.accept(v);
        }
        self.ty.accept(v);
    }
}
