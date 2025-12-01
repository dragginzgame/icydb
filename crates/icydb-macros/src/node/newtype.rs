use crate::{
    node::traits::{HasDef, HasSchema},
    prelude::*,
};

///
/// Newtype
///

#[derive(Debug, FromMeta)]
pub struct Newtype {
    #[darling(default, skip)]
    pub def: Def,

    pub primitive: Option<Primitive>,
    pub item: Item,

    #[darling(default)]
    pub default: Option<Arg>,

    #[darling(default)]
    pub ty: Type,

    #[darling(default)]
    pub traits: TraitBuilder,
}

impl HasDef for Newtype {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl HasSchema for Newtype {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Newtype
    }
}

impl HasSchemaPart for Newtype {
    fn schema_part(&self) -> TokenStream {
        // panic on invalid primitive/item combinations
        match (self.primitive, self.item.primitive) {
            (Some(a), Some(b)) if a != b => {
                panic!("invalid #[newtype] config: conflicting primitive ({a:?}) and item({b:?})");
            }
            (None, Some(_)) => {
                panic!(
                    "invalid #[newtype] config: item has a primitive but outer 'primitive' is not set"
                );
            }
            _ => {}
        }

        let def = self.def.schema_part();
        let item = self.item.schema_part();
        let default = quote_option(self.default.as_ref(), Arg::schema_part);
        let ty = self.ty.schema_part();

        // quote
        let sp = paths().schema;
        quote! {
            #sp::node::Newtype {
                def: #def,
                item: #item,
                default: #default,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for Newtype {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();

        traits.extend(vec![
            TraitKind::Deref,
            TraitKind::DerefMut,
            TraitKind::Inner,
        ]);

        // primitive traits
        if let Some(primitive) = self.primitive {
            if primitive.supports_arithmetic() {
                traits.extend(vec![
                    TraitKind::Add,
                    TraitKind::AddAssign,
                    TraitKind::Mul,
                    TraitKind::MulAssign,
                    TraitKind::Sub,
                    TraitKind::SubAssign,
                    TraitKind::Sum,
                ]);
            }
            if primitive.supports_copy() {
                traits.add(TraitKind::Copy);
            }
            if primitive.supports_display() {
                traits.add(TraitKind::Display);
            }
            if primitive.supports_hash() {
                traits.add(TraitKind::Hash);
            }
            if primitive.supports_num_cast() {
                traits.extend(vec![
                    TraitKind::NumCast,
                    TraitKind::NumFromPrimitive,
                    TraitKind::NumToPrimitive,
                ]);
            }
            if primitive.supports_ord() {
                traits.add(TraitKind::Ord);
                traits.add(TraitKind::PartialOrd);
            }
        }

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        use crate::imp::*;

        match t {
            TraitKind::PartialEq => PartialEqTrait::strategy(self).map(|s| s.with_derive(t)),
            TraitKind::PartialOrd => PartialOrdTrait::strategy(self).map(|s| s.with_derive(t)),

            TraitKind::Add => AddTrait::strategy(self),
            TraitKind::AddAssign => AddAssignTrait::strategy(self),
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::FieldValue => FieldValueTrait::strategy(self),
            TraitKind::Filterable => FilterableTrait::strategy(self),
            TraitKind::From => FromTrait::strategy(self),
            TraitKind::Inner => InnerTrait::strategy(self),
            TraitKind::NumCast => NumCastTrait::strategy(self),
            TraitKind::NumToPrimitive => NumToPrimitiveTrait::strategy(self),
            TraitKind::NumFromPrimitive => NumFromPrimitiveTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::Sub => SubTrait::strategy(self),
            TraitKind::SubAssign => SubAssignTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::View => ViewTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for Newtype {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let item = &self.item.type_expr();

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub #item);
        }
    }
}

impl ToTokens for Newtype {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
