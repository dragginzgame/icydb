use crate::{
    imp::*,
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

impl ValidateNode for Newtype {
    fn validate(&self) -> Result<(), DarlingError> {
        self.traits.with_type_traits().validate()?;
        self.item.validate()?;

        match (self.primitive, self.item.primitive) {
            (Some(a), Some(b)) if a != b => Err(DarlingError::custom(format!(
                "invalid #[newtype] config: conflicting primitive ({a:?}) and item({b:?})"
            ))),
            (None, Some(_)) => Err(DarlingError::custom(
                "invalid #[newtype] config: item has a primitive but outer 'primitive' is not set",
            )),
            _ => Ok(()),
        }
    }
}

impl HasSchema for Newtype {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Newtype
    }
}

impl HasSchemaPart for Newtype {
    fn schema_part(&self) -> TokenStream {
        debug_assert!(self.validate().is_ok(), "invalid #[newtype] config");

        let def = self.def.schema_part();
        let item = self.item.schema_part();
        let default = quote_option(self.default.as_ref(), Arg::schema_part);
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::Newtype {
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

        // all newtypes
        traits.extend(vec![
            TraitKind::FieldValue,
            TraitKind::Inherent,
            TraitKind::Inner,
        ]);

        // primitive traits
        if let Some(primitive) = self.primitive {
            if primitive.supports_arithmetic() {
                traits.extend(vec![
                    TraitKind::Add,
                    TraitKind::AddAssign,
                    TraitKind::Div,
                    TraitKind::DivAssign,
                    TraitKind::Mul,
                    TraitKind::MulAssign,
                    TraitKind::Sub,
                    TraitKind::SubAssign,
                    TraitKind::Sum,
                ]);
            }
            if primitive.supports_remainder() {
                traits.add(TraitKind::Rem);
            }
            if primitive.supports_copy() {
                traits.add(TraitKind::Copy);
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
        match t {
            TraitKind::AsView => AsViewTrait::strategy(self),
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::FieldValue => FieldValueTrait::strategy(self),
            TraitKind::From => FromTrait::strategy(self),
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::Inner => Some(TraitStrategy::from_derive(TraitKind::Inner)),
            TraitKind::NumCast => NumCastTrait::strategy(self),
            TraitKind::NumToPrimitive => NumToPrimitiveTrait::strategy(self),
            TraitKind::NumFromPrimitive => NumFromPrimitiveTrait::strategy(self),
            TraitKind::PartialEq => PartialEqTrait::strategy(self).map(|s| s.with_derive(t)),
            TraitKind::PartialOrd => PartialOrdTrait::strategy(self).map(|s| s.with_derive(t)),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::MergePatch => MergePatchTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::Newtype;
    use crate::prelude::*;

    const ALL_PRIMITIVES: [Primitive; 28] = [
        Primitive::Account,
        Primitive::Blob,
        Primitive::Bool,
        Primitive::Date,
        Primitive::Decimal,
        Primitive::Duration,
        Primitive::E8s,
        Primitive::E18s,
        Primitive::Float32,
        Primitive::Float64,
        Primitive::Int,
        Primitive::Int8,
        Primitive::Int16,
        Primitive::Int32,
        Primitive::Int64,
        Primitive::Int128,
        Primitive::Nat,
        Primitive::Nat8,
        Primitive::Nat16,
        Primitive::Nat32,
        Primitive::Nat64,
        Primitive::Nat128,
        Primitive::Principal,
        Primitive::Subaccount,
        Primitive::Text,
        Primitive::Timestamp,
        Primitive::Ulid,
        Primitive::Unit,
    ];

    const ARITHMETIC_TRAITS: [TraitKind; 9] = [
        TraitKind::Add,
        TraitKind::AddAssign,
        TraitKind::Div,
        TraitKind::DivAssign,
        TraitKind::Mul,
        TraitKind::MulAssign,
        TraitKind::Sub,
        TraitKind::SubAssign,
        TraitKind::Sum,
    ];

    fn newtype_with_primitive(primitive: Primitive) -> Newtype {
        Newtype {
            def: Def::default(),
            primitive: Some(primitive),
            item: Item {
                primitive: Some(primitive),
                ..Default::default()
            },
            default: None,
            ty: Type::default(),
            traits: TraitBuilder::default(),
        }
    }

    fn has_all_arithmetic_traits(traits: &[TraitKind]) -> bool {
        ARITHMETIC_TRAITS
            .iter()
            .all(|trait_kind| traits.contains(trait_kind))
    }

    #[test]
    fn arithmetic_traits_match_supports_arithmetic() {
        for primitive in ALL_PRIMITIVES {
            let newtype = newtype_with_primitive(primitive);
            let traits = newtype.traits();
            let has_arithmetic = has_all_arithmetic_traits(&traits);

            assert_eq!(has_arithmetic, primitive.supports_arithmetic());
        }
    }
}
