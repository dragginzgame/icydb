//! Module: node::newtype
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

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
    pub(crate) def: Def,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    pub(crate) item: Item,

    #[darling(default)]
    pub(crate) default: Option<Arg>,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

const PRIMITIVE_NEWTYPE_TRAITS: [TraitKind; 18] = [
    TraitKind::Add,
    TraitKind::AddAssign,
    TraitKind::Div,
    TraitKind::DivAssign,
    TraitKind::Mul,
    TraitKind::MulAssign,
    TraitKind::Neg,
    TraitKind::Product,
    TraitKind::Rem,
    TraitKind::RemAssign,
    TraitKind::Sub,
    TraitKind::SubAssign,
    TraitKind::Sum,
    TraitKind::Copy,
    TraitKind::Hash,
    TraitKind::NumericValue,
    TraitKind::Ord,
    TraitKind::PartialOrd,
];

impl HasDef for Newtype {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Newtype {
    fn validate(&self) -> Result<(), DarlingError> {
        self.validate_traits()?;
        self.item.validate()?;

        if let (Some(Primitive::Blob), Some(default @ Arg::String(value))) =
            (self.item.primitive, self.default.as_ref())
        {
            schema_default_rust_expr(default, self.item.primitive, self.def.paths())
                .map_err(|error| DarlingError::custom(error).with_span(value))?;
        }

        if self.traits.explicitly_adds(TraitKind::Default) && self.default.is_none() {
            return Err(DarlingError::custom(format!(
                "Default was requested for newtype {}, but no `default = ...` constructor is configured",
                self.def.ident()
            ))
            .with_span(&self.def.ident()));
        }

        Ok(())
    }
}

impl HasSchema for Newtype {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Newtype
    }
}

impl HasSchemaPart for Newtype {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        debug_assert!(self.validate().is_ok(), "invalid #[newtype] config");

        let def = self.def.schema_part(paths);
        let name = self.current_name_literal(self.name.as_ref());
        let item = self.item.schema_part(paths);
        let default = quote_option(self.default.as_ref(), |value| {
            Arg::schema_part(value, paths)
        });
        let ty = self.ty.schema_part(paths);

        // quote
        quote! {
            #model::node::Newtype::new(#def, #name, #item, #default, #ty)
        }
    }
}

impl HasTraits for Newtype {
    fn application_type_kind(&self) -> Option<ApplicationTypeKind> {
        Some(ApplicationTypeKind::Newtype)
    }

    fn trait_builder(&self) -> Option<&TraitBuilder> {
        Some(&self.traits)
    }

    fn trait_baseline(&self) -> TraitSet {
        let mut traits = application_type_trait_set();

        // all newtypes
        traits.add(TraitKind::From);
        traits.add(TraitKind::Inner);

        // Rust wrapper capabilities are independent of database scalar
        // arithmetic, ordering, and index eligibility.
        if let Some(primitive) = self.item.primitive {
            traits.extend(
                PRIMITIVE_NEWTYPE_TRAITS.into_iter().filter(|trait_kind| {
                    primitive_supports_generated_trait(primitive, *trait_kind)
                }),
            );
        }

        traits
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::From => FromTrait::strategy(self),
            TraitKind::Inner => Some(TraitStrategy::from_derive(TraitKind::Inner)),
            TraitKind::NumericValue => NumericValueTrait::strategy(self),
            TraitKind::PartialEq => PartialEqTrait::strategy(self).map(|s| s.with_derive(t)),
            TraitKind::PartialOrd => PartialOrdTrait::strategy(self).map(|s| s.with_derive(t)),
            TraitKind::NormalizeAuto => NormalizeAutoTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

const fn primitive_supports_generated_trait(primitive: Primitive, trait_kind: TraitKind) -> bool {
    match trait_kind {
        TraitKind::Add | TraitKind::AddAssign | TraitKind::Sub | TraitKind::SubAssign => {
            primitive_supports_full_arithmetic(primitive)
                || matches!(primitive, Primitive::Duration)
        }
        TraitKind::Div
        | TraitKind::DivAssign
        | TraitKind::Mul
        | TraitKind::MulAssign
        | TraitKind::Product
        | TraitKind::Sum => primitive_supports_full_arithmetic(primitive),
        TraitKind::Neg => primitive_supports_neg(primitive),
        TraitKind::Rem | TraitKind::RemAssign => matches!(
            primitive,
            Primitive::Decimal
                | Primitive::Int8
                | Primitive::Int16
                | Primitive::Int32
                | Primitive::Int64
                | Primitive::Int128
                | Primitive::Nat8
                | Primitive::Nat16
                | Primitive::Nat32
                | Primitive::Nat64
                | Primitive::Nat128
        ),
        TraitKind::Copy => primitive.supports_copy(),
        TraitKind::Hash | TraitKind::Ord | TraitKind::PartialOrd => true,
        TraitKind::NumericValue => primitive.supports_numeric_value(),
        TraitKind::Clone
        | TraitKind::Debug
        | TraitKind::Default
        | TraitKind::Deserialize
        | TraitKind::Serialize
        | TraitKind::Deref
        | TraitKind::DerefMut
        | TraitKind::Display
        | TraitKind::Eq
        | TraitKind::From
        | TraitKind::FromIterator
        | TraitKind::Inner
        | TraitKind::IntoIterator
        | TraitKind::NormalizeAuto
        | TraitKind::NormalizeCustom
        | TraitKind::PartialEq
        | TraitKind::Path
        | TraitKind::ValidateAuto
        | TraitKind::ValidateCustom
        | TraitKind::Visitable => false,
    }
}

const fn primitive_supports_neg(primitive: Primitive) -> bool {
    matches!(
        primitive,
        Primitive::Decimal
            | Primitive::Int8
            | Primitive::Int16
            | Primitive::Int32
            | Primitive::Int64
            | Primitive::Int128
    )
}

const fn primitive_supports_full_arithmetic(primitive: Primitive) -> bool {
    matches!(
        primitive,
        Primitive::Decimal
            | Primitive::Int8
            | Primitive::Int16
            | Primitive::Int32
            | Primitive::Int64
            | Primitive::Int128
            | Primitive::Nat8
            | Primitive::Nat16
            | Primitive::Nat32
            | Primitive::Nat64
            | Primitive::Nat128
    )
}

impl HasType for Newtype {
    fn type_part(&self, paths: &CratePaths) -> TokenStream {
        let ident = self.def.ident();
        let item = &self.item.type_expr(paths);

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub #item);
        }
    }
}

///
/// TESTS
///

impl Newtype {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        let base = self.all_tokens(paths);
        let typed_adapter = crate::node::typed_adapter::newtype_adapter_tokens(self, paths);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
        tokens
    }
}

#[cfg(test)]
mod tests {
    use super::{Newtype, primitive_supports_full_arithmetic, primitive_supports_neg};
    use crate::prelude::*;
    use darling::{FromMeta, ast::NestedMeta};
    use quote::quote;

    const ALL_PRIMITIVES: [Primitive; 25] = [
        Primitive::Account,
        Primitive::Blob,
        Primitive::Bool,
        Primitive::Date,
        Primitive::Decimal,
        Primitive::Duration,
        Primitive::Float32,
        Primitive::Float64,
        Primitive::Int8,
        Primitive::Int16,
        Primitive::Int32,
        Primitive::Int64,
        Primitive::Int128,
        Primitive::Nat8,
        Primitive::Nat16,
        Primitive::Nat32,
        Primitive::Nat64,
        Primitive::Nat128,
        Primitive::Principal,
        Primitive::Subaccount,
        Primitive::Text,
        Primitive::Timestamp,
        Primitive::U256,
        Primitive::Ulid,
        Primitive::Unit,
    ];

    const ADDITIVE_TRAITS: [TraitKind; 4] = [
        TraitKind::Add,
        TraitKind::AddAssign,
        TraitKind::Sub,
        TraitKind::SubAssign,
    ];

    const MULTIPLICATIVE_AND_FOLD_TRAITS: [TraitKind; 6] = [
        TraitKind::Div,
        TraitKind::DivAssign,
        TraitKind::Mul,
        TraitKind::MulAssign,
        TraitKind::Product,
        TraitKind::Sum,
    ];

    #[test]
    fn blob_newtype_defaults_reject_nonhex_before_generation() {
        for (text, valid) in [
            ("", true),
            ("DeAdBeEf", true),
            ("+1", false),
            ("raw text", false),
        ] {
            let mut node = newtype_with_primitive(Primitive::Blob);
            node.def = Def::new(
                syn::parse_quote!(
                    pub struct TestBlob;
                ),
                CratePaths::test(),
            );
            node.item.unbounded = true;
            node.default = Some(Arg::String(syn::LitStr::new(
                text,
                proc_macro2::Span::call_site(),
            )));
            assert_eq!(node.validate().is_ok(), valid);
        }
    }

    #[test]
    fn from_list_parses_nested_item_primitive() {
        let args = NestedMeta::parse_meta_list(quote!(item(prim = "Decimal")))
            .expect("newtype args should parse");

        let node = Newtype::from_list(&args).expect("newtype meta should lower");

        assert_eq!(node.item.primitive, Some(Primitive::Decimal));
    }

    fn newtype_with_primitive(primitive: Primitive) -> Newtype {
        Newtype {
            def: Def::default(),
            name: None,
            item: Item {
                primitive: Some(primitive),
                ..Default::default()
            },
            default: None,
            ty: Type::default(),
            traits: TraitBuilder::default(),
        }
    }

    #[test]
    fn primitive_newtypes_use_rust_hash_and_order_capabilities() {
        for primitive in ALL_PRIMITIVES {
            let newtype = newtype_with_primitive(primitive);
            let traits = newtype.traits();

            for trait_kind in [TraitKind::Hash, TraitKind::Ord, TraitKind::PartialOrd] {
                assert!(
                    traits.contains(&trait_kind),
                    "{primitive:?} should generate {trait_kind:?}"
                );
            }
        }
    }

    #[test]
    fn duration_newtype_generates_only_its_supported_additive_operators() {
        let traits = newtype_with_primitive(Primitive::Duration).traits();

        for trait_kind in ADDITIVE_TRAITS {
            assert!(traits.contains(&trait_kind));
        }
        for trait_kind in MULTIPLICATIVE_AND_FOLD_TRAITS {
            assert!(!traits.contains(&trait_kind));
        }
        assert!(!traits.contains(&TraitKind::Rem));
        assert!(!traits.contains(&TraitKind::RemAssign));
        assert!(!traits.contains(&TraitKind::Neg));
    }

    #[test]
    fn remainder_assignment_matches_remainder_capabilities() {
        for primitive in ALL_PRIMITIVES {
            let traits = newtype_with_primitive(primitive).traits();
            assert_eq!(
                traits.contains(&TraitKind::Rem),
                traits.contains(&TraitKind::RemAssign),
                "{primitive:?} remainder capability should be internally consistent"
            );
        }
    }

    #[test]
    fn product_matches_full_arithmetic_capabilities() {
        for primitive in ALL_PRIMITIVES {
            let traits = newtype_with_primitive(primitive).traits();
            assert_eq!(
                primitive_supports_full_arithmetic(primitive),
                traits.contains(&TraitKind::Product),
                "{primitive:?} product capability should match its arithmetic domain"
            );
        }
    }

    #[test]
    fn neg_is_generated_only_for_signed_exact_numeric_primitives() {
        for primitive in ALL_PRIMITIVES {
            let traits = newtype_with_primitive(primitive).traits();
            assert_eq!(
                primitive_supports_neg(primitive),
                traits.contains(&TraitKind::Neg),
                "{primitive:?} signed-negation capability should be exact"
            );
        }
    }
}
