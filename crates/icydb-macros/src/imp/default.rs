use crate::prelude::*;

///
/// DefaultTrait
///

pub struct DefaultTrait {}

///
/// Entity
///

impl Imp<Entity> for DefaultTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(default_strategy(&node.def, &node.fields))
    }
}

///
/// Enum
///

impl Imp<Enum> for DefaultTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let default_variant = node.default_variant().expect("default is required");
        let variant_ident = default_variant.effective_ident();

        // if the default variant carries a value, generate it as `(Default::default())`
        let inner = if default_variant.value.is_some() {
            quote!(Self::#variant_ident(Default::default()))
        } else {
            quote!(Self::#variant_ident)
        };

        let q = quote! {
            fn default() -> Self {
                #inner
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Default)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for DefaultTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(default_strategy(&node.def, &node.fields))
    }
}

// default_strategy
fn default_strategy(def: &Def, fields: &FieldList) -> TraitStrategy {
    if fields.iter().all(|f| f.default.is_none()) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    // assignments
    let assignments = fields.into_iter().map(|f| {
        let ident = &f.ident;
        let expr = f.default_expr();

        quote!(#ident: #expr)
    });

    // build default
    let q = quote! {
        fn default() -> Self {
            Self { #(#assignments),* }
        }
    };

    let tokens = Implementor::new(def, TraitKind::Default)
        .set_tokens(q)
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}

///
/// Newtype
///

impl Imp<Newtype> for DefaultTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        // If no default we just want to derive
        let Some(default_expr) = &node.default else {
            return Some(TraitStrategy::from_derive(TraitKind::Default));
        };

        let q = quote! {
            fn default() -> Self {
                Self(#default_expr.into())
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Default)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
