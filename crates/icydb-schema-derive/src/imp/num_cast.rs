use crate::prelude::*;

///
/// NumCastTrait
///

pub struct NumCastTrait {}

///
/// Newtype
///

impl Imp<Newtype> for NumCastTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let num_fn = match primitive.num_cast_fn() {
            Ok(num_fn) => num_fn,
            Err(err) => return Some(TraitStrategy::from_impl(err.write_errors())),
        };
        let to_method = format_ident!("to_{}", num_fn);
        let from_method = format_ident!("from_{}", num_fn);

        Some(single_trait_strategy(
            node.def(),
            TraitKind::NumCast,
            quote! {
                fn from<T: ::icydb::traits::NumToPrimitive>(n: T) -> Option<Self> {
                    let num = n.#to_method()?;
                    <Self as ::icydb::traits::NumFromPrimitive>::#from_method(num)
                }
            },
        ))
    }
}

///
/// NumFromPrimitiveTrait
///

pub struct NumFromPrimitiveTrait {}

///
/// Newtype
///

impl Imp<Newtype> for NumFromPrimitiveTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let item = &node.item.type_expr();

        let mut q = quote! {
            fn from_i64(n: i64) -> Option<Self> {
                #item::from_i64(n).map(Self)
            }

            fn from_u64(n: u64) -> Option<Self> {
                #item::from_u64(n).map(Self)
            }
        };

        // floats
        if primitive.is_float() {
            q.extend(quote! {
                fn from_f64(n: f64) -> Option<Self> {
                    #item::from_f64(n).map(Self)
                }
            });
        }

        Some(single_trait_strategy(
            node.def(),
            TraitKind::NumFromPrimitive,
            q,
        ))
    }
}

///
/// NumToPrimitiveTrait
///

pub struct NumToPrimitiveTrait {}

///
/// Newtype
///

impl Imp<Newtype> for NumToPrimitiveTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        Some(single_trait_strategy(
            node.def(),
            TraitKind::NumToPrimitive,
            quote! {
                fn to_i64(&self) -> Option<i64> {
                    ::icydb::__reexports::num_traits::NumCast::from(self.0)
                }

                fn to_u64(&self) -> Option<u64> {
                    ::icydb::__reexports::num_traits::NumCast::from(self.0)
                }
            },
        ))
    }
}

fn single_trait_strategy(def: &Def, kind: TraitKind, tokens: TokenStream) -> TraitStrategy {
    let tokens = Implementor::new(def, kind)
        .set_tokens(tokens)
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}
