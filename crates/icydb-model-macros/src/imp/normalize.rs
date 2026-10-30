//! Module: imp::normalize
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

use crate::{
    imp::field_walk::{cardinality_wrapper, field_walk_bindings},
    prelude::*,
};

/// ---------------------------------------------------------------------------
/// NormalizeAuto
/// ---------------------------------------------------------------------------

pub struct NormalizeAutoTrait;

/// Each node type can emit normalizer code for its *own value only*.
/// Traversal into children is handled by the visitor.

pub trait NormalizeAutoFn {
    fn self_tokens(_: &Self, __paths: &CratePaths) -> TokenStream {
        quote!()
    }
}

macro_rules! impl_normalize_auto {
    ($($ty:ty),* $(,)?) => {
        $(impl Imp<$ty> for NormalizeAutoTrait {
            fn strategy(node: &$ty) -> Option<TraitStrategy> {
        let paths = node.def().paths();
                let self_tokens = NormalizeAutoFn::self_tokens(node, paths);

                let tokens = Implementor::new(node.def(), TraitKind::NormalizeAuto)
                    .add_tokens(self_tokens)
                    .impl_tokens(paths);

                Some(TraitStrategy::from_impl(tokens))
            }
        })*
    };
}

impl_normalize_auto!(Enum, List, Map, Newtype, Set);

/// ---------------------------------------------------------------------------
/// Entity / Record
/// ---------------------------------------------------------------------------
/// Apply field-item normalizers using the declared cardinality.
/// Do NOT recurse.
impl Imp<Entity> for NormalizeAutoTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        Some(field_list_normalize_strategy(
            node.def(),
            &node.fields,
            paths,
        ))
    }
}

impl Imp<Record> for NormalizeAutoTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        Some(field_list_normalize_strategy(
            node.def(),
            &node.fields,
            paths,
        ))
    }
}

/// ---------------------------------------------------------------------------
/// Enum
/// ---------------------------------------------------------------------------
/// No direct normalization for enum selection.
/// Payload normalization occurs when payload node is visited.
impl NormalizeAutoFn for Enum {}

/// ---------------------------------------------------------------------------
/// Newtype
/// ---------------------------------------------------------------------------
/// Apply normalizers attached to the newtype itself / its inner value.
impl NormalizeAutoFn for Newtype {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        fn_wrap_normalize_self(newtype_normalizers(node, paths), paths)
    }
}

/// ---------------------------------------------------------------------------
/// List / Set / Map
/// ---------------------------------------------------------------------------
/// Container-declared item hooks run here; the visitor separately visits each
/// child's own normalization implementation. Sorted keys are never mutated.
impl NormalizeAutoFn for List {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let mut statements =
            generate_normalizers(&node.ty.normalizers, quote!(self.0), None, paths);
        let items = generate_normalizers(
            &node.item.normalizers,
            quote!(*v),
            Some(quote!(#model::visitor::PathSegment::Index(i))),
            paths,
        );
        if !items.is_empty() {
            statements.push(quote! {
                for (i, v) in self.0.iter_mut().enumerate() {
                    #(#items)*
                }
            });
        }
        fn_wrap_normalize_self(
            (!statements.is_empty()).then(|| quote! { #(#statements)* }),
            paths,
        )
    }
}

impl NormalizeAutoFn for Set {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        container_self_tokens(&node.ty.normalizers, paths)
    }
}

impl NormalizeAutoFn for Map {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let mut statements =
            generate_normalizers(&node.ty.normalizers, quote!(self.0), None, paths);
        let values = generate_normalizers(
            &node.value.item.normalizers,
            quote!(*v),
            Some(quote!(#model::visitor::PathSegment::Index(i))),
            paths,
        );
        if let Some(values) = cardinality_wrapper(node.value.cardinality(), values, quote!(v)) {
            statements.push(quote! {
                for (i, v) in self.0.values_mut().enumerate() {
                    #values
                }
            });
        }
        fn_wrap_normalize_self(
            (!statements.is_empty()).then(|| quote! { #(#statements)* }),
            paths,
        )
    }
}

/// ---------------------------------------------------------------------------
/// Helpers
/// ---------------------------------------------------------------------------

/// Emit normalizer calls.
/// Errors are recorded via VisitorContext.
fn generate_normalizers(
    normalizers: &[TypeNormalizer],
    target: TokenStream,
    seg: Option<TokenStream>,
    paths: &CratePaths,
) -> Vec<TokenStream> {
    let model = &paths.model;
    normalizers
        .iter()
        .map(|normalizer| match &seg {
            None => normalizer_call(normalizer, &target, quote!(ctx), paths),
            Some(seg) => {
                let call = normalizer_call(normalizer, &target, quote!(&mut __field_ctx), paths);
                quote! {{
                    let mut __field_ctx = #model::visitor::ScopedContext::new(ctx, #seg);
                    #call
                }}
            }
        })
        .collect()
}

// Context selection does not change callback identity, invocation or issues.
fn normalizer_call(
    normalizer: &TypeNormalizer,
    target: &TokenStream,
    context: TokenStream,
    paths: &CratePaths,
) -> TokenStream {
    let model = &paths.model;
    let ctor = normalizer.quote_constructor();
    let callback_type = &normalizer.path;
    quote! {{
        let mut __callback_ctx = #model::visitor::CallbackContext::new(
            #context,
            #model::visitor::CallbackIdentity::new(
                #model::visitor::CallbackKind::Normalizer,
                ::core::any::type_name::<#callback_type>(),
            ),
        );
        if let ::core::result::Result::Err(msg) = #model::visitor::Normalizer::normalize_with_context(
            &(#ctor), &mut #target, &mut __callback_ctx,
        ) {
            #model::visitor::VisitorContext::add_issue(
                &mut __callback_ctx, #model::visitor::Issue::from(msg),
            );
        }
    }}
}

/// Normalizers attached to the container itself (not items).
fn container_normalizers(
    normalizers: &[TypeNormalizer],
    target: TokenStream,
    paths: &CratePaths,
) -> Option<TokenStream> {
    let stmts = generate_normalizers(normalizers, target, None, paths);
    if stmts.is_empty() {
        None
    } else {
        Some(quote! { #(#stmts)* })
    }
}

/// Emit container normalization without mutable item callbacks, as required by sets.
fn container_self_tokens(normalizers: &[TypeNormalizer], paths: &CratePaths) -> TokenStream {
    fn_wrap_normalize_self(
        container_normalizers(normalizers, quote!(self.0), paths),
        paths,
    )
}

/// Field-level normalizers for Entity / Record.
/// Applies item callbacks through each field's declared cardinality.
fn field_list(def: &Def, fields: &FieldList, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    let bindings = field_walk_bindings(fields);
    let field_table_ident = format_ident!("__NORMALIZE_FIELDS");

    let normalize_helpers = fields
        .iter()
        .zip(bindings.iter())
        .filter_map(|(field, binding)| {
            let stmts = generate_normalizers(
                &field.value.item.normalizers,
                quote!(*v),
                Some(binding.path_segment(paths)),
                paths,
            );
            let stmts = cardinality_wrapper(
                field.value.cardinality(),
                stmts,
                binding.member_mut_from(quote!(node)),
            )?;
            let fn_ident = binding.normalize_fn_ident();

            Some(quote! {
                fn #fn_ident(
                    node: &mut Self,
                    ctx: &mut dyn #model::visitor::VisitorContext,
                ) {
                    #stmts
                }
            })
        });

    let descriptors = bindings
        .iter()
        .zip(fields.iter())
        .filter_map(|(binding, field)| {
            if field.value.item.normalizers.is_empty() {
                None
            } else {
                let normalize_fn = binding.normalize_fn_ident();

                Some(quote! {
                    #model::visitor::NormalizeFieldDescriptor::new(Self::#normalize_fn)
                })
            }
        });

    let inherent_tokens = Implementor::inherent(def)
        .set_tokens(quote! {
            #(#normalize_helpers)*

            const #field_table_ident: &'static [#model::visitor::NormalizeFieldDescriptor<Self>] =
                &[#(#descriptors),*];
        })
        .impl_tokens(paths);

    let trait_tokens = Implementor::new(def, TraitKind::NormalizeAuto)
        .add_tokens(fn_wrap_normalize_self(
            Some(quote! {
                #model::visitor::drive_normalize_fields(self, ctx, Self::#field_table_ident);
            }),
            paths,
        ))
        .impl_tokens(paths);

    quote! {
        #inherent_tokens
        #trait_tokens
    }
}

/// Entity and record normalize generation share the same field-driven strategy.
fn field_list_normalize_strategy(
    def: &Def,
    fields: &FieldList,
    paths: &CratePaths,
) -> TraitStrategy {
    TraitStrategy::from_impl(field_list(def, fields, paths))
}

/// Normalizers for a newtype’s inner value (`self.0`).
fn newtype_normalizers(node: &Newtype, paths: &CratePaths) -> Option<TokenStream> {
    let target = quote!(self.0);

    let mut stmts = Vec::new();
    stmts.extend(generate_normalizers(
        &node.ty.normalizers,
        target.clone(),
        None,
        paths,
    ));
    stmts.extend(generate_normalizers(
        &node.item.normalizers,
        target,
        None,
        paths,
    ));

    if stmts.is_empty() {
        None
    } else {
        Some(quote! { #(#stmts)* })
    }
}

/// Emit `fn normalize_self(&mut self, ctx: &mut dyn VisitorContext)`
/// only if there is something to do.
fn fn_wrap_normalize_self(inner: Option<TokenStream>, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    match inner {
        None => quote!(),
        Some(inner) => quote! {
            fn normalize_self(
                &mut self,
                ctx: &mut dyn #model::visitor::VisitorContext
            ) {
                #inner
            }
        },
    }
}
