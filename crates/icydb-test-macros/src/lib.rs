//! This crate provides test-only entity scaffolding and is NOT a replacement
//! for icydb-schema-derive.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{
    Expr, ExprArray, ExprLit, ExprPath, Fields, GenericArgument, Ident, ItemStruct, Lit, LitStr,
    Meta, Path, PathArguments, Token, Type, TypePath, parse::Parser, punctuated::Punctuated,
    spanned::Spanned,
};

/// Generate minimal `EntityKind` and `Path` impls for test entities.
#[proc_macro_attribute]
pub fn test_entity(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Phase 1: parse inputs.
    let input = syn::parse_macro_input!(item as ItemStruct);
    let struct_ident = &input.ident;
    let args = match TestEntityArgs::parse(attr) {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };

    // Phase 2: collect struct field metadata.
    let field_map = match FieldMap::from_struct(&input) {
        Ok(map) => map,
        Err(err) => return err.to_compile_error().into(),
    };

    // Phase 3: build field models and primary key metadata.
    let field_models = build_field_models(&args, &field_map, struct_ident);

    // Phase 4: emit constants and impls.
    let struct_name_upper = struct_ident.to_string().to_ascii_uppercase();
    let fields_ident = Ident::new(
        &format!("__ICYDB_TEST_FIELDS_{struct_name_upper}"),
        Span::call_site(),
    );
    let pk_field_ident = Ident::new(
        &format!("__ICYDB_TEST_PRIMARY_KEY_{struct_name_upper}"),
        Span::call_site(),
    );
    let model_ident = Ident::new(
        &format!("__ICYDB_TEST_MODEL_{struct_name_upper}"),
        Span::call_site(),
    );

    let crate_path = &args.crate_path;
    let entity_name = &args.entity_name;
    let entity_path = &args.path;
    let datastore = &args.datastore;
    let canister = &args.canister;
    let primary_key_ident = &args.primary_key;
    let primary_key_name = LitStr::new(&primary_key_ident.to_string(), primary_key_ident.span());
    let field_names = &args.fields;

    let field_model_entries = &field_models.models;
    let aux_consts = &field_models.aux_consts;
    let field_len = field_models.len;

    let pk_field_model =
        field_models.primary_key_model(crate_path, &pk_field_ident, &primary_key_name);
    let pk_field_ref = field_models.primary_key_ref(&fields_ident, &pk_field_ident);

    let expanded = quote! {
        #input

        #(#aux_consts)*

        const #fields_ident: [#crate_path::model::field::EntityFieldModel; #field_len] = [
            #(#field_model_entries),*
        ];

        #pk_field_model

        const #model_ident: #crate_path::model::entity::EntityModel = #crate_path::model::entity::EntityModel {
            path: #entity_path,
            entity_name: #entity_name,
            primary_key: #pk_field_ref,
            fields: &#fields_ident,
            indexes: &[],
        };

        impl #crate_path::traits::Path for #struct_ident {
            const PATH: &'static str = #entity_path;
        }

        impl #crate_path::traits::EntityKind for #struct_ident {
            type Id = #crate_path::types::Ref<Self>;
            type DataStore = #datastore;
            type Canister = #canister;

            const ENTITY_NAME: &'static str = #entity_name;
            const PRIMARY_KEY: &'static str = #primary_key_name;
            const FIELDS: &'static [&'static str] = &[#(#field_names),*];
            const INDEXES: &'static [&'static #crate_path::model::index::IndexModel] = &[];
            const MODEL: &'static #crate_path::model::entity::EntityModel = &#model_ident;

            fn id(&self) -> Self::Id {
                self.#primary_key_ident
            }

            fn set_id(&mut self, id: Self::Id) {
                self.#primary_key_ident = id;
            }
        }

        impl #struct_ident {
            /// Return the entity's primary key.
            #[must_use]
            pub fn key(&self) -> #crate_path::types::Ref<Self> {
                self.#primary_key_ident
            }

            /// Alias for [`Self::key`].
            #[must_use]
            pub fn primary_key(&self) -> #crate_path::types::Ref<Self> {
                self.#primary_key_ident
            }

            /// Set the entity's primary key.
            pub fn set_primary_key(&mut self, key: #crate_path::types::Ref<Self>) {
                self.#primary_key_ident = key;
            }
        }
    };

    expanded.into()
}

// Parsed and validated macro arguments for #[test_entity].
struct TestEntityArgs {
    crate_path: Path,
    entity_name: LitStr,
    path: LitStr,
    datastore: Path,
    canister: Path,
    primary_key: Ident,
    fields: Vec<LitStr>,
}

impl TestEntityArgs {
    // Parse attribute arguments into a strongly-typed struct.
    fn parse(attr: TokenStream) -> Result<Self, syn::Error> {
        // Phase 1: parse the raw meta list.
        let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
        let args = parser.parse(attr)?;

        // Phase 2: collect arguments.
        let mut crate_path = None;
        let mut entity_name = None;
        let mut path = None;
        let mut datastore = None;
        let mut canister = None;
        let mut primary_key = None;
        let mut fields = None;

        for meta in args {
            let Meta::NameValue(name_value) = meta else {
                return Err(syn::Error::new(
                    meta.span(),
                    "test_entity expects key = value arguments",
                ));
            };
            let ident = name_value
                .path
                .get_ident()
                .ok_or_else(|| syn::Error::new(name_value.path.span(), "invalid argument name"))?
                .to_string();

            match ident.as_str() {
                "crate" => {
                    crate_path = Some(parse_path(&name_value.value, "crate")?);
                }
                "entity_name" => {
                    entity_name = Some(parse_lit_str(&name_value.value, "entity_name")?);
                }
                "path" => {
                    path = Some(parse_lit_str(&name_value.value, "path")?);
                }
                "datastore" => {
                    datastore = Some(parse_path(&name_value.value, "datastore")?);
                }
                "canister" => {
                    canister = Some(parse_path(&name_value.value, "canister")?);
                }
                "primary_key" => {
                    primary_key = Some(parse_ident(&name_value.value, "primary_key")?);
                }
                "fields" => {
                    fields = Some(parse_fields(&name_value.value)?);
                }
                _ => {
                    return Err(syn::Error::new(
                        name_value.path.span(),
                        format!("unknown test_entity argument: {ident}"),
                    ));
                }
            }
        }

        // Phase 3: enforce required arguments.
        let crate_path = crate_path
            .unwrap_or_else(|| syn::parse_str("::icydb").expect("default crate path should parse"));
        let entity_name = require_arg(entity_name, "entity_name")?;
        let path = require_arg(path, "path")?;
        let datastore = require_arg(datastore, "datastore")?;
        let canister = require_arg(canister, "canister")?;
        let primary_key = require_arg(primary_key, "primary_key")?;
        let fields = require_arg(fields, "fields")?;

        Ok(Self {
            crate_path,
            entity_name,
            path,
            datastore,
            canister,
            primary_key,
            fields,
        })
    }
}

// Field lookup helper for struct definitions.
struct FieldMap<'a> {
    fields: std::collections::HashMap<String, &'a Type>,
}

impl<'a> FieldMap<'a> {
    // Collect named fields into a lookup map.
    fn from_struct(input: &'a ItemStruct) -> Result<Self, syn::Error> {
        let Fields::Named(fields) = &input.fields else {
            return Err(syn::Error::new(
                input.span(),
                "test_entity requires a struct with named fields",
            ));
        };

        let mut map = std::collections::HashMap::with_capacity(fields.named.len());
        for field in &fields.named {
            if let Some(ident) = &field.ident {
                map.insert(ident.to_string(), &field.ty);
            }
        }

        Ok(Self { fields: map })
    }

    // Infer the runtime kind for a field name, defaulting to Unsupported.
    fn primary_key_kind_for(&self, name: &str) -> KindToken {
        let Some(ty) = self.fields.get(name) else {
            return KindToken::Unsupported;
        };

        infer_primary_key_kind(ty)
    }
}

// Computed field-model metadata for code generation.
struct FieldModels {
    models: Vec<TokenStream2>,
    aux_consts: Vec<TokenStream2>,
    primary_key_kind: KindToken,
    primary_key_index: Option<usize>,
    len: usize,
}

impl FieldModels {
    // Build a primary key field definition if it is not in the field list.
    fn primary_key_model(
        &self,
        crate_path: &Path,
        pk_ident: &Ident,
        pk_name: &LitStr,
    ) -> TokenStream2 {
        if self.primary_key_index.is_some() {
            return TokenStream2::new();
        }

        let kind_tokens = self.primary_key_kind.tokens(crate_path);

        quote! {
            const #pk_ident: #crate_path::model::field::EntityFieldModel = #crate_path::model::field::EntityFieldModel {
                name: #pk_name,
                kind: #kind_tokens,
            };
        }
    }

    // Resolve the primary key reference for EntityModel.
    fn primary_key_ref(&self, fields_ident: &Ident, pk_ident: &Ident) -> TokenStream2 {
        if let Some(index) = self.primary_key_index {
            let index = syn::Index::from(index);

            return quote!(&#fields_ident[#index]);
        }

        quote!(&#pk_ident)
    }
}

// Parse and build the field models used in the generated EntityModel.
fn build_field_models(
    args: &TestEntityArgs,
    field_map: &FieldMap<'_>,
    struct_ident: &Ident,
) -> FieldModels {
    let mut models = Vec::with_capacity(args.fields.len());
    let mut aux_consts = Vec::new();
    let mut ref_key_kind_ident = None;
    let crate_path = &args.crate_path;

    // Phase 1: build field entries from the provided field list.
    let primary_key_name = args.primary_key.to_string();
    for (idx, field) in args.fields.iter().enumerate() {
        let name = field.value();
        let is_primary_key = name == primary_key_name;
        let kind_tokens = field_kind_tokens(
            field_map,
            &name,
            is_primary_key,
            &args.crate_path,
            struct_ident,
            idx,
            &mut aux_consts,
            &mut ref_key_kind_ident,
        );

        models.push(quote!(#crate_path::model::field::EntityFieldModel {
            name: #field,
            kind: #kind_tokens,
        }));
    }

    // Phase 2: resolve primary key metadata.
    let primary_key_index = args
        .fields
        .iter()
        .position(|field| field.value() == primary_key_name);
    let primary_key_kind = field_map.primary_key_kind_for(&primary_key_name);

    FieldModels {
        models,
        aux_consts,
        primary_key_kind,
        primary_key_index,
        len: args.fields.len(),
    }
}

// Token-level kind mapping used for minimal EntityFieldModel generation.
#[derive(Clone, Copy)]
enum KindToken {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
    E8s,
    E18s,
    Float32,
    Float64,
    Int,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Uint,
    Uint128,
    UintBig,
    Ulid,
    Unit,
    Unsupported,
}

impl KindToken {
    // Render the kind as an EntityFieldKind token path.
    fn tokens(self, crate_path: &Path) -> TokenStream2 {
        match self {
            Self::Account => quote!(#crate_path::model::field::EntityFieldKind::Account),
            Self::Blob => quote!(#crate_path::model::field::EntityFieldKind::Blob),
            Self::Bool => quote!(#crate_path::model::field::EntityFieldKind::Bool),
            Self::Date => quote!(#crate_path::model::field::EntityFieldKind::Date),
            Self::Decimal => quote!(#crate_path::model::field::EntityFieldKind::Decimal),
            Self::Duration => quote!(#crate_path::model::field::EntityFieldKind::Duration),
            Self::E8s => quote!(#crate_path::model::field::EntityFieldKind::E8s),
            Self::E18s => quote!(#crate_path::model::field::EntityFieldKind::E18s),
            Self::Float32 => quote!(#crate_path::model::field::EntityFieldKind::Float32),
            Self::Float64 => quote!(#crate_path::model::field::EntityFieldKind::Float64),
            Self::Int => quote!(#crate_path::model::field::EntityFieldKind::Int),
            Self::Int128 => quote!(#crate_path::model::field::EntityFieldKind::Int128),
            Self::IntBig => quote!(#crate_path::model::field::EntityFieldKind::IntBig),
            Self::Principal => quote!(#crate_path::model::field::EntityFieldKind::Principal),
            Self::Subaccount => quote!(#crate_path::model::field::EntityFieldKind::Subaccount),
            Self::Text => quote!(#crate_path::model::field::EntityFieldKind::Text),
            Self::Timestamp => quote!(#crate_path::model::field::EntityFieldKind::Timestamp),
            Self::Uint => quote!(#crate_path::model::field::EntityFieldKind::Uint),
            Self::Uint128 => quote!(#crate_path::model::field::EntityFieldKind::Uint128),
            Self::UintBig => quote!(#crate_path::model::field::EntityFieldKind::UintBig),
            Self::Ulid => quote!(#crate_path::model::field::EntityFieldKind::Ulid),
            Self::Unit => quote!(#crate_path::model::field::EntityFieldKind::Unit),
            Self::Unsupported => quote!(#crate_path::model::field::EntityFieldKind::Unsupported),
        }
    }
}

// Resolve a field kind token for a specific field name.
#[allow(clippy::too_many_arguments)]
fn field_kind_tokens(
    field_map: &FieldMap<'_>,
    name: &str,
    is_primary_key: bool,
    crate_path: &Path,
    struct_ident: &Ident,
    field_index: usize,
    aux_consts: &mut Vec<TokenStream2>,
    ref_key_kind_ident: &mut Option<Ident>,
) -> TokenStream2 {
    // Phase 1: resolve the declared field type.
    let Some(ty) = field_map.fields.get(name) else {
        return KindToken::Unsupported.tokens(crate_path);
    };

    // Phase 2: primary keys use scalar identity mapping.
    if is_primary_key {
        return infer_primary_key_kind(ty).tokens(crate_path);
    }

    // Phase 3: normalize wrappers before ref detection.
    let ty = peel_reference(ty);
    let ty = unwrap_option(ty).unwrap_or(ty);

    // Phase 4: detect ref collections before direct refs.
    if let Some(inner) = unwrap_vec(ty)
        && let Some(target) = extract_ref_target(inner)
    {
        return ref_kind_tokens(
            target,
            true,
            crate_path,
            struct_ident,
            field_index,
            aux_consts,
            ref_key_kind_ident,
        );
    }

    // Phase 5: direct refs.
    if let Some(target) = extract_ref_target(ty) {
        return ref_kind_tokens(
            target,
            false,
            crate_path,
            struct_ident,
            field_index,
            aux_consts,
            ref_key_kind_ident,
        );
    }

    // Phase 6: scalar fallback.

    infer_scalar_kind(ty).tokens(crate_path)
}

fn ref_kind_tokens(
    target: &Type,
    list: bool,
    crate_path: &Path,
    struct_ident: &Ident,
    field_index: usize,
    aux_consts: &mut Vec<TokenStream2>,
    ref_key_kind_ident: &mut Option<Ident>,
) -> TokenStream2 {
    // Phase 1: ensure a shared key-kind constant exists for this entity.
    let struct_name_upper = struct_ident.to_string().to_ascii_uppercase();
    let key_kind_ident = ref_key_kind_ident.get_or_insert_with(|| {
        let ident = Ident::new(
            &format!("__ICYDB_TEST_KEY_KIND_{struct_name_upper}"),
            Span::call_site(),
        );
        aux_consts.push(quote! {
            const #ident: #crate_path::model::field::EntityFieldKind =
                #crate_path::model::field::EntityFieldKind::Ulid;
        });
        ident
    });

    // Phase 2: define a ref-kind constant for this specific field.
    let ref_ident = Ident::new(
        &format!("__ICYDB_TEST_REF_KIND_{struct_name_upper}_{field_index}"),
        Span::call_site(),
    );
    aux_consts.push(quote! {
        const #ref_ident: #crate_path::model::field::EntityFieldKind =
            #crate_path::model::field::EntityFieldKind::Ref {
                target_path: <#target as #crate_path::traits::Path>::PATH,
                key_kind: &#key_kind_ident,
            };
    });

    // Phase 3: return either the direct ref kind or a list wrapper.

    if list {
        quote!(#crate_path::model::field::EntityFieldKind::List(&#ref_ident))
    } else {
        quote!(#ref_ident)
    }
}

// Infer a runtime kind for primary keys (Ref<T> treated as Ulid).
fn infer_primary_key_kind(ty: &Type) -> KindToken {
    let ty = peel_reference(ty);
    if let Some(inner) = unwrap_option(ty) {
        return infer_primary_key_kind(inner);
    }

    match ty {
        Type::Tuple(tuple) if tuple.elems.is_empty() => KindToken::Unit,
        Type::Path(path) => kind_from_path_primary_key(path),
        _ => KindToken::Unsupported,
    }
}

// Infer scalar kinds for non-reference fields.
fn infer_scalar_kind(ty: &Type) -> KindToken {
    match ty {
        Type::Tuple(tuple) if tuple.elems.is_empty() => KindToken::Unit,
        Type::Path(path) => kind_from_path(path),
        _ => KindToken::Unsupported,
    }
}

// Recognize common scalar shapes from a type path's last segment.
fn kind_from_path(path: &TypePath) -> KindToken {
    let Some(ident) = path
        .path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
    else {
        return KindToken::Unsupported;
    };

    match ident.as_str() {
        "Account" => KindToken::Account,
        "Blob" => KindToken::Blob,
        "bool" => KindToken::Bool,
        "Date" => KindToken::Date,
        "Decimal" => KindToken::Decimal,
        "Duration" => KindToken::Duration,
        "E8s" => KindToken::E8s,
        "E18s" => KindToken::E18s,
        "Float32" => KindToken::Float32,
        "Float64" => KindToken::Float64,
        "i8" | "i16" | "i32" | "i64" => KindToken::Int,
        "i128" => KindToken::Int128,
        "Int" => KindToken::IntBig,
        "Principal" => KindToken::Principal,
        "Subaccount" => KindToken::Subaccount,
        "String" | "str" => KindToken::Text,
        "Timestamp" => KindToken::Timestamp,
        "u8" | "u16" | "u32" | "u64" => KindToken::Uint,
        "u128" => KindToken::Uint128,
        "Nat" => KindToken::UintBig,
        "Ulid" => KindToken::Ulid,
        _ => KindToken::Unsupported,
    }
}

// Primary key mapping allows Ref<T> to stand in for scalar identity.
fn kind_from_path_primary_key(path: &TypePath) -> KindToken {
    let Some(ident) = path
        .path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
    else {
        return KindToken::Unsupported;
    };

    if ident == "Ref" {
        return KindToken::Ulid;
    }

    kind_from_path(path)
}

// Strip reference wrappers like &T or &mut T.
fn peel_reference(ty: &Type) -> &Type {
    if let Type::Reference(reference) = ty {
        return peel_reference(&reference.elem);
    }

    ty
}

// Extract the inner type from Option<T> if present.
fn unwrap_option(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };

    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };

    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

// Extract the inner type from Vec<T> if present.
fn unwrap_vec(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };

    let segment = type_path.path.segments.last()?;
    if segment.ident != "Vec" {
        return None;
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };

    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

// Extract the target type from Ref<T> if present.
fn extract_ref_target(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };

    let segment = type_path.path.segments.last()?;
    if segment.ident != "Ref" {
        return None;
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };

    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

// Parse a required string literal argument.
fn parse_lit_str(expr: &Expr, name: &str) -> Result<LitStr, syn::Error> {
    let Expr::Lit(ExprLit {
        lit: Lit::Str(lit), ..
    }) = expr
    else {
        return Err(syn::Error::new(
            expr.span(),
            format!("test_entity expects {name} to be a string literal"),
        ));
    };

    Ok(lit.clone())
}

// Parse a required path argument.
fn parse_path(expr: &Expr, name: &str) -> Result<Path, syn::Error> {
    let Expr::Path(ExprPath { path, .. }) = expr else {
        return Err(syn::Error::new(
            expr.span(),
            format!("test_entity expects {name} to be a path"),
        ));
    };

    Ok(path.clone())
}

// Parse a required identifier argument.
fn parse_ident(expr: &Expr, name: &str) -> Result<Ident, syn::Error> {
    let Expr::Path(ExprPath { path, .. }) = expr else {
        return Err(syn::Error::new(
            expr.span(),
            format!("test_entity expects {name} to be an identifier"),
        ));
    };

    let ident = path.get_ident().ok_or_else(|| {
        syn::Error::new(
            path.span(),
            format!("test_entity expects {name} to be an identifier"),
        )
    })?;

    Ok(ident.clone())
}

// Parse the fields array from the attribute.
fn parse_fields(expr: &Expr) -> Result<Vec<LitStr>, syn::Error> {
    let Expr::Array(ExprArray { elems, .. }) = expr else {
        return Err(syn::Error::new(
            expr.span(),
            "test_entity expects fields to be an array of string literals",
        ));
    };

    let mut out = Vec::with_capacity(elems.len());
    for elem in elems {
        let Expr::Lit(ExprLit {
            lit: Lit::Str(lit), ..
        }) = elem
        else {
            return Err(syn::Error::new(
                elem.span(),
                "test_entity expects fields to be an array of string literals",
            ));
        };
        out.push(lit.clone());
    }

    Ok(out)
}

// Enforce that required arguments were provided.
fn require_arg<T>(value: Option<T>, name: &str) -> Result<T, syn::Error> {
    value.ok_or_else(|| {
        syn::Error::new(
            Span::call_site(),
            format!("test_entity missing required argument: {name}"),
        )
    })
}
