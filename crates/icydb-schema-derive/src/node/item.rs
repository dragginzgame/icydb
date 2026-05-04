use crate::prelude::*;

///
/// Item
///

#[expect(
    clippy::struct_excessive_bools,
    reason = "darling maps independent item directives directly onto this parse node"
)]
#[derive(Clone, Debug, Default, FromMeta)]
pub struct Item {
    #[darling(default)]
    pub(crate) is: Option<Path>,

    #[darling(default, rename = "prim")]
    pub(crate) primitive: Option<Primitive>,

    #[darling(default)]
    pub(crate) scale: Option<u32>,

    #[darling(default)]
    pub(crate) max_len: Option<u32>,

    #[darling(default)]
    pub(crate) unbounded: bool,

    #[darling(default, rename = "rel")]
    pub(crate) relation: Option<Path>,

    #[darling(default)]
    pub(crate) strong: bool,

    #[darling(default)]
    pub(crate) weak: bool,

    #[darling(multiple, rename = "sanitizer")]
    pub(crate) sanitizers: Vec<TypeSanitizer>,

    #[darling(multiple, rename = "validator")]
    pub(crate) validators: Vec<TypeValidator>,

    #[darling(default)]
    pub(crate) indirect: bool,
}

impl Item {
    pub fn validate(&self) -> Result<(), DarlingError> {
        // Phase 1: reject incompatible option pairs.
        if self.is.is_some() && self.primitive.is_some() {
            return Err(DarlingError::custom(
                "item may not specify both is and prim",
            ));
        }

        // Phase 2: validate relation strength flags.
        if self.strong && self.weak {
            return Err(DarlingError::custom(
                "relation cannot be both strong and weak",
            ));
        }
        if self.relation.is_none() && (self.strong || self.weak) {
            return Err(DarlingError::custom(
                "strong/weak may only be used with rel",
            ));
        }
        if let Some(relation) = &self.relation
            && self.primitive.is_none()
        {
            return Err(
                DarlingError::custom(
                    "rel fields must explicitly declare prim = \"...\"; implicit identity types are forbidden",
                )
                .with_span(relation),
            );
        }

        // Phase 3: enforce relation constraints.
        if let Some(relation) = &self.relation
            && self.indirect
        {
            return Err(
                DarlingError::custom("relations cannot be set to indirect").with_span(relation)
            );
        }

        // Phase 4: validate scalar metadata owned by item directives.
        if self.scale.is_some() && !matches!(self.primitive, Some(Primitive::Decimal)) {
            return Err(DarlingError::custom(
                "scale may only be used with prim = \"Decimal\"",
            ));
        }
        if matches!(self.primitive, Some(Primitive::Decimal)) && self.scale.is_none() {
            return Err(DarlingError::custom(
                "prim = \"Decimal\" requires item(scale = N)",
            ));
        }
        if self.max_len.is_some()
            && !matches!(self.primitive, Some(Primitive::Text | Primitive::Blob))
        {
            return Err(DarlingError::custom(
                "max_len may only be used with prim = \"Text\" or prim = \"Blob\"",
            ));
        }
        if self.max_len.is_some_and(|max_len| max_len == 0) {
            return Err(DarlingError::custom(
                "item(max_len = N) requires a positive value",
            ));
        }
        if self.unbounded && !matches!(self.primitive, Some(Primitive::Text | Primitive::Blob)) {
            return Err(DarlingError::custom(
                "unbounded may only be used with prim = \"Text\" or prim = \"Blob\"",
            ));
        }
        if self.unbounded && self.max_len.is_some() {
            return Err(DarlingError::custom(
                "unbounded cannot be combined with max_len",
            ));
        }
        if self.max_len.is_none() && !self.unbounded {
            match self.primitive {
                Some(Primitive::Text) => {
                    return Err(DarlingError::custom(
                        "prim = \"Text\" requires either item(max_len = N) or item(unbounded)",
                    ));
                }
                Some(Primitive::Blob) => {
                    return Err(DarlingError::custom(
                        "prim = \"Blob\" requires either item(max_len = N) or item(unbounded)",
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn target(&self) -> ItemTarget {
        debug_assert!(
            !(self.is.is_some() && self.primitive.is_some()),
            "item 'is' cannot be combined with 'prim'",
        );

        if let Some(path) = &self.is {
            return ItemTarget::Is(path.clone());
        }
        if let Some(prim) = &self.primitive {
            return ItemTarget::Primitive(*prim);
        }

        ItemTarget::Primitive(Primitive::Unit)
    }

    pub fn created_at() -> Self {
        Self {
            primitive: Some(Primitive::Timestamp),
            sanitizers: vec![TypeSanitizer::new(
                "icydb::base::sanitizer::time::CreatedAt",
                Args::none(),
            )],
            ..Default::default()
        }
    }

    pub fn updated_at() -> Self {
        Self {
            primitive: Some(Primitive::Timestamp),
            sanitizers: vec![TypeSanitizer::new(
                "icydb::base::sanitizer::time::UpdatedAt",
                Args::none(),
            )],
            ..Default::default()
        }
    }

    pub const fn is_relation(&self) -> bool {
        self.relation.is_some()
    }
}

impl HasSchemaPart for Item {
    fn schema_part(&self) -> TokenStream {
        let target = self.target().schema_part();
        let relation = quote_option(self.relation.as_ref(), to_path);
        let scale = quote_option(self.scale.as_ref(), |scale| quote!(#scale));
        let max_len = quote_option(self.max_len.as_ref(), |max_len| quote!(#max_len));
        let validators = quote_slice(&self.validators, TypeValidator::schema_part);
        let sanitizers = quote_slice(&self.sanitizers, TypeSanitizer::schema_part);
        let indirect = self.indirect;

        quote! {
            ::icydb::schema::node::Item::new(
                #target,
                #relation,
                #scale,
                #max_len,
                #validators,
                #sanitizers,
                #indirect,
            )
        }
    }
}

impl HasTypeExpr for Item {
    fn type_expr(&self) -> TokenStream {
        let ty = self.target().type_expr();

        if self.indirect {
            quote!(Box<#ty>)
        } else {
            quote!(#ty)
        }
    }
}

///
/// ItemTarget
///

pub enum ItemTarget {
    Is(Path),
    Primitive(Primitive),
}

impl HasSchemaPart for ItemTarget {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::Is(path) => {
                let path = quote_one(path, to_path);
                quote! {
                    ::icydb::schema::node::ItemTarget::Is(#path)
                }
            }
            Self::Primitive(prim) => {
                quote! {
                    ::icydb::schema::node::ItemTarget::Primitive(#prim)
                }
            }
        }
    }
}

impl HasTypeExpr for ItemTarget {
    fn type_expr(&self) -> TokenStream {
        match self {
            Self::Is(path) => quote!(#path),
            Self::Primitive(prim) => {
                let ty = prim.as_type();
                quote!(#ty)
            }
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::Item;
    use darling::{FromMeta, ast::NestedMeta};
    use icydb_schema::types::Primitive;
    use quote::quote;

    #[test]
    fn validate_accepts_scale_for_decimal_primitive() {
        let item = Item {
            primitive: Some(Primitive::Decimal),
            scale: Some(8),
            ..Item::default()
        };

        assert!(item.validate().is_ok());
    }

    #[test]
    fn validate_rejects_scale_for_non_decimal_primitive() {
        let item = Item {
            primitive: Some(Primitive::Nat64),
            scale: Some(8),
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }

    #[test]
    fn validate_rejects_scale_without_declared_primitive() {
        let item = Item {
            scale: Some(8),
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }

    #[test]
    fn validate_rejects_decimal_without_scale() {
        let item = Item {
            primitive: Some(Primitive::Decimal),
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }

    #[test]
    fn validate_accepts_max_len_for_text_primitive() {
        let item = Item {
            primitive: Some(Primitive::Text),
            max_len: Some(32),
            ..Item::default()
        };

        assert!(item.validate().is_ok());
    }

    #[test]
    fn validate_accepts_max_len_for_blob_primitive() {
        let item = Item {
            primitive: Some(Primitive::Blob),
            max_len: Some(32),
            ..Item::default()
        };

        assert!(item.validate().is_ok());
    }

    #[test]
    fn validate_accepts_explicit_unbounded_for_text_primitive() {
        let item = Item {
            primitive: Some(Primitive::Text),
            unbounded: true,
            ..Item::default()
        };

        assert!(item.validate().is_ok());
    }

    #[test]
    fn validate_accepts_explicit_unbounded_for_blob_primitive() {
        let item = Item {
            primitive: Some(Primitive::Blob),
            unbounded: true,
            ..Item::default()
        };

        assert!(item.validate().is_ok());
    }

    #[test]
    fn from_list_accepts_unbounded_flag_directive() {
        let args = NestedMeta::parse_meta_list(quote!(prim = "Text", unbounded))
            .expect("item args should parse");

        let item = Item::from_list(&args).expect("item meta should lower");

        assert!(item.unbounded);
        assert!(item.validate().is_ok());
    }

    #[test]
    fn from_list_accepts_unbounded_name_value_directive() {
        let args = NestedMeta::parse_meta_list(quote!(prim = "Blob", unbounded = true))
            .expect("item args should parse");

        let item = Item::from_list(&args).expect("item meta should lower");

        assert!(item.unbounded);
        assert!(item.validate().is_ok());
    }

    #[test]
    fn validate_rejects_max_len_for_unbounded_primitive() {
        let item = Item {
            primitive: Some(Primitive::Nat64),
            max_len: Some(32),
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }

    #[test]
    fn validate_rejects_implicit_unbounded_text() {
        let item = Item {
            primitive: Some(Primitive::Text),
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }

    #[test]
    fn validate_rejects_implicit_unbounded_blob() {
        let item = Item {
            primitive: Some(Primitive::Blob),
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }

    #[test]
    fn validate_rejects_unbounded_with_max_len() {
        let item = Item {
            primitive: Some(Primitive::Text),
            max_len: Some(32),
            unbounded: true,
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }

    #[test]
    fn validate_rejects_max_len_without_declared_primitive() {
        let item = Item {
            max_len: Some(32),
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_len() {
        let item = Item {
            primitive: Some(Primitive::Text),
            max_len: Some(0),
            ..Item::default()
        };

        assert!(item.validate().is_err());
    }
}
