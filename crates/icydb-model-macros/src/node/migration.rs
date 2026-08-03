//! Parser and token lowering for one source-declared database migration plan.

use crate::prelude::*;
use darling::ast::NestedMeta;

#[derive(Debug)]
pub(crate) struct MigrationPlan {
    transitions: Vec<EntityMigration>,
}

#[derive(Debug)]
struct EntityMigration {
    entity: LitStr,
    from: u32,
    from_name: Option<LitStr>,
    renames: Vec<Rename>,
    transforms: Vec<Transform>,
}

#[derive(Debug)]
enum Rename {
    Field {
        from: LitStr,
        to: LitStr,
    },
    NamedType {
        from: LitStr,
        to: LitStr,
    },
    Variant {
        named_type: LitStr,
        from: LitStr,
        to: LitStr,
    },
    RecordField {
        named_type: LitStr,
        from: LitStr,
        to: LitStr,
    },
    Relation {
        from: LitStr,
        to: LitStr,
    },
    Constraint {
        from: LitStr,
        to: LitStr,
    },
    Rule {
        named_type: LitStr,
        from: LitStr,
        to: LitStr,
    },
}

#[derive(Debug)]
enum Transform {
    Fill {
        to: LitStr,
        literal: TokenStream,
    },
    Copy {
        from: LitStr,
        to: LitStr,
    },
    CheckedCast {
        from: LitStr,
        to: LitStr,
        target: TokenStream,
    },
    Coalesce {
        from: LitStr,
        to: LitStr,
        literal: TokenStream,
    },
}

impl MigrationPlan {
    pub(crate) fn constructor_tokens(&self) -> TokenStream {
        let transitions = self
            .transitions
            .iter()
            .map(EntityMigration::constructor_tokens);
        quote! {
            Some(|| {
                ::icydb_model::schema::SchemaMigrationPlan::try_new(vec![#(#transitions),*])
            })
        }
    }
}

impl FromMeta for MigrationPlan {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut transitions = Vec::new();
        for item in items {
            let list = expect_list(item, "migrations(...) accepts only entity_migration(...)")?;
            if !list.path.is_ident("entity_migration") {
                return Err(DarlingError::custom(
                    "migrations(...) accepts only entity_migration(...)",
                )
                .with_span(&list.path));
            }
            transitions.push(EntityMigration::parse(&parse_list(list)?)?);
        }
        if transitions.is_empty() {
            return Err(DarlingError::custom(
                "migrations(...) requires at least one entity_migration(...) declaration",
            ));
        }
        Ok(Self { transitions })
    }
}

impl EntityMigration {
    fn parse(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut entity = None;
        let mut from = None;
        let mut from_name = None;
        let mut renames = Vec::new();
        let mut transforms = Vec::new();
        let mut renames_seen = false;
        let mut transforms_seen = false;
        for item in items {
            match item {
                NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("entity") => {
                    set_once(&mut entity, string_value(value)?, &value.path)?;
                }
                NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("from") => {
                    set_once(&mut from, u32_value(value)?, &value.path)?;
                }
                NestedMeta::Meta(syn::Meta::NameValue(value))
                    if value.path.is_ident("from_name") =>
                {
                    set_once(&mut from_name, string_value(value)?, &value.path)?;
                }
                NestedMeta::Meta(syn::Meta::List(list)) if list.path.is_ident("renames") => {
                    if renames_seen {
                        return Err(duplicate(&list.path));
                    }
                    renames_seen = true;
                    renames = parse_list(list)?
                        .iter()
                        .map(Rename::parse)
                        .collect::<Result<_, _>>()?;
                }
                NestedMeta::Meta(syn::Meta::List(list)) if list.path.is_ident("transforms") => {
                    if transforms_seen {
                        return Err(duplicate(&list.path));
                    }
                    transforms_seen = true;
                    transforms = parse_list(list)?
                        .iter()
                        .map(Transform::parse)
                        .collect::<Result<_, _>>()?;
                }
                _ => {
                    return Err(DarlingError::custom(
                        "entity_migration(...) supports entity, from, from_name, renames(...), and transforms(...) only",
                    ));
                }
            }
        }
        if renames.is_empty() && transforms.is_empty() && from_name.is_none() {
            return Err(DarlingError::custom(
                "entity_migration(...) must declare a rename or transform",
            ));
        }
        let from =
            from.ok_or_else(|| DarlingError::custom("entity_migration requires from = N"))?;
        if from == 0 {
            return Err(DarlingError::custom(
                "entity_migration predecessor version must be nonzero",
            ));
        }
        Ok(Self {
            entity: entity.ok_or_else(|| {
                DarlingError::custom("entity_migration requires entity = \"...\"")
            })?,
            from,
            from_name,
            renames,
            transforms,
        })
    }

    fn constructor_tokens(&self) -> TokenStream {
        let entity = &self.entity;
        let from = self.from;
        let from_name = self.from_name.as_ref().map_or_else(
            || quote!(None),
            |name| quote!(Some(::icydb_model::schema::EntitySourceKey::try_new(#name.to_string())?)),
        );
        let renames = self.renames.iter().map(Rename::constructor_tokens);
        let transforms = self.transforms.iter().map(Transform::constructor_tokens);
        quote! {
            ::icydb_model::schema::EntityMigration::try_new(
                ::icydb_model::schema::EntitySourceKey::try_new(#entity.to_string())?,
                ::icydb_model::schema::DeclaredEntityVersion::try_new(#from)?,
                #from_name,
                vec![#(#renames),*],
                vec![#(#transforms),*],
            )?
        }
    }
}

impl Rename {
    fn parse(item: &NestedMeta) -> Result<Self, DarlingError> {
        let list = expect_list(item, "rename declarations must use name(...)")?;
        let items = parse_list(list)?;
        let named_type = optional_string(&items, "named_type")?;
        let from = required_string(&items, "from")?;
        let to = required_string(&items, "to")?;
        ensure_only_names(
            &items,
            if named_type.is_some() {
                &["named_type", "from", "to"]
            } else {
                &["from", "to"]
            },
        )?;
        if list.path.is_ident("field") {
            reject_present(named_type, &list.path).map(|()| Self::Field { from, to })
        } else if list.path.is_ident("named_type") {
            reject_present(named_type, &list.path).map(|()| Self::NamedType { from, to })
        } else if list.path.is_ident("variant") {
            Ok(Self::Variant {
                named_type: required_string(&items, "named_type")?,
                from,
                to,
            })
        } else if list.path.is_ident("record_field") {
            Ok(Self::RecordField {
                named_type: required_string(&items, "named_type")?,
                from,
                to,
            })
        } else if list.path.is_ident("relation") {
            reject_present(named_type, &list.path).map(|()| Self::Relation { from, to })
        } else if list.path.is_ident("constraint") {
            reject_present(named_type, &list.path).map(|()| Self::Constraint { from, to })
        } else if list.path.is_ident("rule") {
            Ok(Self::Rule {
                named_type: required_string(&items, "named_type")?,
                from,
                to,
            })
        } else {
            Err(DarlingError::custom("unknown migration rename declaration").with_span(&list.path))
        }
    }

    fn constructor_tokens(&self) -> TokenStream {
        let field = |value: &LitStr| quote!(::icydb_model::schema::FieldSourceKey::try_new(#value.to_string())?);
        let ty = |value: &LitStr| quote!(::icydb_model::schema::TypeSourceKey::try_new(#value.to_string())?);
        match self {
            Self::Field { from, to } => {
                let from = field(from);
                let to = field(to);
                quote!(::icydb_model::schema::SchemaMigrationRename::Field { from: #from, to: #to })
            }
            Self::NamedType { from, to } => {
                let from = ty(from);
                let to = ty(to);
                quote!(::icydb_model::schema::SchemaMigrationRename::NamedType { from: #from, to: #to })
            }
            Self::Variant {
                named_type,
                from,
                to,
            } => {
                let named_type = ty(named_type);
                let from = ty(from);
                let to = ty(to);
                quote!(::icydb_model::schema::SchemaMigrationRename::EnumVariant { named_type: #named_type, from: #from, to: #to })
            }
            Self::RecordField {
                named_type,
                from,
                to,
            } => {
                let named_type = ty(named_type);
                let from = field(from);
                let to = field(to);
                quote!(::icydb_model::schema::SchemaMigrationRename::RecordField { named_type: #named_type, from: #from, to: #to })
            }
            Self::Relation { from, to } => {
                quote!(::icydb_model::schema::SchemaMigrationRename::Relation { from: ::icydb_model::schema::RelationSourceKey::try_new(#from.to_string())?, to: ::icydb_model::schema::RelationSourceKey::try_new(#to.to_string())? })
            }
            Self::Constraint { from, to } => {
                quote!(::icydb_model::schema::SchemaMigrationRename::Constraint { from: ::icydb_model::schema::ConstraintSourceKey::try_new(#from.to_string())?, to: ::icydb_model::schema::ConstraintSourceKey::try_new(#to.to_string())? })
            }
            Self::Rule {
                named_type,
                from,
                to,
            } => {
                let named_type = ty(named_type);
                quote!(::icydb_model::schema::SchemaMigrationRename::Rule { named_type: #named_type, from: ::icydb_model::schema::RuleSourceKey::try_new(#from.to_string())?, to: ::icydb_model::schema::RuleSourceKey::try_new(#to.to_string())? })
            }
        }
    }
}

impl Transform {
    fn parse(item: &NestedMeta) -> Result<Self, DarlingError> {
        let list = expect_list(
            item,
            "transform declarations must use fill(...) or rewrite(...)",
        )?;
        let items = parse_list(list)?;
        if list.path.is_ident("fill") {
            let to = required_string(&items, "to")?;
            let literal = required_nested(&items, "literal").and_then(parse_literal)?;
            ensure_only_mixed(&items, &["to"], &["literal"])?;
            return Ok(Self::Fill { to, literal });
        }
        if !list.path.is_ident("rewrite") {
            return Err(
                DarlingError::custom("unknown migration transform declaration")
                    .with_span(&list.path),
            );
        }
        let from = required_string(&items, "from")?;
        let to = required_string(&items, "to")?;
        let operations = items.iter().filter_map(|item| match item {
            NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("copy") => Some(Ok(("copy", None))),
            NestedMeta::Meta(syn::Meta::List(operation)) if operation.path.is_ident("checked_cast") => Some(parse_checked_cast(operation).map(|tokens| ("checked_cast", Some(tokens)))),
            NestedMeta::Meta(syn::Meta::List(operation)) if operation.path.is_ident("coalesce") => Some(parse_coalesce(operation).map(|tokens| ("coalesce", Some(tokens)))),
            NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("from") || value.path.is_ident("to") => None,
            _ => Some(Err(DarlingError::custom("rewrite(...) accepts exactly one of copy, checked_cast(...), or coalesce(literal(...))"))),
        }).collect::<Result<Vec<_>, _>>()?;
        if operations.len() != 1 {
            return Err(DarlingError::custom(
                "rewrite(...) requires exactly one transform operation",
            ));
        }
        match &operations[0] {
            ("copy", _) => Ok(Self::Copy { from, to }),
            ("checked_cast", Some(target)) => Ok(Self::CheckedCast {
                from,
                to,
                target: target.clone(),
            }),
            ("coalesce", Some(literal)) => Ok(Self::Coalesce {
                from,
                to,
                literal: literal.clone(),
            }),
            _ => Err(DarlingError::custom("invalid rewrite transform")),
        }
    }

    fn constructor_tokens(&self) -> TokenStream {
        let field = |value: &LitStr| quote!(::icydb_model::schema::FieldSourceKey::try_new(#value.to_string())?);
        match self {
            Self::Fill { to, literal } => {
                let to = field(to);
                quote!(::icydb_model::schema::SchemaMigrationTransform::Fill { to: #to, literal: #literal })
            }
            Self::Copy { from, to } => {
                let from = field(from);
                let to = field(to);
                quote!(::icydb_model::schema::SchemaMigrationTransform::Copy { from: #from, to: #to })
            }
            Self::CheckedCast { from, to, target } => {
                let from = field(from);
                let to = field(to);
                quote!(::icydb_model::schema::SchemaMigrationTransform::CheckedCast { from: #from, to: #to, target: #target })
            }
            Self::Coalesce { from, to, literal } => {
                let from = field(from);
                let to = field(to);
                quote!(::icydb_model::schema::SchemaMigrationTransform::Coalesce { from: #from, to: #to, literal: #literal })
            }
        }
    }
}

fn parse_checked_cast(list: &syn::MetaList) -> Result<TokenStream, DarlingError> {
    let items = parse_list(list)?;
    ensure_only_names(&items, &["to"])?;
    scalar_type_tokens(&required_string(&items, "to")?)
}

fn parse_coalesce(list: &syn::MetaList) -> Result<TokenStream, DarlingError> {
    let items = parse_list(list)?;
    ensure_only_mixed(&items, &[], &["literal"])?;
    parse_literal(required_nested(&items, "literal")?)
}

fn scalar_type_tokens(value: &LitStr) -> Result<TokenStream, DarlingError> {
    let text = value.value();
    if let Some(scale) = text
        .strip_prefix("Decimal(")
        .and_then(|value| value.strip_suffix(')'))
    {
        let scale = scale.parse::<u32>().map_err(|_| {
            DarlingError::custom("Decimal checked_cast target requires a u32 scale")
                .with_span(value)
        })?;
        if scale > icydb_schema::Decimal::max_supported_scale() {
            return Err(DarlingError::custom(
                "Decimal checked_cast scale exceeds the supported maximum",
            )
            .with_span(value));
        }
        return Ok(quote!(::icydb_model::schema::ScalarType::Decimal { scale: #scale }));
    }
    let variant = match text.as_str() {
        "Int8" | "Int16" | "Int32" | "Int64" | "Int128" | "Nat8" | "Nat16" | "Nat32" | "Nat64"
        | "Nat128" => Ident::new(text.as_str(), value.span()),
        _ => {
            return Err(DarlingError::custom(
                "checked_cast target must be a fixed Int/Nat kind or Decimal(scale)",
            )
            .with_span(value));
        }
    };
    Ok(quote!(::icydb_model::schema::ScalarType::#variant))
}

fn parse_literal(list: &syn::MetaList) -> Result<TokenStream, DarlingError> {
    let items = parse_list(list)?;
    if items.len() != 1 {
        return Err(DarlingError::custom(
            "literal(...) requires exactly one typed value",
        ));
    }
    match &items[0] {
        NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("unit") => Ok(quote!(
            ::icydb_model::schema::ScalarLiteral::Unit(::icydb_model::schema::Unit)
        )),
        NestedMeta::Meta(syn::Meta::List(value)) if value.path.is_ident("enum") => {
            let nested = parse_list(value)?;
            ensure_only_names(&nested, &["named_type", "variant"])?;
            let named_type = required_string(&nested, "named_type")?;
            let variant = required_string(&nested, "variant")?;
            Ok(quote!(::icydb_model::schema::ScalarLiteral::EnumUnit {
                enum_type: ::icydb_model::schema::TypeSourceKey::try_new(#named_type.to_string())?,
                variant: ::icydb_model::schema::TypeSourceKey::try_new(#variant.to_string())?,
            }))
        }
        NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("bool") => {
            let syn::Expr::Lit(expr) = &value.value else {
                return Err(literal_error(&value.value));
            };
            let syn::Lit::Bool(literal) = &expr.lit else {
                return Err(literal_error(&value.value));
            };
            Ok(quote!(::icydb_model::schema::ScalarLiteral::Bool(#literal)))
        }
        NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("int") => {
            let expression = integer_expression(&value.value, true)?;
            Ok(quote!(::icydb_model::schema::ScalarLiteral::Int((#expression) as i128)))
        }
        NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("nat") => {
            let expression = integer_expression(&value.value, false)?;
            Ok(quote!(::icydb_model::schema::ScalarLiteral::Nat((#expression) as u128)))
        }
        NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("text") => {
            let text = string_value(value)?;
            Ok(quote!(::icydb_model::schema::ScalarLiteral::Text(#text.to_string())))
        }
        NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("float32") => {
            let expression = float_expression(&value.value)?;
            Ok(quote!(::icydb_model::schema::ScalarLiteral::Float32(
                ::icydb_model::schema::Float32::try_new((#expression) as f32)
                    .ok_or(::icydb_model::schema::SchemaContractError::InvalidLiteral)?
            )))
        }
        NestedMeta::Meta(syn::Meta::NameValue(value)) if value.path.is_ident("float64") => {
            let expression = float_expression(&value.value)?;
            Ok(quote!(::icydb_model::schema::ScalarLiteral::Float64(
                ::icydb_model::schema::Float64::try_new((#expression) as f64)
                    .ok_or(::icydb_model::schema::SchemaContractError::InvalidLiteral)?
            )))
        }
        NestedMeta::Meta(syn::Meta::NameValue(value)) => {
            let Some(kind) = value.path.get_ident().map(ToString::to_string) else {
                return Err(literal_error(&value.value));
            };
            if !matches!(
                kind.as_str(),
                "account"
                    | "blob"
                    | "date"
                    | "decimal"
                    | "duration"
                    | "int_big"
                    | "nat_big"
                    | "principal"
                    | "subaccount"
                    | "timestamp"
                    | "ulid"
            ) {
                return Err(DarlingError::custom("unsupported typed migration literal"));
            }
            let text = string_value(value)?;
            Ok(quote!(::icydb_model::node::migration_literal_from_text(#kind, #text)?))
        }
        _ => Err(DarlingError::custom(
            "literal(...) requires one supported typed MigrationProgramV1 literal",
        )),
    }
}

fn float_expression(value: &syn::Expr) -> Result<&syn::Expr, DarlingError> {
    let valid = matches!(
        value,
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Float(_) | syn::Lit::Int(_),
            ..
        })
    ) || matches!(
        value,
        syn::Expr::Unary(syn::ExprUnary {
            op: syn::UnOp::Neg(_),
            expr,
            ..
        }) if matches!(expr.as_ref(), syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Float(_) | syn::Lit::Int(_), .. }))
    );
    valid.then_some(value).ok_or_else(|| literal_error(value))
}

fn integer_expression(value: &syn::Expr, signed: bool) -> Result<&syn::Expr, DarlingError> {
    let valid = matches!(
        value,
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Int(_),
            ..
        })
    ) || signed
        && matches!(value, syn::Expr::Unary(syn::ExprUnary { op: syn::UnOp::Neg(_), expr, .. }) if matches!(expr.as_ref(), syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Int(_), .. })));
    valid.then_some(value).ok_or_else(|| literal_error(value))
}

fn parse_list(list: &syn::MetaList) -> Result<Vec<NestedMeta>, DarlingError> {
    Ok(NestedMeta::parse_meta_list(list.tokens.clone())?)
}

fn expect_list<'a>(item: &'a NestedMeta, message: &str) -> Result<&'a syn::MetaList, DarlingError> {
    match item {
        NestedMeta::Meta(syn::Meta::List(list)) => Ok(list),
        _ => Err(DarlingError::custom(message)),
    }
}

fn required_nested<'a>(
    items: &'a [NestedMeta],
    name: &str,
) -> Result<&'a syn::MetaList, DarlingError> {
    let mut found = items.iter().filter_map(|item| match item {
        NestedMeta::Meta(syn::Meta::List(list)) if list.path.is_ident(name) => Some(list),
        _ => None,
    });
    let value = found
        .next()
        .ok_or_else(|| DarlingError::custom(format!("requires {name}(...)")))?;
    if found.next().is_some() {
        return Err(DarlingError::custom(format!(
            "accepts {name}(...) exactly once"
        )));
    }
    Ok(value)
}

fn required_string(items: &[NestedMeta], name: &str) -> Result<LitStr, DarlingError> {
    optional_string(items, name)?
        .ok_or_else(|| DarlingError::custom(format!("requires {name} = \"...\"")))
}

fn optional_string(items: &[NestedMeta], name: &str) -> Result<Option<LitStr>, DarlingError> {
    let mut found = None;
    for item in items {
        if let NestedMeta::Meta(syn::Meta::NameValue(value)) = item
            && value.path.is_ident(name)
        {
            set_once(&mut found, string_value(value)?, &value.path)?;
        }
    }
    Ok(found)
}

fn ensure_only_names(items: &[NestedMeta], names: &[&str]) -> Result<(), DarlingError> {
    ensure_only_mixed(items, names, &[])
}

fn ensure_only_mixed(
    items: &[NestedMeta],
    names: &[&str],
    lists: &[&str],
) -> Result<(), DarlingError> {
    for item in items {
        let valid = match item {
            NestedMeta::Meta(syn::Meta::NameValue(value)) => {
                names.iter().any(|name| value.path.is_ident(name))
            }
            NestedMeta::Meta(syn::Meta::List(value)) => {
                lists.iter().any(|name| value.path.is_ident(name))
            }
            _ => false,
        };
        if !valid {
            return Err(DarlingError::custom(
                "unsupported migration declaration argument",
            ));
        }
    }
    Ok(())
}

fn string_value(value: &syn::MetaNameValue) -> Result<LitStr, DarlingError> {
    let syn::Expr::Lit(expression) = &value.value else {
        return Err(literal_error(&value.value));
    };
    let syn::Lit::Str(literal) = &expression.lit else {
        return Err(literal_error(&value.value));
    };
    Ok(literal.clone())
}

fn u32_value(value: &syn::MetaNameValue) -> Result<u32, DarlingError> {
    let syn::Expr::Lit(expression) = &value.value else {
        return Err(literal_error(&value.value));
    };
    let syn::Lit::Int(literal) = &expression.lit else {
        return Err(literal_error(&value.value));
    };
    literal
        .base10_parse()
        .map_err(|_| literal_error(&value.value))
}

fn set_once<T>(slot: &mut Option<T>, value: T, path: &syn::Path) -> Result<(), DarlingError> {
    if slot.replace(value).is_some() {
        return Err(duplicate(path));
    }
    Ok(())
}

fn reject_present<T>(value: Option<T>, path: &syn::Path) -> Result<(), DarlingError> {
    if value.is_some() {
        return Err(
            DarlingError::custom("rename declaration does not accept named_type").with_span(path),
        );
    }
    Ok(())
}

fn duplicate(path: &syn::Path) -> DarlingError {
    DarlingError::custom("migration declaration argument may appear only once").with_span(path)
}

fn literal_error(value: &syn::Expr) -> DarlingError {
    DarlingError::custom("migration declaration has an invalid literal").with_span(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn parse(tokens: TokenStream) -> Result<MigrationPlan, DarlingError> {
        MigrationPlan::from_list(&NestedMeta::parse_meta_list(tokens)?)
    }

    #[test]
    fn complete_plan_grammar_parses_and_emits_one_constructor() {
        let plan = parse(quote!(
            entity_migration(
                entity = "Account",
                from = 1,
                from_name = "User",
                renames(
                    field(from = "email", to = "primary_email"),
                    named_type(from = "Status", to = "AccountStatus"),
                    variant(named_type = "Status", from = "Active", to = "Enabled"),
                    record_field(named_type = "Profile", from = "display", to = "display_name"),
                    constraint(from = "old", to = "new"),
                    rule(named_type = "Rating", from = "range", to = "valid_range"),
                    relation(from = "author", to = "creator")
                ),
                transforms(
                    rewrite(from = "age", to = "age2", checked_cast(to = "Nat16")),
                    fill(to = "status", literal(enum(named_type = "AccountStatus", variant = "Enabled")))
                )
            )
        )).expect("frozen grammar should parse");
        let tokens = plan.constructor_tokens().to_string();
        assert!(tokens.contains("SchemaMigrationPlan :: try_new"));
        assert!(tokens.contains("SchemaMigrationRename :: EnumVariant"));
        assert!(tokens.contains("SchemaMigrationTransform :: CheckedCast"));
    }

    #[test]
    fn empty_and_unknown_plan_forms_reject() {
        assert!(parse(quote!()).is_err());
        assert!(parse(quote!(entity_migration(entity = "User", from = 1))).is_err());
        assert!(parse(quote!(unknown(entity = "User", from = 1))).is_err());
    }

    #[test]
    fn decimal_cast_target_accepts_only_the_frozen_parenthesized_spelling() {
        assert!(scalar_type_tokens(&LitStr::new("Decimal(8)", Span::call_site())).is_ok());
        assert!(scalar_type_tokens(&LitStr::new("Decimal:8", Span::call_site())).is_err());
    }
}
