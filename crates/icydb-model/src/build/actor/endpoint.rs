//! Module: build::actor::endpoint
//! Responsibility: parse, validate, plan, and emit the closed 0.217 endpoint declaration vocabulary.
//! Does not own: the public macro, exported capability assertions, or private handler bodies.
//! Boundary: package-private pre-cut compiler machinery used only by later actor codegen landings.

use std::{collections::BTreeSet, fmt};

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    Attribute, Expr, ExprLit, ExprPath, Ident, Lit, Path, Token, parenthesized,
    parse::{Parse, ParseStream, Parser},
    punctuated::Punctuated,
};

/// One accepted authorization token from the closed declaration grammar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EndpointAuthorization {
    Public,
    Controller,
}

/// One accepted SQL update-admission token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EndpointUpdateAdmission {
    PrimaryKeyOnly,
    BoundedDeterministic,
}

/// One exact Cargo capability required by an active endpoint declaration.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum EndpointCapability {
    IcydbSql,
    IcydbSqlExplain,
    IcydbMetricsExtended,
    CanisterTestAdminApi,
}

impl EndpointCapability {
    const fn feature(self) -> &'static str {
        match self {
            Self::IcydbSql => "icydb/sql",
            Self::IcydbSqlExplain => "icydb/sql-explain",
            Self::IcydbMetricsExtended => "icydb/metrics-extended",
            Self::CanisterTestAdminApi => "test-admin-api",
        }
    }
}

/// Exact authorization route owned by one generated wrapper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EndpointAuthorizationPlan {
    Public,
    SqlController,
    OperationalController,
    SchemaController,
}

/// Closed feature conjunction required by one active declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EndpointCapabilityPlan(&'static [EndpointCapability]);

impl EndpointCapabilityPlan {
    const fn features(self) -> &'static [EndpointCapability] {
        self.0
    }
}

/// Fixed endpoint declaration identities accepted by 0.217.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum EndpointName {
    SqlQuery,
    Ddl,
    Update,
    Integrity,
    FixturesReset,
    FixturesLoad,
    Metrics,
    MetricsExtended,
    MetricsReset,
    Snapshot,
    Schema,
}

impl EndpointName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SqlQuery => "icydb_sql_query",
            Self::Ddl => "icydb_ddl",
            Self::Update => "icydb_update",
            Self::Integrity => "icydb_integrity",
            Self::FixturesReset => "icydb_fixtures_reset",
            Self::FixturesLoad => "icydb_fixtures_load",
            Self::Metrics => "icydb_metrics",
            Self::MetricsExtended => "icydb_metrics_extended",
            Self::MetricsReset => "icydb_metrics_reset",
            Self::Snapshot => "icydb_snapshot",
            Self::Schema => "icydb_schema",
        }
    }

    fn parse(ident: &Ident) -> Result<Self, EndpointDeclarationError> {
        match ident.to_string().as_str() {
            "icydb_sql_query" => Ok(Self::SqlQuery),
            "icydb_ddl" => Ok(Self::Ddl),
            "icydb_update" => Ok(Self::Update),
            "icydb_integrity" => Ok(Self::Integrity),
            "icydb_fixtures_reset" => Ok(Self::FixturesReset),
            "icydb_fixtures_load" => Ok(Self::FixturesLoad),
            "icydb_metrics" => Ok(Self::Metrics),
            "icydb_metrics_extended" => Ok(Self::MetricsExtended),
            "icydb_metrics_reset" => Ok(Self::MetricsReset),
            "icydb_snapshot" => Ok(Self::Snapshot),
            "icydb_schema" => Ok(Self::Schema),
            name => Err(EndpointDeclarationError::UnknownEndpoint {
                name: name.to_string(),
            }),
        }
    }
}

impl fmt::Display for EndpointName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Validated endpoint-specific declaration payload.
#[derive(Clone)]
pub(crate) enum EndpointDeclarationKind {
    SqlQuery {
        introspection: bool,
    },
    Ddl,
    Update {
        admission: EndpointUpdateAdmission,
    },
    Integrity,
    FixturesReset,
    FixturesLoad {
        handler: Path,
    },
    Metrics {
        authorization: EndpointAuthorization,
    },
    MetricsExtended {
        authorization: EndpointAuthorization,
    },
    MetricsReset,
    Snapshot,
    Schema {
        authorization: EndpointAuthorization,
    },
}

impl EndpointDeclarationKind {
    const fn name(&self) -> EndpointName {
        match self {
            Self::SqlQuery { .. } => EndpointName::SqlQuery,
            Self::Ddl => EndpointName::Ddl,
            Self::Update { .. } => EndpointName::Update,
            Self::Integrity => EndpointName::Integrity,
            Self::FixturesReset => EndpointName::FixturesReset,
            Self::FixturesLoad { .. } => EndpointName::FixturesLoad,
            Self::Metrics { .. } => EndpointName::Metrics,
            Self::MetricsExtended { .. } => EndpointName::MetricsExtended,
            Self::MetricsReset => EndpointName::MetricsReset,
            Self::Snapshot => EndpointName::Snapshot,
            Self::Schema { .. } => EndpointName::Schema,
        }
    }

    fn capability_plan(&self) -> EndpointCapabilityPlan {
        use EndpointCapability::{
            CanisterTestAdminApi, IcydbMetricsExtended, IcydbSql, IcydbSqlExplain,
        };

        let capabilities = match self {
            Self::SqlQuery {
                introspection: false,
            }
            | Self::Ddl
            | Self::Update { .. }
            | Self::Integrity => &[IcydbSql][..],
            Self::SqlQuery {
                introspection: true,
            } => &[IcydbSql, IcydbSqlExplain][..],
            Self::FixturesReset | Self::FixturesLoad { .. } => {
                &[IcydbSql, CanisterTestAdminApi][..]
            }
            Self::MetricsExtended { .. } => &[IcydbMetricsExtended][..],
            Self::Metrics { .. } | Self::MetricsReset | Self::Snapshot | Self::Schema { .. } => &[],
        };

        EndpointCapabilityPlan(capabilities)
    }

    const fn authorization_plan(&self) -> EndpointAuthorizationPlan {
        match self {
            Self::SqlQuery { .. }
            | Self::Ddl
            | Self::Update { .. }
            | Self::Integrity
            | Self::FixturesReset
            | Self::FixturesLoad { .. } => EndpointAuthorizationPlan::SqlController,
            Self::Metrics { authorization } | Self::MetricsExtended { authorization } => {
                match authorization {
                    EndpointAuthorization::Public => EndpointAuthorizationPlan::Public,
                    EndpointAuthorization::Controller => {
                        EndpointAuthorizationPlan::OperationalController
                    }
                }
            }
            Self::MetricsReset | Self::Snapshot => EndpointAuthorizationPlan::OperationalController,
            Self::Schema { authorization } => match authorization {
                EndpointAuthorization::Public => EndpointAuthorizationPlan::Public,
                EndpointAuthorization::Controller => EndpointAuthorizationPlan::SchemaController,
            },
        }
    }
}

/// One validated declaration and its ordinary Rust `cfg` attributes.
#[derive(Clone)]
pub(crate) struct EndpointDeclaration {
    cfg_attributes: Vec<Attribute>,
    kind: EndpointDeclarationKind,
}

/// One validated, duplicate-free declaration block in authored order.
#[derive(Clone)]
pub(crate) struct EndpointDeclarations(Vec<EndpointDeclaration>);

/// Typed failure from the private closed declaration compiler.
#[derive(Clone, Debug, Eq, thiserror::Error, PartialEq)]
pub(crate) enum EndpointDeclarationError {
    #[error("invalid endpoint declaration syntax: {message}")]
    Syntax { message: String },

    #[error("unknown endpoint declaration `{name}`")]
    UnknownEndpoint { name: String },

    #[error("endpoint `{endpoint}` is declared more than once")]
    DuplicateEndpoint { endpoint: EndpointName },

    #[error("endpoint `{endpoint}` does not accept declaration options")]
    OptionsNotAccepted { endpoint: EndpointName },

    #[error("endpoint `{endpoint}` requires declaration options")]
    MissingOptions { endpoint: EndpointName },

    #[error("endpoint `{endpoint}` requires option `{option}`")]
    MissingOption {
        endpoint: EndpointName,
        option: &'static str,
    },

    #[error("endpoint `{endpoint}` does not accept option `{option}`")]
    UnknownOption {
        endpoint: EndpointName,
        option: String,
    },

    #[error("endpoint `{endpoint}` repeats option `{option}`")]
    DuplicateOption {
        endpoint: EndpointName,
        option: String,
    },

    #[error("endpoint `{endpoint}` option `{option}` requires {expected}, not `{actual}`")]
    InvalidOptionValue {
        endpoint: EndpointName,
        option: &'static str,
        expected: &'static str,
        actual: String,
    },

    #[error("endpoint `{endpoint}` accepts only `#[cfg(...)]`, not `#[{attribute}]`")]
    UnsupportedAttribute {
        endpoint: EndpointName,
        attribute: String,
    },
}

/// Parse and validate one closed endpoint declaration block.
pub(crate) fn parse_endpoint_declarations(
    input: TokenStream,
) -> Result<EndpointDeclarations, EndpointDeclarationError> {
    let raw = syn::parse2::<RawEndpointDeclarations>(input).map_err(syntax_error)?;
    let mut names = BTreeSet::new();
    let mut declarations = Vec::with_capacity(raw.0.len());

    for declaration in raw.0 {
        let declaration = validate_declaration(declaration)?;
        let name = declaration.kind.name();
        if !names.insert(name) {
            return Err(EndpointDeclarationError::DuplicateEndpoint { endpoint: name });
        }
        declarations.push(declaration);
    }

    Ok(EndpointDeclarations(declarations))
}

/// Emit fixed public wrapper tokens for one validated declaration block.
pub(crate) fn emit_endpoint_wrappers(declarations: &EndpointDeclarations) -> TokenStream {
    let wrappers = declarations.0.iter().map(emit_endpoint_wrapper);

    quote! {
        #(#wrappers)*
    }
}

/// Parse, validate, and emit one internal wrapper-token bundle.
pub(crate) fn compile_endpoint_declarations(
    input: TokenStream,
) -> Result<TokenStream, EndpointDeclarationError> {
    let declarations = parse_endpoint_declarations(input)?;

    Ok(emit_endpoint_wrappers(&declarations))
}

/// Emit the private controller-authorization helpers used by the fixed wrappers.
pub(crate) fn emit_endpoint_authorization_helpers() -> TokenStream {
    quote! {
        pub(crate) mod endpoint_authorization {
            fn require_controller(
                boundary: ::icydb::diagnostic::RuntimeBoundaryCode,
            ) -> Result<(), ::icydb::Error> {
                let caller = ::icydb::__reexports::ic_cdk::api::msg_caller();
                if !::icydb::__reexports::ic_cdk::api::is_controller(&caller) {
                    return Err(::icydb::Error::from_runtime_boundary(
                        boundary,
                        ::icydb::ErrorOrigin::Interface,
                    ));
                }

                Ok(())
            }

            pub(crate) fn require_sql_controller() -> Result<(), ::icydb::Error> {
                require_controller(
                    ::icydb::diagnostic::RuntimeBoundaryCode::SqlSurfaceControllerRequired,
                )
            }

            pub(crate) fn require_operational_controller() -> Result<(), ::icydb::Error> {
                require_controller(
                    ::icydb::diagnostic::RuntimeBoundaryCode::OperationalSurfaceControllerRequired,
                )
            }

            pub(crate) fn require_schema_controller() -> Result<(), ::icydb::Error> {
                require_controller(
                    ::icydb::diagnostic::RuntimeBoundaryCode::SchemaSurfaceControllerRequired,
                )
            }
        }
    }
}

struct RawEndpointDeclarations(Vec<RawEndpointDeclaration>);

impl Parse for RawEndpointDeclarations {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut declarations = Vec::new();
        while !input.is_empty() {
            declarations.push(input.parse()?);
        }

        Ok(Self(declarations))
    }
}

struct RawEndpointDeclaration {
    attributes: Vec<Attribute>,
    name: Ident,
    options: Option<TokenStream>,
}

impl Parse for RawEndpointDeclaration {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let attributes = input.call(Attribute::parse_outer)?;
        let name = input.parse()?;
        let options = if input.peek(syn::token::Paren) {
            let content;
            parenthesized!(content in input);
            Some(content.parse()?)
        } else {
            None
        };
        input.parse::<Token![;]>()?;

        Ok(Self {
            attributes,
            name,
            options,
        })
    }
}

struct RawOption {
    name: Ident,
    value: Expr,
}

impl Parse for RawOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name = input.parse()?;
        input.parse::<Token![=]>()?;
        let value = input.parse()?;

        Ok(Self { name, value })
    }
}

fn validate_declaration(
    raw: RawEndpointDeclaration,
) -> Result<EndpointDeclaration, EndpointDeclarationError> {
    let name = EndpointName::parse(&raw.name)?;
    validate_attributes(name, raw.attributes.as_slice())?;

    let kind = match name {
        EndpointName::SqlQuery => EndpointDeclarationKind::SqlQuery {
            introspection: parse_bool_option(
                name,
                &required_option(&raw, name, "introspection")?,
                "introspection",
            )?,
        },
        EndpointName::Ddl => {
            reject_options(&raw, name)?;
            EndpointDeclarationKind::Ddl
        }
        EndpointName::Update => EndpointDeclarationKind::Update {
            admission: parse_update_admission(name, &required_option(&raw, name, "admission")?)?,
        },
        EndpointName::Integrity => {
            reject_options(&raw, name)?;
            EndpointDeclarationKind::Integrity
        }
        EndpointName::FixturesReset => {
            reject_options(&raw, name)?;
            EndpointDeclarationKind::FixturesReset
        }
        EndpointName::FixturesLoad => EndpointDeclarationKind::FixturesLoad {
            handler: parse_handler_path(name, &required_option(&raw, name, "handler")?)?,
        },
        EndpointName::Metrics => EndpointDeclarationKind::Metrics {
            authorization: parse_authorization(
                name,
                &required_option(&raw, name, "authorization")?,
            )?,
        },
        EndpointName::MetricsExtended => EndpointDeclarationKind::MetricsExtended {
            authorization: parse_authorization(
                name,
                &required_option(&raw, name, "authorization")?,
            )?,
        },
        EndpointName::MetricsReset => {
            reject_options(&raw, name)?;
            EndpointDeclarationKind::MetricsReset
        }
        EndpointName::Snapshot => {
            reject_options(&raw, name)?;
            EndpointDeclarationKind::Snapshot
        }
        EndpointName::Schema => EndpointDeclarationKind::Schema {
            authorization: parse_authorization(
                name,
                &required_option(&raw, name, "authorization")?,
            )?,
        },
    };

    Ok(EndpointDeclaration {
        cfg_attributes: raw.attributes,
        kind,
    })
}

fn validate_attributes(
    endpoint: EndpointName,
    attributes: &[Attribute],
) -> Result<(), EndpointDeclarationError> {
    for attribute in attributes {
        if !attribute.path().is_ident("cfg") {
            return Err(EndpointDeclarationError::UnsupportedAttribute {
                endpoint,
                attribute: attribute.path().to_token_stream().to_string(),
            });
        }
    }

    Ok(())
}

const fn reject_options(
    raw: &RawEndpointDeclaration,
    endpoint: EndpointName,
) -> Result<(), EndpointDeclarationError> {
    if raw.options.is_some() {
        return Err(EndpointDeclarationError::OptionsNotAccepted { endpoint });
    }

    Ok(())
}

fn required_option(
    raw: &RawEndpointDeclaration,
    endpoint: EndpointName,
    expected: &'static str,
) -> Result<Expr, EndpointDeclarationError> {
    let Some(tokens) = raw.options.clone() else {
        return Err(EndpointDeclarationError::MissingOptions { endpoint });
    };
    let options = Punctuated::<RawOption, Token![,]>::parse_terminated
        .parse2(tokens)
        .map_err(syntax_error)?;
    let mut value = None;

    for option in options {
        let option_name = option.name.to_string();
        if option_name != expected {
            return Err(EndpointDeclarationError::UnknownOption {
                endpoint,
                option: option_name,
            });
        }
        if value.replace(option.value).is_some() {
            return Err(EndpointDeclarationError::DuplicateOption {
                endpoint,
                option: option_name,
            });
        }
    }

    value.ok_or(EndpointDeclarationError::MissingOption {
        endpoint,
        option: expected,
    })
}

fn parse_bool_option(
    endpoint: EndpointName,
    value: &Expr,
    option: &'static str,
) -> Result<bool, EndpointDeclarationError> {
    if let Expr::Lit(ExprLit {
        lit: Lit::Bool(value),
        ..
    }) = value
    {
        return Ok(value.value());
    }

    Err(invalid_option_value(
        endpoint,
        option,
        "the literal `true` or `false`",
        value,
    ))
}

fn parse_update_admission(
    endpoint: EndpointName,
    value: &Expr,
) -> Result<EndpointUpdateAdmission, EndpointDeclarationError> {
    match single_ident(value).as_deref() {
        Some("primary_key_only") => Ok(EndpointUpdateAdmission::PrimaryKeyOnly),
        Some("bounded_deterministic") => Ok(EndpointUpdateAdmission::BoundedDeterministic),
        _ => Err(invalid_option_value(
            endpoint,
            "admission",
            "`primary_key_only` or `bounded_deterministic`",
            value,
        )),
    }
}

fn parse_authorization(
    endpoint: EndpointName,
    value: &Expr,
) -> Result<EndpointAuthorization, EndpointDeclarationError> {
    match single_ident(value).as_deref() {
        Some("public") => Ok(EndpointAuthorization::Public),
        Some("controller") => Ok(EndpointAuthorization::Controller),
        _ => Err(invalid_option_value(
            endpoint,
            "authorization",
            "`public` or `controller`",
            value,
        )),
    }
}

fn parse_handler_path(
    endpoint: EndpointName,
    value: &Expr,
) -> Result<Path, EndpointDeclarationError> {
    if let Expr::Path(ExprPath {
        attrs,
        qself: None,
        path,
    }) = value
        && attrs.is_empty()
    {
        return Ok(path.clone());
    }

    Err(invalid_option_value(
        endpoint,
        "handler",
        "a Rust function path",
        value,
    ))
}

fn single_ident(value: &Expr) -> Option<String> {
    let Expr::Path(ExprPath {
        attrs,
        qself: None,
        path,
    }) = value
    else {
        return None;
    };
    if !attrs.is_empty() || path.leading_colon.is_some() || path.segments.len() != 1 {
        return None;
    }
    let segment = path.segments.first()?;
    if !matches!(segment.arguments, syn::PathArguments::None) {
        return None;
    }

    Some(segment.ident.to_string())
}

fn invalid_option_value(
    endpoint: EndpointName,
    option: &'static str,
    expected: &'static str,
    actual: &Expr,
) -> EndpointDeclarationError {
    EndpointDeclarationError::InvalidOptionValue {
        endpoint,
        option,
        expected,
        actual: actual.to_token_stream().to_string(),
    }
}

fn syntax_error(error: syn::Error) -> EndpointDeclarationError {
    EndpointDeclarationError::Syntax {
        message: error.to_string(),
    }
}

fn emit_endpoint_wrapper(declaration: &EndpointDeclaration) -> TokenStream {
    let attributes = &declaration.cfg_attributes;
    let authorization = emit_result_authorization(declaration.kind.authorization_plan());
    let wrapper = match &declaration.kind {
        EndpointDeclarationKind::SqlQuery { introspection } => {
            emit_sql_query_wrapper(*introspection, &authorization)
        }
        EndpointDeclarationKind::Ddl => emit_ddl_wrapper(&authorization),
        EndpointDeclarationKind::Update { admission } => {
            emit_update_wrapper(*admission, &authorization)
        }
        EndpointDeclarationKind::Integrity => emit_integrity_wrapper(),
        EndpointDeclarationKind::FixturesReset => emit_fixtures_reset_wrapper(&authorization),
        EndpointDeclarationKind::FixturesLoad { handler } => {
            emit_fixtures_load_wrapper(handler, &authorization)
        }
        EndpointDeclarationKind::Metrics { .. } => emit_metrics_wrapper(false, &authorization),
        EndpointDeclarationKind::MetricsExtended { .. } => {
            emit_metrics_wrapper(true, &authorization)
        }
        EndpointDeclarationKind::MetricsReset => emit_metrics_reset_wrapper(&authorization),
        EndpointDeclarationKind::Snapshot => emit_snapshot_wrapper(&authorization),
        EndpointDeclarationKind::Schema { .. } => emit_schema_wrapper(&authorization),
    };

    quote! {
        #(#attributes)*
        #wrapper
    }
}

fn emit_sql_query_wrapper(introspection: bool, authorization: &TokenStream) -> TokenStream {
    let introspection = if introspection {
        quote!(true)
    } else {
        quote!(false)
    };

    quote! {
        #[::icydb::__reexports::ic_cdk::query(name = "icydb_query")]
        fn __icydb_export_icydb_query(
            sql: String,
        ) -> Result<::icydb::db::sql::SqlQueryPerfResult, ::icydb::Error> {
            #authorization
            crate::__icydb_generated::endpoint_handlers::sql_query::<#introspection>(sql)
        }
    }
}

fn emit_ddl_wrapper(authorization: &TokenStream) -> TokenStream {
    quote! {
        #[::icydb::__reexports::ic_cdk::update(name = "icydb_ddl")]
        fn __icydb_export_icydb_ddl(
            sql: String,
        ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
            #authorization
            crate::__icydb_generated::endpoint_handlers::sql_ddl(sql)
        }
    }
}

fn emit_update_wrapper(
    admission: EndpointUpdateAdmission,
    authorization: &TokenStream,
) -> TokenStream {
    let handler = match admission {
        EndpointUpdateAdmission::PrimaryKeyOnly => quote! {
            crate::__icydb_generated::endpoint_handlers::sql_update_primary_key(sql)
        },
        EndpointUpdateAdmission::BoundedDeterministic => quote! {
            crate::__icydb_generated::endpoint_handlers::sql_update_bounded(sql)
        },
    };

    quote! {
        #[::icydb::__reexports::ic_cdk::update(name = "icydb_update")]
        fn __icydb_export_icydb_update(
            sql: String,
        ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
            #authorization
            #handler
        }
    }
}

fn emit_integrity_wrapper() -> TokenStream {
    quote! {
        #[allow(clippy::result_large_err)]
        #[::icydb::__reexports::ic_cdk::update(name = "icydb_integrity")]
        fn __icydb_export_icydb_integrity(
            sql: String,
        ) -> Result<::icydb::db::IntegrityCheckResult, ::icydb::db::SqlIntegrityError> {
            crate::__icydb_generated::endpoint_authorization::require_sql_controller()
                .map_err(::icydb::db::SqlIntegrityError::Sql)?;
            crate::__icydb_generated::endpoint_handlers::sql_integrity(sql)
        }
    }
}

fn emit_fixtures_reset_wrapper(authorization: &TokenStream) -> TokenStream {
    quote! {
        #[::icydb::__reexports::ic_cdk::update(name = "icydb_fixtures_reset")]
        fn __icydb_export_icydb_fixtures_reset() -> Result<(), ::icydb::Error> {
            #authorization
            crate::__icydb_generated::endpoint_handlers::fixtures_reset()
        }
    }
}

fn emit_fixtures_load_wrapper(handler: &Path, authorization: &TokenStream) -> TokenStream {
    quote! {
        #[::icydb::__reexports::ic_cdk::update(name = "icydb_fixtures_load")]
        fn __icydb_export_icydb_fixtures_load() -> Result<(), ::icydb::Error> {
            #authorization
            let handler: fn() -> Result<(), ::icydb::Error> = #handler;
            crate::__icydb_generated::endpoint_handlers::fixtures_load(handler)
        }
    }
}

fn emit_metrics_wrapper(extended: bool, authorization: &TokenStream) -> TokenStream {
    if extended {
        quote! {
            #[::icydb::__reexports::ic_cdk::query(name = "icydb_metrics_extended")]
            fn __icydb_export_icydb_metrics_extended(
                window_start_ms: Option<u64>,
            ) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
                #authorization
                crate::__icydb_generated::endpoint_handlers::metrics_extended(window_start_ms)
            }
        }
    } else {
        quote! {
            #[::icydb::__reexports::ic_cdk::query(name = "icydb_metrics")]
            fn __icydb_export_icydb_metrics(
                window_start_ms: Option<u64>,
            ) -> Result<::icydb::metrics::CompactMetricsReport, ::icydb::Error> {
                #authorization
                crate::__icydb_generated::endpoint_handlers::metrics(window_start_ms)
            }
        }
    }
}

fn emit_metrics_reset_wrapper(authorization: &TokenStream) -> TokenStream {
    quote! {
        #[::icydb::__reexports::ic_cdk::update(name = "icydb_metrics_reset")]
        fn __icydb_export_icydb_metrics_reset() -> Result<(), ::icydb::Error> {
            #authorization
            crate::__icydb_generated::endpoint_handlers::metrics_reset()
        }
    }
}

fn emit_snapshot_wrapper(authorization: &TokenStream) -> TokenStream {
    quote! {
        #[::icydb::__reexports::ic_cdk::query(name = "icydb_snapshot")]
        fn __icydb_export_icydb_snapshot() -> Result<::icydb::db::StorageReport, ::icydb::Error> {
            #authorization
            crate::__icydb_generated::endpoint_handlers::snapshot()
        }
    }
}

fn emit_schema_wrapper(authorization: &TokenStream) -> TokenStream {
    quote! {
        #[::icydb::__reexports::ic_cdk::query(name = "icydb_schema")]
        fn __icydb_export_icydb_schema(
        ) -> Result<Vec<::icydb::db::EntitySchemaDescription>, ::icydb::Error> {
            #authorization
            crate::__icydb_generated::endpoint_handlers::schema()
        }
    }
}

fn emit_result_authorization(authorization: EndpointAuthorizationPlan) -> TokenStream {
    match authorization {
        EndpointAuthorizationPlan::Public => TokenStream::new(),
        EndpointAuthorizationPlan::SqlController => quote! {
            crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
        },
        EndpointAuthorizationPlan::OperationalController => quote! {
            crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
        },
        EndpointAuthorizationPlan::SchemaController => quote! {
            crate::__icydb_generated::endpoint_authorization::require_schema_controller()?;
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use proc_macro2::TokenStream;
    use quote::quote;

    use super::{
        EndpointAuthorizationPlan, EndpointDeclarationError, EndpointDeclarationKind, EndpointName,
        compile_endpoint_declarations, emit_endpoint_authorization_helpers,
        parse_endpoint_declarations,
    };

    fn compact_tokens(tokens: TokenStream) -> String {
        tokens
            .to_string()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect()
    }

    fn accepted_declarations() -> TokenStream {
        quote! {
            #[cfg(feature = "local-sql-query")]
            icydb_sql_query(introspection = true);
            icydb_ddl;
            icydb_update(admission = primary_key_only);
            icydb_integrity;
            icydb_fixtures_reset;
            icydb_fixtures_load(handler = crate::load_fixtures);
            icydb_metrics(authorization = public);
            #[cfg(feature = "local-extended-metrics")]
            icydb_metrics_extended(authorization = controller);
            icydb_metrics_reset;
            icydb_snapshot;
            icydb_schema(authorization = controller);
        }
    }

    fn declaration_kind(tokens: TokenStream) -> EndpointDeclarationKind {
        let mut declarations = parse_endpoint_declarations(tokens)
            .expect("single accepted declaration should parse")
            .0;
        assert_eq!(declarations.len(), 1);

        declarations
            .pop()
            .expect("single accepted declaration should remain")
            .kind
    }

    fn assert_capabilities(tokens: TokenStream, expected: &[&str]) {
        let kind = declaration_kind(tokens);
        let observed = kind
            .capability_plan()
            .features()
            .iter()
            .map(|capability| capability.feature())
            .collect::<Vec<_>>();

        assert_eq!(observed, expected);
    }

    #[test]
    fn accepted_declarations_emit_the_exact_wrapper_snapshot() {
        let observed = compile_endpoint_declarations(accepted_declarations())
            .expect("accepted declaration block should compile");
        let expected = quote! {
            #[cfg(feature = "local-sql-query")]
            #[::icydb::__reexports::ic_cdk::query(name = "icydb_query")]
            fn __icydb_export_icydb_query(
                sql: String,
            ) -> Result<::icydb::db::sql::SqlQueryPerfResult, ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                crate::__icydb_generated::endpoint_handlers::sql_query::<true>(sql)
            }

            #[::icydb::__reexports::ic_cdk::update(name = "icydb_ddl")]
            fn __icydb_export_icydb_ddl(
                sql: String,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                crate::__icydb_generated::endpoint_handlers::sql_ddl(sql)
            }

            #[::icydb::__reexports::ic_cdk::update(name = "icydb_update")]
            fn __icydb_export_icydb_update(
                sql: String,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                crate::__icydb_generated::endpoint_handlers::sql_update_primary_key(sql)
            }

            #[allow(clippy::result_large_err)]
            #[::icydb::__reexports::ic_cdk::update(name = "icydb_integrity")]
            fn __icydb_export_icydb_integrity(
                sql: String,
            ) -> Result<::icydb::db::IntegrityCheckResult, ::icydb::db::SqlIntegrityError> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()
                    .map_err(::icydb::db::SqlIntegrityError::Sql)?;
                crate::__icydb_generated::endpoint_handlers::sql_integrity(sql)
            }

            #[::icydb::__reexports::ic_cdk::update(name = "icydb_fixtures_reset")]
            fn __icydb_export_icydb_fixtures_reset() -> Result<(), ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                crate::__icydb_generated::endpoint_handlers::fixtures_reset()
            }

            #[::icydb::__reexports::ic_cdk::update(name = "icydb_fixtures_load")]
            fn __icydb_export_icydb_fixtures_load() -> Result<(), ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_sql_controller()?;
                let handler: fn() -> Result<(), ::icydb::Error> = crate::load_fixtures;
                crate::__icydb_generated::endpoint_handlers::fixtures_load(handler)
            }

            #[::icydb::__reexports::ic_cdk::query(name = "icydb_metrics")]
            fn __icydb_export_icydb_metrics(
                window_start_ms: Option<u64>,
            ) -> Result<::icydb::metrics::CompactMetricsReport, ::icydb::Error> {
                crate::__icydb_generated::endpoint_handlers::metrics(window_start_ms)
            }

            #[cfg(feature = "local-extended-metrics")]
            #[::icydb::__reexports::ic_cdk::query(name = "icydb_metrics_extended")]
            fn __icydb_export_icydb_metrics_extended(
                window_start_ms: Option<u64>,
            ) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
                crate::__icydb_generated::endpoint_handlers::metrics_extended(window_start_ms)
            }

            #[::icydb::__reexports::ic_cdk::update(name = "icydb_metrics_reset")]
            fn __icydb_export_icydb_metrics_reset() -> Result<(), ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
                crate::__icydb_generated::endpoint_handlers::metrics_reset()
            }

            #[::icydb::__reexports::ic_cdk::query(name = "icydb_snapshot")]
            fn __icydb_export_icydb_snapshot(
            ) -> Result<::icydb::db::StorageReport, ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_operational_controller()?;
                crate::__icydb_generated::endpoint_handlers::snapshot()
            }

            #[::icydb::__reexports::ic_cdk::query(name = "icydb_schema")]
            fn __icydb_export_icydb_schema(
            ) -> Result<Vec<::icydb::db::EntitySchemaDescription>, ::icydb::Error> {
                crate::__icydb_generated::endpoint_authorization::require_schema_controller()?;
                crate::__icydb_generated::endpoint_handlers::schema()
            }
        };

        assert_eq!(compact_tokens(observed), compact_tokens(expected));
    }

    #[test]
    fn each_declaration_emits_exactly_one_fixed_method() {
        let tokens = compile_endpoint_declarations(accepted_declarations())
            .expect("accepted declaration block should compile");
        let file = syn::parse2::<syn::File>(tokens)
            .expect("emitted wrappers should remain valid Rust syntax");
        let wrappers = file
            .items
            .into_iter()
            .filter_map(|item| match item {
                syn::Item::Fn(function) => Some(function),
                _ => None,
            })
            .collect::<Vec<_>>();
        let names = wrappers
            .iter()
            .map(|wrapper| wrapper.sig.ident.to_string())
            .collect::<BTreeSet<_>>();
        let expected_names = BTreeSet::from([
            "__icydb_export_icydb_ddl".to_string(),
            "__icydb_export_icydb_fixtures_load".to_string(),
            "__icydb_export_icydb_fixtures_reset".to_string(),
            "__icydb_export_icydb_integrity".to_string(),
            "__icydb_export_icydb_metrics".to_string(),
            "__icydb_export_icydb_metrics_extended".to_string(),
            "__icydb_export_icydb_metrics_reset".to_string(),
            "__icydb_export_icydb_query".to_string(),
            "__icydb_export_icydb_schema".to_string(),
            "__icydb_export_icydb_snapshot".to_string(),
            "__icydb_export_icydb_update".to_string(),
        ]);

        assert_eq!(wrappers.len(), 11);
        assert_eq!(names, expected_names);
        for wrapper in wrappers {
            let method_attributes = wrapper
                .attrs
                .iter()
                .filter(|attribute| {
                    attribute.path().segments.last().is_some_and(|segment| {
                        matches!(segment.ident.to_string().as_str(), "query" | "update")
                    })
                })
                .count();
            assert_eq!(method_attributes, 1);
        }
    }

    #[test]
    fn alternative_closed_options_select_only_their_exact_handlers() {
        let observed = compile_endpoint_declarations(quote! {
            icydb_sql_query(introspection = false);
            icydb_update(admission = bounded_deterministic);
            icydb_metrics(authorization = controller);
            icydb_schema(authorization = public);
        })
        .expect("alternative closed options should compile");
        let observed = compact_tokens(observed);

        assert!(observed.contains("endpoint_handlers::sql_query::<false>(sql)"));
        assert!(observed.contains("endpoint_handlers::sql_update_bounded(sql)"));
        assert!(!observed.contains("sql_update_primary_key"));
        assert_eq!(
            observed
                .matches("require_operational_controller()?")
                .count(),
            1,
        );
        assert!(!observed.contains("require_schema_controller"));
    }

    #[test]
    fn sql_and_fixture_capability_conjunctions_are_exact() {
        assert_capabilities(
            quote!(icydb_sql_query(introspection = false);),
            &["icydb/sql"],
        );
        assert_capabilities(
            quote!(icydb_sql_query(introspection = true);),
            &["icydb/sql", "icydb/sql-explain"],
        );
        assert_capabilities(quote!(icydb_ddl;), &["icydb/sql"]);
        assert_capabilities(
            quote!(icydb_update(admission = primary_key_only);),
            &["icydb/sql"],
        );
        assert_capabilities(quote!(icydb_integrity;), &["icydb/sql"]);
        assert_capabilities(
            quote!(icydb_fixtures_reset;),
            &["icydb/sql", "test-admin-api"],
        );
        assert_capabilities(
            quote!(icydb_fixtures_load(handler = load_fixtures);),
            &["icydb/sql", "test-admin-api"],
        );
    }

    #[test]
    fn report_capability_conjunctions_are_exact() {
        assert_capabilities(quote!(icydb_metrics(authorization = public);), &[]);
        assert_capabilities(
            quote!(icydb_metrics_extended(authorization = public);),
            &["icydb/metrics-extended"],
        );
        assert_capabilities(quote!(icydb_metrics_reset;), &[]);
        assert_capabilities(quote!(icydb_snapshot;), &[]);
        assert_capabilities(quote!(icydb_schema(authorization = public);), &[]);
    }

    #[test]
    fn authorization_plans_are_endpoint_specific_and_closed() {
        for input in [
            quote!(icydb_sql_query(introspection = false);),
            quote!(icydb_ddl;),
            quote!(icydb_update(admission = bounded_deterministic);),
            quote!(icydb_integrity;),
            quote!(icydb_fixtures_reset;),
            quote!(icydb_fixtures_load(handler = load_fixtures);),
        ] {
            assert_eq!(
                declaration_kind(input).authorization_plan(),
                EndpointAuthorizationPlan::SqlController,
            );
        }
        for input in [quote!(icydb_metrics_reset;), quote!(icydb_snapshot;)] {
            assert_eq!(
                declaration_kind(input).authorization_plan(),
                EndpointAuthorizationPlan::OperationalController,
            );
        }
        assert_eq!(
            declaration_kind(quote!(icydb_metrics(authorization = public);)).authorization_plan(),
            EndpointAuthorizationPlan::Public,
        );
        assert_eq!(
            declaration_kind(quote!(icydb_metrics_extended(authorization = controller);))
                .authorization_plan(),
            EndpointAuthorizationPlan::OperationalController,
        );
        assert_eq!(
            declaration_kind(quote!(icydb_schema(authorization = controller);))
                .authorization_plan(),
            EndpointAuthorizationPlan::SchemaController,
        );
    }

    #[test]
    fn private_authorization_helpers_map_exact_typed_failures() {
        let helpers = compact_tokens(emit_endpoint_authorization_helpers());

        assert!(helpers.contains("RuntimeBoundaryCode::SqlSurfaceControllerRequired"));
        assert!(helpers.contains("RuntimeBoundaryCode::OperationalSurfaceControllerRequired"),);
        assert!(helpers.contains("RuntimeBoundaryCode::SchemaSurfaceControllerRequired"));
        assert_eq!(helpers.matches("ErrorOrigin::Interface").count(), 1);
        assert_eq!(helpers.matches("is_controller(&caller)").count(), 1);
        for forbidden in ["::query", "::update", "export_name", "no_mangle"] {
            assert!(!helpers.contains(forbidden));
        }
    }

    #[test]
    fn fixture_load_emits_one_exact_function_item_coercion() {
        let wrapper = compact_tokens(
            compile_endpoint_declarations(quote! {
                icydb_fixtures_load(handler = crate::fixtures::load);
            })
            .expect("fixture declaration should compile"),
        );

        assert_eq!(
            wrapper
                .matches("lethandler:fn()->Result<(),::icydb::Error>=crate::fixtures::load;")
                .count(),
            1,
        );
        assert_eq!(
            wrapper
                .matches("endpoint_handlers::fixtures_load(handler)")
                .count(),
            1,
        );
    }

    #[test]
    fn unknown_and_duplicate_endpoint_failures_are_typed() {
        assert!(matches!(
            parse_endpoint_declarations(quote!(icydb_everything;)),
            Err(EndpointDeclarationError::UnknownEndpoint { .. })
        ));
        assert_eq!(
            parse_endpoint_declarations(quote! {
                icydb_snapshot;
                #[cfg(feature = "other")]
                icydb_snapshot;
            })
            .err(),
            Some(EndpointDeclarationError::DuplicateEndpoint {
                endpoint: EndpointName::Snapshot,
            }),
        );
    }

    #[test]
    fn unsupported_attributes_and_option_shapes_fail_with_distinct_types() {
        assert!(matches!(
            parse_endpoint_declarations(quote! {
                #[cfg_attr(feature = "local", allow(dead_code))]
                icydb_snapshot;
            }),
            Err(EndpointDeclarationError::UnsupportedAttribute { .. })
        ));
        assert_eq!(
            parse_endpoint_declarations(quote!(icydb_snapshot();)).err(),
            Some(EndpointDeclarationError::OptionsNotAccepted {
                endpoint: EndpointName::Snapshot,
            }),
        );
        assert!(matches!(
            parse_endpoint_declarations(quote! {
                icydb_metrics(visibility = public);
            }),
            Err(EndpointDeclarationError::UnknownOption { .. })
        ));
        assert!(matches!(
            parse_endpoint_declarations(quote! {
                icydb_metrics(
                    authorization = public,
                    authorization = controller,
                );
            }),
            Err(EndpointDeclarationError::DuplicateOption { .. })
        ));
    }

    #[test]
    fn required_options_and_values_remain_closed() {
        assert_eq!(
            parse_endpoint_declarations(quote!(icydb_update;)).err(),
            Some(EndpointDeclarationError::MissingOptions {
                endpoint: EndpointName::Update,
            }),
        );
        assert!(matches!(
            parse_endpoint_declarations(quote!(icydb_update();)),
            Err(EndpointDeclarationError::MissingOption { .. })
        ));
        for input in [
            quote!(icydb_sql_query(introspection = enabled);),
            quote!(icydb_update(admission = unrestricted);),
            quote!(icydb_metrics(authorization = application);),
            quote!(icydb_fixtures_load(handler = load_fixtures());),
        ] {
            assert!(matches!(
                parse_endpoint_declarations(input),
                Err(EndpointDeclarationError::InvalidOptionValue { .. })
            ));
        }
    }

    #[test]
    fn malformed_declaration_syntax_stays_distinct_from_vocabulary_rejection() {
        assert!(matches!(
            parse_endpoint_declarations(quote!(icydb_snapshot)),
            Err(EndpointDeclarationError::Syntax { .. })
        ));
    }
}
