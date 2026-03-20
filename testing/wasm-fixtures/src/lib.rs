//! Shared schema-shape helpers for wasm audit fixtures.

///
/// assert_generated_sql_dispatch_surface_is_stable
///
/// Assert that one generated audit-canister actor surface still uses the
/// shared reduced-SQL runtime descriptor form.
///
pub fn assert_generated_sql_dispatch_surface_is_stable(actor: &str) {
    assert!(
        actor.contains("pub mod sql_dispatch"),
        "generated actor surface must include sql_dispatch module"
    );
    assert!(
        actor.contains("from_statement_route"),
        "generated sql_dispatch must include from_statement_route resolver"
    );
    assert!(
        actor.contains("pub struct SqlLaneTable"),
        "generated sql_dispatch must include one SqlLaneTable function-pointer descriptor"
    );
    assert!(
        actor.contains("pub struct SqlEntityDescriptor"),
        "generated sql_dispatch must include one SqlEntityDescriptor runtime descriptor"
    );
    assert!(
        actor.contains("SQL_ENTITY_DESCRIPTORS"),
        "generated sql_dispatch must include one static descriptor table"
    );
    assert!(
        !actor.contains("enum SqlEntityRoute"),
        "generated sql_dispatch must not regress to enum-based per-entity routing"
    );
    assert!(
        actor.contains("pub fn query ("),
        "generated sql_dispatch must include query convenience entrypoint"
    );
    assert!(
        !actor.contains("pub fn describe_schema ("),
        "generated sql_dispatch must not include removed describe_schema helper"
    );
    assert!(
        !actor.contains("pub fn describe ("),
        "generated sql_dispatch must not include removed describe helper"
    );
    assert!(
        !actor.contains("pub fn show_indexes ("),
        "generated sql_dispatch must not include removed show_indexes helper"
    );
}

///
/// define_simple_audit_entities
///
/// Generate one or more repeated simple audit entities for wasm-size fixtures.
///
#[macro_export]
macro_rules! define_simple_audit_entities {
    ($store:literal; $($entity:ident),+ $(,)?) => {
        $(
            #[doc = ""]
            #[doc = stringify!($entity)]
            #[doc = ""]
            #[doc = "Repeated simple audit entity used to measure base per-entity wasm cost."]
            #[doc = ""]
            #[entity(
                store = $store,
                pk(field = "id"),
                fields(
                    field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
                    field(ident = "name", value(item(prim = "Text")), default = "String::new")
                )
            )]
            pub struct $entity {}
        )+
    };
}

///
/// define_complex_audit_types
///
/// Generate the shared structured helper types used by complex wasm-size audit
/// entities.
///
#[macro_export]
macro_rules! define_complex_audit_types {
    () => {
        #[doc = ""]
        #[doc = "AuditText"]
        #[doc = ""]
        #[doc = "Lower-cased text newtype reused across complex audit entities."]
        #[doc = ""]
        #[newtype(
            primitive = "Text",
            item(prim = "Text"),
            ty(
                sanitizer(path = "base::sanitizer::text::case::Lower"),
                validator(path = "base::validator::text::case::Lower")
            )
        )]
        pub struct AuditText {}

        #[doc = ""]
        #[doc = "AuditCode"]
        #[doc = ""]
        #[doc = "Upper-snake text newtype reused across complex audit entities."]
        #[doc = ""]
        #[newtype(
            primitive = "Text",
            item(prim = "Text"),
            ty(validator(path = "base::validator::text::case::UpperSnake"))
        )]
        pub struct AuditCode {}

        #[doc = ""]
        #[doc = "AuditScoreList"]
        #[doc = ""]
        #[doc = "Bounded numeric list reused across complex audit entities."]
        #[doc = ""]
        #[list(
            item(
                prim = "Nat32",
                validator(path = "base::validator::num::Lte", args(1000))
            ),
            ty(validator(path = "base::validator::len::Max", args(16)))
        )]
        pub struct AuditScoreList {}

        #[doc = ""]
        #[doc = "AuditAliasList"]
        #[doc = ""]
        #[doc = "Normalized alias list reused across complex audit entities."]
        #[doc = ""]
        #[list(item(is = "AuditText"))]
        pub struct AuditAliasList {}

        #[doc = ""]
        #[doc = "AuditTagSet"]
        #[doc = ""]
        #[doc = "Bounded normalized set reused across complex audit entities."]
        #[doc = ""]
        #[set(
            item(is = "AuditText"),
            ty(validator(path = "base::validator::len::Max", args(16)))
        )]
        pub struct AuditTagSet {}

        #[doc = ""]
        #[doc = "AuditAttributeMap"]
        #[doc = ""]
        #[doc = "Bounded map reused across complex audit entities."]
        #[doc = ""]
        #[map(
            key(prim = "Text", validator(path = "base::validator::text::case::Lower")),
            value(item(is = "AuditText")),
            ty(validator(path = "base::validator::len::Max", args(16)))
        )]
        pub struct AuditAttributeMap {}

        #[doc = ""]
        #[doc = "AuditProfile"]
        #[doc = ""]
        #[doc = "Embedded record reused across complex audit entities."]
        #[doc = ""]
        #[record(fields(
            field(ident = "display_name", value(item(prim = "Text"))),
            field(ident = "nickname", value(opt, item(is = "AuditText"))),
            field(ident = "scores", value(item(is = "AuditScoreList"))),
            field(ident = "tags", value(item(is = "AuditTagSet"))),
            field(ident = "attributes", value(item(is = "AuditAttributeMap")))
        ))]
        pub struct AuditProfile {}

        #[doc = ""]
        #[doc = "AuditState"]
        #[doc = ""]
        #[doc = "Payload-bearing enum reused across complex audit entities."]
        #[doc = ""]
        #[enum_(
            variant(ident = "Draft", default),
            variant(ident = "Published", value(item(is = "AuditProfile"))),
            variant(ident = "Archived", value(item(is = "AuditText")))
        )]
        pub struct AuditState {}
    };
}

///
/// define_complex_audit_entities
///
/// Generate one or more repeated complex audit entities for wasm-size
/// fixtures.
///
#[macro_export]
macro_rules! define_complex_audit_entities {
    ($store:literal, $anchor:literal; $($entity:ident),+ $(,)?) => {
        $(
            #[doc = ""]
            #[doc = stringify!($entity)]
            #[doc = ""]
            #[doc = "Repeated complex audit entity used to measure macro-heavy per-entity wasm cost."]
            #[doc = ""]
            #[entity(
                store = $store,
                pk(field = "id"),
                fields(
                    field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
                    field(ident = "slug", value(item(is = "AuditText"))),
                    field(ident = "code", value(item(is = "AuditCode"))),
                    field(ident = "profile", value(item(is = "AuditProfile"))),
                    field(ident = "profile_opt", value(opt, item(is = "AuditProfile"))),
                    field(ident = "state", value(item(is = "AuditState"))),
                    field(ident = "tags", value(item(is = "AuditTagSet"))),
                    field(ident = "scores", value(item(is = "AuditScoreList"))),
                    field(ident = "attributes", value(item(is = "AuditAttributeMap"))),
                    field(ident = "aliases", value(item(is = "AuditAliasList"))),
                    field(
                        ident = "owner_id",
                        value(opt, item(rel = $anchor, prim = "Ulid"))
                    ),
                    field(
                        ident = "sibling_ids",
                        value(many, item(rel = $anchor, prim = "Ulid"))
                    )
                )
            )]
            pub struct $entity {}
        )+
    };
}
