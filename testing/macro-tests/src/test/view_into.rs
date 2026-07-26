#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_test_fixtures::macro_test::view_into::*;

///
/// TESTS
///

#[cfg(test)]
mod test {
    use super::*;

    #[entity(source_key = "testing/macro-tests/src/test/view_into.rs::entity::nested::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(source_key = "id", ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded))),
            field(source_key = "score", ident = "score", value(item(prim = "Nat32"))),
            field(source_key = "tags", ident = "tags", value(many, item(prim = "Text", unbounded))),
            field(source_key = "nickname", ident = "nickname", value(opt, item(prim = "Text", unbounded)))
        )
    )]
    pub struct ViewIntoRoundTripHarness {}

    #[test]
    fn view_into_round_trip() {
        let mut entity = ViewIntoRoundTripHarness {
            id: Ulid::generate(),
            name: "primary".into(),
            score: 42,
            tags: vec!["alpha".into(), "beta".into()],
            nickname: Some("prime".into()),
            created_at: icydb::types::Timestamp::default(),
            updated_at: icydb::types::Timestamp::default(),
        };

        let cloned: ViewIntoRoundTripHarness = entity.clone();
        assert_eq!(cloned.name, "primary");
        assert_eq!(cloned.score, 42);
        assert_eq!(cloned.tags, vec!["alpha".to_string(), "beta".to_string()]);
        assert_eq!(cloned.nickname.as_deref(), Some("prime"));

        entity.name = "updated".into();
        let restored: ViewIntoRoundTripHarness = cloned;
        assert_eq!(restored.name, "primary");
        assert_eq!(restored.score, 42);
        assert_eq!(restored.tags, vec!["alpha".to_string(), "beta".to_string()]);
        assert_eq!(restored.nickname.as_deref(), Some("prime"));
    }
}
