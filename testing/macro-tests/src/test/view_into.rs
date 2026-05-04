#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_test_fixtures::macro_test::view_into::*;

///
/// TESTS
///

#[cfg(test)]
mod test {
    use super::*;

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(ident = "name", value(item(prim = "Text", unbounded))),
            field(ident = "score", value(item(prim = "Nat32"))),
            field(ident = "tags", value(many, item(prim = "Text", unbounded))),
            field(ident = "nickname", value(opt, item(prim = "Text", unbounded)))
        )
    )]
    pub struct ViewIntoRoundTripHarness {}

    #[test]
    fn view_into_round_trip() {
        let mut entity = ViewIntoRoundTripHarness {
            name: "primary".into(),
            score: 42,
            tags: vec!["alpha".into(), "beta".into()],
            nickname: Some("prime".into()),
            ..Default::default()
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
