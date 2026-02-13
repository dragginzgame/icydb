use crate::prelude::*;

///
/// ViewIntoRoundTrip
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "score", value(item(prim = "Nat32"))),
        field(ident = "tags", value(many, item(prim = "Text"))),
        field(ident = "nickname", value(opt, item(prim = "Text")))
    )
)]
pub struct ViewIntoRoundTrip {}

///
/// TESTS
///

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn view_into_round_trip() {
        let mut entity = ViewIntoRoundTrip {
            name: "primary".into(),
            score: 42,
            tags: vec!["alpha".into(), "beta".into()],
            nickname: Some("prime".into()),
            ..Default::default()
        };

        let view: View<ViewIntoRoundTrip> = entity.clone().into();
        assert_eq!(view.name, "primary");
        assert_eq!(view.score, 42);
        assert_eq!(view.tags, vec!["alpha".to_string(), "beta".to_string()]);
        assert_eq!(view.nickname.as_deref(), Some("prime"));

        entity.name = "updated".into();
        let from_view: ViewIntoRoundTrip = view.into();
        assert_eq!(from_view.name, "primary");
        assert_eq!(from_view.score, 42);
        assert_eq!(
            from_view.tags,
            vec!["alpha".to_string(), "beta".to_string()]
        );
        assert_eq!(from_view.nickname.as_deref(), Some("prime"));
    }
}
