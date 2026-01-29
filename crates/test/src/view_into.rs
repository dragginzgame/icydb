use test_design::test::view_into::{ViewIntoRoundTrip, ViewIntoRoundTripView};

///
/// ViewIntoSuite
/// Validates that generated View and  types participate in `Into` conversions.
///

pub struct ViewIntoSuite;

impl ViewIntoSuite {
    pub fn test() {
        Self::view_into_round_trip();
    }

    fn view_into_round_trip() {
        let mut entity = ViewIntoRoundTrip {
            name: "primary".into(),
            score: 42,
            tags: vec!["alpha".into(), "beta".into()],
            nickname: Some("prime".into()),
            ..Default::default()
        };

        let view: ViewIntoRoundTripView = entity.clone().into();
        assert_eq!(view.name, "primary");
        assert_eq!(view.score, 42);
        assert_eq!(view.tags, vec!["alpha", "beta"]);
        assert_eq!(view.nickname.as_deref(), Some("prime"));

        entity.name = "updated".into();
        let from_view: ViewIntoRoundTrip = view.into();
        assert_eq!(from_view.name, "primary");
        assert_eq!(from_view.score, 42);
        assert_eq!(from_view.tags, vec!["alpha", "beta"]);
        assert_eq!(from_view.nickname.as_deref(), Some("prime"));
    }
}
