use icydb::{
    core::{
        traits::UpdateView,
        view::{ListPatch, MapPatch, SetPatch, Update},
    },
    prelude::*,
};
use std::collections::HashSet;
use test_design::test::merge::{
    MergeEntity, MergeProfile, MergeProfileUpdate, MergeSettings, MergeTags, MergeTuple,
    MergeWrapper, MergeWrapperUpdate,
};

///
/// MergeSuite
///

pub struct MergeSuite;

impl MergeSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> =
            vec![("entity_merge_round_trip", Self::entity_merge_round_trip)];

        for (name, test_fn) in tests {
            println!("Running test: {name}");
            test_fn();
        }
    }

    fn profile(bio: &str, visits: u32) -> MergeProfile {
        MergeProfile {
            bio: bio.into(),
            visits,
            favorite_numbers: vec![1, 2, 3],
        }
    }

    #[allow(clippy::field_reassign_with_default)]
    fn entity_merge_round_trip() {
        let entity = MergeEntity {
            name: "seed".into(),
            score: 1,
            nickname: None,
            scores: vec![10, 20, 30],
            tags: MergeTags::from(vec!["old".to_string(), "stale".to_string()]),
            settings: MergeSettings::from(vec![
                ("keep".to_string(), 1u32),
                ("remove".to_string(), 9u32),
            ]),
            profile: Self::profile("base", 1),
            wrapper: MergeWrapper(Self::profile("wrapper", 2)),
            tuple_field: MergeTuple("tuple".into(), 1),
            opt_profile: None,
            ..Default::default()
        };

        let mut entity = db!().insert(entity).unwrap();

        let mut update: Update<MergeEntity> = Default::default();
        update.nickname = Some(Some("nick".into()));
        update.scores = Some(vec![ListPatch::Update {
            index: 0,
            patch: 99,
        }]);
        update.tags = Some(vec![SetPatch::Clear, SetPatch::Insert("fresh".to_string())]);
        update.settings = Some(vec![
            MapPatch::Upsert {
                key: "keep".to_string(),
                value: 5u32,
            },
            MapPatch::Remove {
                key: "remove".to_string(),
            },
            MapPatch::Upsert {
                key: "extra".to_string(),
                value: 3u32,
            },
        ]);
        update.profile = Some(MergeProfileUpdate {
            visits: Some(10),
            ..Default::default()
        });
        update.wrapper = Some(MergeWrapperUpdate {
            bio: Some("wrapper-updated".into()),
            ..Default::default()
        });
        update.tuple_field = Some((Some("tuple-updated".into()), Some(9)));
        update.opt_profile = Some(Some(MergeProfileUpdate {
            bio: Some("loaded".into()),
            visits: Some(4),
            favorite_numbers: None,
        }));

        entity.merge(update);
        let saved = db!().update(entity).unwrap();
        let key = saved.key();

        let loaded = db!()
            .load::<MergeEntity>()
            .one(key)
            .unwrap()
            .try_entity()
            .unwrap();

        assert_eq!(loaded.nickname.as_deref(), Some("nick"));
        assert_eq!(loaded.scores, vec![99, 20, 30]);

        let tags: HashSet<_> = loaded.tags.iter().cloned().collect();
        let expected_tags: HashSet<_> = vec!["fresh".to_string()].into_iter().collect();
        assert_eq!(tags, expected_tags);

        assert_eq!(loaded.settings.get("keep"), Some(&5));
        assert_eq!(loaded.settings.get("extra"), Some(&3));
        assert!(loaded.settings.get("remove").is_none());

        assert_eq!(loaded.profile.visits, 10);
        assert_eq!(loaded.wrapper.0.bio, "wrapper-updated");
        assert_eq!(loaded.tuple_field.0, "tuple-updated");
        assert_eq!(loaded.tuple_field.1, 9);
        let opt_profile = loaded.opt_profile.expect("profile should be set");
        assert_eq!(opt_profile.bio, "loaded");
        assert_eq!(opt_profile.visits, 4);
        assert!(opt_profile.favorite_numbers.is_empty());
    }
}
