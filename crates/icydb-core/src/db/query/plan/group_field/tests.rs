use crate::db::tests::support::assert_budget_resource;
use crate::db::{
    query::plan::{FieldSlot, GroupField, GroupFieldSet},
    schema::AcceptedFieldKind,
};

fn rank_path() -> GroupField {
    GroupField::scalar_path_for_test(
        "profile.rank",
        "profile",
        vec!["rank".to_string()],
        1,
        &AcceptedFieldKind::Int32,
    )
}

#[test]
fn direct_keys_keep_the_direct_representation() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let mut fields = GroupFieldSet::empty();
        fields
            .push(
                GroupField::Direct(FieldSlot::from_test_accepted_kind(
                    0,
                    "direct_rank",
                    AcceptedFieldKind::Int32,
                )),
                work,
            )
            .unwrap();

        assert_eq!(fields.as_direct().map(<[FieldSlot]>::len), Some(1));
        assert!(fields.as_path_aware().is_none());
    });
}

#[test]
fn first_path_promotes_the_whole_tuple_once_and_preserves_order() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let mut fields = GroupFieldSet::empty();
        fields
            .push(
                GroupField::Direct(FieldSlot::from_test_accepted_kind(
                    0,
                    "direct_rank",
                    AcceptedFieldKind::Int32,
                )),
                work,
            )
            .unwrap();
        fields.push(rank_path(), work).unwrap();
        fields.push(rank_path(), work).unwrap();

        assert!(fields.as_direct().is_none());
        assert_eq!(
            fields.iter().map(|field| field.field()).collect::<Vec<_>>(),
            vec!["direct_rank", "profile.rank"],
        );
    });
}

#[test]
fn preallocated_path_tuple_matches_incremental_identity_without_promotion() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let direct = GroupField::Direct(FieldSlot::from_test_accepted_kind(
            0,
            "direct_rank",
            AcceptedFieldKind::Int32,
        ));
        let mut expected = GroupFieldSet::empty();
        let mut reserved = GroupFieldSet::PathAware(Vec::with_capacity(4));
        for key in [direct.clone(), rank_path(), direct, rank_path()] {
            expected.push(key.clone(), work).unwrap();
            reserved.push(key, work).unwrap();
        }
        assert_eq!(reserved, expected);
        let GroupFieldSet::PathAware(fields) = reserved else {
            panic!("path-aware group tuple");
        };
        assert_eq!(fields.capacity(), 4);
        assert_eq!(fields.len(), 2);
    });
}

#[test]
fn rejected_group_key_insertion_preserves_the_existing_tuple() {
    use crate::db::{
        RequestExecutionRoot,
        query::preparation::PreparationWork,
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
    };

    let direct = FieldSlot::from_test_accepted_kind(0, "rank", AcceptedFieldKind::Int32);
    for (resource, incoming) in [
        (Resource::TemporaryBytes, rank_path()),
        (
            Resource::PredicateExpressionSteps,
            GroupField::Direct(direct.clone()),
        ),
    ] {
        let mut fields = GroupFieldSet::Direct(vec![direct.clone()]);
        let before = fields.clone();
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(resource, 0),
        );
        for _ in 0..2 {
            let error =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    fields.push(incoming.clone(), work)
                })
                .unwrap_err();
            assert_budget_resource(&error.diagnostic_facts(), resource);
            assert_eq!(fields, before);
        }
        crate::db::query::preparation::with_preparation_work(|work| fields.push(incoming, work))
            .unwrap();
    }
}
