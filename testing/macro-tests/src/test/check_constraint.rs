//! Generated named-check proposal coverage.

#[cfg(test)]
use crate::prelude::*;

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::db::{CompareOp, Predicate};

    #[enum_(
        source_key = "testing/macro-tests/src/test/check_constraint.rs::enum_::nested::1",
        variant(source_key = "Bronze", ident = "Bronze"),
        variant(source_key = "Silver", ident = "Silver"),
        variant(source_key = "Gold", ident = "Gold")
    )]
    pub struct GeneratedCheckTier {}

    #[entity(source_key = "testing/macro-tests/src/test/check_constraint.rs::entity::nested::1",
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        constraint(source_key = "balance_nonnegative", name = "balance_nonnegative", check = "balance >= 0"),
        fields(
            field(source_key = "id", ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(source_key = "balance", ident = "balance", value(item(prim = "Int64")))
        )
    )]
    pub struct GeneratedCheckHarness {}

    #[entity(source_key = "testing/macro-tests/src/test/check_constraint.rs::entity::nested::2",
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        constraint(source_key = "active_tier", name = "active_tier",
            check = "tier IN ('Bronze', 'Silver', 'Gold')"
        ),
        fields(
            field(source_key = "id", ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(source_key = "tier", ident = "tier", value(item(is = "GeneratedCheckTier")))
        )
    )]
    pub struct GeneratedEnumCheckHarness {}

    #[test]
    fn generated_check_is_structured_proposal_metadata() {
        let model = <GeneratedCheckHarness as icydb::__macro::EntityDeclaration>::MODEL;
        let [check] = model.check_constraints() else {
            panic!("derive should emit exactly one generated check proposal");
        };

        assert_eq!(check.name(), "balance_nonnegative");
        assert_eq!(check.source_sql(), "balance >= 0");
        let Predicate::Compare(compare) = check.semantics() else {
            panic!("generated check SQL should be parsed before runtime");
        };
        assert_eq!(compare.field(), "balance");
        assert_eq!(compare.op(), CompareOp::Gte);
    }

    #[test]
    fn generated_enum_membership_check_is_structured_proposal_metadata() {
        let model = <GeneratedEnumCheckHarness as icydb::__macro::EntityDeclaration>::MODEL;
        let [check] = model.check_constraints() else {
            panic!("derive should emit exactly one generated enum check proposal");
        };

        let Predicate::Compare(compare) = check.semantics() else {
            panic!("generated enum membership should stay structured before accepted binding");
        };
        assert_eq!(compare.field(), "tier");
        assert_eq!(compare.op(), CompareOp::In);
    }
}
