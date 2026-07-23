//! Generated named-check proposal coverage.

#[cfg(test)]
use crate::prelude::*;

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::db::{CompareOp, Predicate};

    #[enum_(
        variant(ident = "Bronze"),
        variant(ident = "Silver"),
        variant(ident = "Gold")
    )]
    pub struct GeneratedCheckTier {}

    #[entity(
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        constraint(name = "balance_nonnegative", check = "balance >= 0"),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(ident = "balance", value(item(prim = "Int64")))
        )
    )]
    pub struct GeneratedCheckHarness {}

    #[entity(
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        constraint(
            name = "active_tier",
            check = "tier IN ('Bronze', 'Silver', 'Gold')"
        ),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(ident = "tier", value(item(is = "GeneratedCheckTier")))
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
