use icydb_model::canister;

#[canister(
    migrations(
        entity_migration(
            entity = "Account",
            from = 1,
            from_name = "User",
            renames(
                field(from = "email", to = "primary_email"),
                named_type(from = "Status", to = "AccountStatus"),
                variant(named_type = "Status", from = "Active", to = "Enabled"),
                record_field(named_type = "Profile", from = "display", to = "display_name"),
                relation(from = "author", to = "creator"),
                constraint(from = "valid_email", to = "valid_primary_email"),
                rule(named_type = "Rating", from = "range", to = "valid_range")
            ),
            transforms(
                rewrite(from = "age", to = "age", checked_cast(to = "Nat16")),
                rewrite(from = "nickname", to = "display", coalesce(literal(text = "unknown"))),
                rewrite(from = "legacy", to = "current", copy),
                fill(to = "status", literal(enum(named_type = "AccountStatus", variant = "Enabled")))
            )
        )
    ),
    memory_namespace = "test",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 109,
    startup_memory_id = 108
)]
pub struct ApplicationCanister;

fn main() {}
