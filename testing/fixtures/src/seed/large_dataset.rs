use crate::schema::FixtureUser;

/// Build a larger fixture set for stress/integration scenarios.
#[must_use]
pub fn large_users(size: usize) -> Vec<FixtureUser> {
    (0..size)
        .map(|index| FixtureUser {
            name: format!("user-{index}"),
            age: i32::try_from(index).unwrap_or(i32::MAX),
            ..Default::default()
        })
        .collect()
}
