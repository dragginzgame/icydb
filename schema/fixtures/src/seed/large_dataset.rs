use crate::schema::User;

/// Build a larger fixture set for stress/integration scenarios.
#[must_use]
pub fn large_users(size: usize) -> Vec<User> {
    (0..size)
        .map(|index| User {
            name: format!("user-{index}"),
            age: i32::try_from(index).unwrap_or(i32::MAX),
            ..Default::default()
        })
        .collect()
}
