//! Canonical Platform identities through the public facade, outside its modules.

use ic_platform_types::{Account as PlatformAccount, Principal as PlatformPrincipal};
use icydb::{
    ReadAuthorizationContext, ReadAuthorizationDecision, ReadAuthorizationSurface,
    guards::allowlist,
    types::{Account, Principal, Subaccount},
};

#[test]
fn canonical_platform_types_interoperate_with_public_conversions_and_guards() {
    let owner = PlatformPrincipal::from_slice(&[7]);
    let database_owner: Principal = owner.into();
    let restored_owner: PlatformPrincipal = database_owner.into();
    assert_eq!(restored_owner, owner);
    let context = ReadAuthorizationContext {
        caller: owner,
        surface: ReadAuthorizationSurface::Sql,
    };
    assert_eq!(
        allowlist(context, &[owner]),
        ReadAuthorizationDecision::Allow
    );

    let absent = PlatformAccount {
        owner,
        subaccount: None,
    };
    let zero = PlatformAccount {
        owner,
        subaccount: Some([0; 32]),
    };
    let database_absent: Account = absent.into();
    let database_zero: Account = zero.into();
    assert_ne!(database_absent, database_zero);
    assert_eq!(database_absent.subaccount(), None);
    assert_eq!(database_zero.subaccount(), Some(Subaccount::MIN));
    let restored_absent: PlatformAccount = database_absent.into();
    let restored_zero: PlatformAccount = database_zero.into();
    assert_eq!(restored_absent.owner, owner);
    assert_eq!(restored_zero.owner, owner);
    assert_eq!(restored_absent.subaccount, None);
    assert_eq!(restored_zero.subaccount, Some([0; 32]));
}
