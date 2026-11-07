//! Exercise the public startup adapter with Testkit-owned simulated progression.
#![cfg(not(target_family = "wasm"))]

use std::time::Duration;

use ic_testkit::{
    pic::PocketIcStartupConfig,
    pocket_ic::{ClientError, PocketIcBuilder},
};
use icydb::{
    db::DatabaseStartupState,
    testing::{PocketIc, TickUntilError, drive_startup},
};

#[derive(Debug, Eq, PartialEq)]
enum ObservationError {
    Terminal,
}

// Preserve the previous client allowance; managed startup retains its separate
// thirty-second bound. Testkit owns admission of this explicit finite budget.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

#[tokio::test]
async fn public_testkit_constructors_reject_invalid_request_budgets() {
    let url = "http://127.0.0.1:1/";
    for budget in [Duration::ZERO, Duration::MAX] {
        // No prepared simulator is needed: admission must refuse before any
        // exchange, through both the builder and the publicly re-exported type.
        let results = [
            PocketIcBuilder::new(budget)
                .with_server_url(url.parse().unwrap())
                .with_application_subnet()
                .build()
                .await
                .map(|_| ()),
            PocketIc::new(url.parse().unwrap(), budget)
                .await
                .map(|_| ()),
            PocketIc::new_from_existing_instance(url.parse().unwrap(), 0, budget).map(|_| ()),
            PocketIc::list_instances(url.parse().unwrap(), budget)
                .await
                .map(|_| ()),
        ];
        for result in results {
            assert!(matches!(result,
                Err(ClientError::InvalidRequestBudget { timeout }) if timeout == budget));
        }
    }
}

#[tokio::test]
#[ignore = "requires Testkit-prepared POCKET_IC_BIN or IC_TESTKIT_POCKET_IC_URL"]
async fn typed_startup_polling_preserves_round_boundaries_and_failures() {
    let instance = PocketIcStartupConfig::from_env(Duration::from_secs(30))
        .unwrap()
        .build_instance(PocketIcBuilder::new(REQUEST_TIMEOUT).with_application_subnet())
        .await
        .unwrap();
    let pic = instance.instance();
    let before = pic.get_time().await.unwrap();
    let mut observations = 0;
    drive_startup(pic, 0, async |_| {
        observations += 1;
        Ok::<_, ObservationError>(DatabaseStartupState::Ready)
    })
    .await
    .unwrap();
    assert_eq!(observations, 1);
    assert_eq!(pic.get_time().await.unwrap(), before);

    for rounds in [0, 2] {
        let before = pic.get_time().await.unwrap();
        let mut observations = 0;
        let result = drive_startup(pic, rounds, async |_| {
            observations += 1;
            Ok::<_, ObservationError>(DatabaseStartupState::Recovering)
        })
        .await;
        assert!(
            matches!(result, Err(TickUntilError::ProgressLimit { rounds: actual }) if actual == rounds)
        );
        assert_eq!(observations, rounds + 1);
        assert!(pic.get_time().await.unwrap() >= before + Duration::from_secs(u64::from(rounds)));
    }

    let mut observations = 0;
    drive_startup(pic, 2, async |_| {
        observations += 1;
        Ok::<_, ObservationError>(if observations == 3 {
            DatabaseStartupState::Ready
        } else {
            DatabaseStartupState::Recovering
        })
    })
    .await
    .unwrap();
    assert_eq!(observations, 3);

    // Failure at the first observation and after progress both stop immediately.
    for pending in [0, 1] {
        let mut observations = 0;
        let result = drive_startup(pic, 3, async |_| {
            observations += 1;
            if observations <= pending {
                Ok(DatabaseStartupState::Recovering)
            } else {
                Err(ObservationError::Terminal)
            }
        })
        .await;
        assert!(matches!(
            result,
            Err(TickUntilError::Failed(ObservationError::Terminal))
        ));
        assert_eq!(observations, pending + 1);
    }
    instance.close().await.unwrap();
}
