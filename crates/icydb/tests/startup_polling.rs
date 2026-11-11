//! Exercise the public startup adapter with Testkit-owned simulated progression.
#![cfg(not(target_family = "wasm"))]

use std::time::Duration;

use ic_testkit::pic::PocketIcStartupConfig;
use icydb::{
    db::DatabaseStartupState,
    testing::{TickUntilError, drive_startup},
};
use pocket_ic::PocketIcBuilder;

#[derive(Debug, Eq, PartialEq)]
enum ObservationError {
    Terminal,
}

// Keep the existing request allowance; Testkit separately bounds managed startup.
const REQUEST_TIMEOUT_MS: u64 = 300_000;

#[tokio::test]
#[ignore = "requires Testkit-prepared POCKET_IC_BIN or IC_TESTKIT_POCKET_IC_URL"]
async fn typed_startup_polling_preserves_round_boundaries_and_failures() {
    let instance = PocketIcStartupConfig::from_env(Duration::from_secs(30))
        .unwrap()
        .build_instance(
            PocketIcBuilder::new()
                .with_max_request_time_ms(Some(REQUEST_TIMEOUT_MS))
                .with_application_subnet(),
        )
        .await
        .unwrap();
    let pic = instance.instance();
    let before = pic.get_time().await;
    let mut observations = 0;
    drive_startup(&instance, 0, async || {
        observations += 1;
        Ok::<_, ObservationError>(DatabaseStartupState::Ready)
    })
    .await
    .unwrap();
    assert_eq!(observations, 1);
    assert_eq!(pic.get_time().await, before);

    for rounds in [0, 2] {
        let before = pic.get_time().await;
        let mut observations = 0;
        let result = drive_startup(&instance, rounds, async || {
            observations += 1;
            Ok::<_, ObservationError>(DatabaseStartupState::Recovering)
        })
        .await;
        assert!(
            matches!(result, Err(TickUntilError::ProgressLimit { rounds: actual }) if actual == rounds)
        );
        assert_eq!(observations, rounds + 1);
        assert!(pic.get_time().await >= before + Duration::from_secs(u64::from(rounds)));
    }

    let mut observations = 0;
    drive_startup(&instance, 2, async || {
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
        let result = drive_startup(&instance, 3, async || {
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
    instance.close().await;
}
