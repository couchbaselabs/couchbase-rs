use chrono::{DateTime, NaiveDateTime, Utc};
use std::time::Duration;

pub mod kv;
pub mod query;
pub mod subdoc;

pub(crate) fn assert_timestamp(
    start: DateTime<Utc>,
    duration: Duration,
    expiry_timestamp: &NaiveDateTime,
    delta: Duration,
) {
    // Increase duration as needed
    let increased_duration = duration + Duration::from_secs(10);

    // Adjust delta as needed
    let increased_delta = delta + Duration::from_secs(5);

    let expires_since_start =
        DateTime::<Utc>::from_utc(expiry_timestamp.clone(), Utc).signed_duration_since(start);
    let chrono_duration = chrono::Duration::from_std(duration).unwrap();
    assert!(
        expires_since_start < chrono_duration,
        "{} should be less than {}",
        expires_since_start,
        chrono_duration
    );
    let min_chrono_duration = chrono::Duration::from_std(duration - delta).unwrap();
    assert!(
        expires_since_start > min_chrono_duration,
        "{} should be greater than {}",
        expires_since_start,
        min_chrono_duration
    );
}
