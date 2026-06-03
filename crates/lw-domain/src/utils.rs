use chrono::{DateTime, Utc};

pub fn timestamp_seconds_to_datetime(ts: u64) -> DateTime<Utc> {
    if ts > 0 {
        return DateTime::from_timestamp(ts as i64, 0).unwrap();
    }
    Utc::now()
}
