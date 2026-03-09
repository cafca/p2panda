use time::format_description::FormatItem;
use time::macros::format_description;
use time::OffsetDateTime;

const HUMAN_READABLE_TIMESTAMP_FORMAT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute] UTC");

pub(crate) fn format_unix_timestamp_secs(timestamp: u64) -> String {
    let Ok(timestamp) = i64::try_from(timestamp) else {
        return "Invalid timestamp".to_owned();
    };

    let Ok(date_time) = OffsetDateTime::from_unix_timestamp(timestamp) else {
        return "Invalid timestamp".to_owned();
    };

    date_time
        .format(HUMAN_READABLE_TIMESTAMP_FORMAT)
        .unwrap_or_else(|_| "Invalid timestamp".to_owned())
}

pub(crate) fn format_optional_unix_timestamp_secs(timestamp: Option<u64>) -> String {
    timestamp
        .map(format_unix_timestamp_secs)
        .unwrap_or_else(|| "Never".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_unix_timestamps_as_human_readable_utc_dates() {
        assert_eq!(format_unix_timestamp_secs(86_400), "1970-01-02 00:00 UTC");
    }

    #[test]
    fn missing_optional_timestamps_render_as_never() {
        assert_eq!(format_optional_unix_timestamp_secs(None), "Never");
    }

    #[test]
    fn out_of_range_timestamps_render_as_invalid() {
        assert_eq!(format_unix_timestamp_secs(u64::MAX), "Invalid timestamp");
    }
}
