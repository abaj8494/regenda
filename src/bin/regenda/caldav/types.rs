use chrono::{DateTime, NaiveDate, Utc};

/// Information about a discovered calendar.
#[derive(Clone, Debug)]
pub struct CalendarInfo {
    pub name: String,
    pub path: String,
    pub color: Option<String>,
    pub visible: bool,
    pub server_name: String,
}

/// A single calendar event.
#[derive(Clone, Debug)]
pub struct Event {
    pub uid: String,
    pub summary: String,
    pub start: DateTime<Utc>,
    pub end: Option<DateTime<Utc>>,
    pub location: Option<String>,
    pub description: Option<String>,
    pub calendar_name: String,
    pub all_day: bool,
}

impl Event {
    /// Get the date of this event in the given timezone offset.
    pub fn date_in_tz(&self, tz: &chrono_tz::Tz) -> NaiveDate {
        use chrono::TimeZone;
        tz.from_utc_datetime(&self.start.naive_utc())
            .date_naive()
    }

    /// Format the start time as HH:MM in the given timezone.
    pub fn start_time_str(&self, tz: &chrono_tz::Tz) -> String {
        use chrono::TimeZone;
        let local = tz.from_utc_datetime(&self.start.naive_utc());
        local.format("%H:%M").to_string()
    }

    /// Format the end time as HH:MM in the given timezone.
    pub fn end_time_str(&self, tz: &chrono_tz::Tz) -> Option<String> {
        self.end.map(|end| {
            use chrono::TimeZone;
            let local = tz.from_utc_datetime(&end.naive_utc());
            local.format("%H:%M").to_string()
        })
    }

    /// Format the start date/time for display.
    pub fn start_datetime_str(&self, tz: &chrono_tz::Tz) -> String {
        use chrono::TimeZone;
        let local = tz.from_utc_datetime(&self.start.naive_utc());
        local.format("%A, %B %-d, %Y %H:%M").to_string()
    }

    /// Format the end date/time for display.
    pub fn end_datetime_str(&self, tz: &chrono_tz::Tz) -> Option<String> {
        self.end.map(|end| {
            use chrono::TimeZone;
            let local = tz.from_utc_datetime(&end.naive_utc());
            local.format("%A, %B %-d, %Y %H:%M").to_string()
        })
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start
    }
}

impl Eq for Event {}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start
            .cmp(&other.start)
            .then_with(|| self.summary.cmp(&other.summary))
    }
}

/// Status of a background CalDAV fetch.
#[derive(Clone, Debug)]
pub enum FetchStatus {
    Loading { message: String },
    Done { calendars: Vec<CalendarInfo>, events: Vec<Event> },
    Error { message: String },
    /// One or more Google sources need OAuth device authorization.
    NeedsOAuth { server_names: Vec<String> },
}
