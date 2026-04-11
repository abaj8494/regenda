use super::types::Event;
use crate::canvas::color;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use ical::parser::ical::component::IcalEvent;
use ical::parser::ical::IcalParser;


/// Parse iCalendar data and extract events.
pub fn parse_ical_events(ical_data: &str, calendar_name: &str, calendar_color: Option<color>) -> Vec<Event> {
    let reader = std::io::BufReader::new(ical_data.as_bytes());
    let parser = IcalParser::new(reader);
    let mut events = Vec::new();

    for calendar in parser.flatten() {
        for vevent in calendar.events {
            if let Some(event) = parse_vevent(&vevent, calendar_name, calendar_color) {
                events.push(event);
            }
        }
    }

    events
}

fn parse_vevent(vevent: &IcalEvent, calendar_name: &str, calendar_color: Option<color>) -> Option<Event> {
    let mut summary = String::new();
    let mut uid = String::new();
    let mut dtstart_str = None;
    let mut dtstart_params = Vec::new();
    let mut dtend_str = None;
    let mut dtend_params = Vec::new();
    let mut location = None;
    let mut description = None;

    for prop in &vevent.properties {
        match prop.name.as_str() {
            "SUMMARY" => {
                summary = prop.value.clone().unwrap_or_default();
            }
            "UID" => {
                uid = prop.value.clone().unwrap_or_default();
            }
            "DTSTART" => {
                dtstart_str = prop.value.clone();
                if let Some(params) = &prop.params {
                    dtstart_params = params.clone();
                }
            }
            "DTEND" => {
                dtend_str = prop.value.clone();
                if let Some(params) = &prop.params {
                    dtend_params = params.clone();
                }
            }
            "LOCATION" => {
                let val = prop.value.clone().unwrap_or_default();
                if !val.is_empty() {
                    location = Some(val);
                }
            }
            "DESCRIPTION" => {
                let val = prop.value.clone().unwrap_or_default();
                if !val.is_empty() {
                    // Unescape common iCal escape sequences
                    let val = val
                        .replace("\\n", "\n")
                        .replace("\\,", ",")
                        .replace("\\;", ";")
                        .replace("\\\\", "\\");
                    description = Some(val);
                }
            }
            _ => {}
        }
    }

    let dtstart_raw = dtstart_str?;

    let (start, all_day) = parse_datetime(&dtstart_raw, &dtstart_params)?;

    let end = dtend_str.and_then(|s| parse_datetime(&s, &dtend_params).map(|(dt, _)| dt));

    Some(Event {
        uid,
        summary,
        start,
        end,
        location,
        description,
        calendar_name: calendar_name.to_string(),
        calendar_color,
        all_day,
    })
}

/// Parse a DTSTART/DTEND value, handling DATE, DATETIME, and TZID.
/// Returns (DateTime<Utc>, is_all_day).
fn parse_datetime(
    value: &str,
    params: &[(String, Vec<String>)],
) -> Option<(DateTime<Utc>, bool)> {
    let value = value.trim();

    // All-day event: DATE format YYYYMMDD
    if value.len() == 8 && value.chars().all(|c| c.is_ascii_digit()) {
        let date = NaiveDate::parse_from_str(value, "%Y%m%d").ok()?;
        let dt = date.and_time(NaiveTime::from_hms_opt(0, 0, 0)?);
        return Some((Utc.from_utc_datetime(&dt), true));
    }

    // Check for TZID parameter
    let tzid = params.iter().find_map(|(k, v)| {
        if k == "TZID" {
            v.first().cloned()
        } else {
            None
        }
    });

    // DATETIME with Z suffix (UTC)
    if value.ends_with('Z') {
        let clean = value.trim_end_matches('Z');
        let ndt = NaiveDateTime::parse_from_str(clean, "%Y%m%dT%H%M%S").ok()?;
        return Some((Utc.from_utc_datetime(&ndt), false));
    }

    // DATETIME with TZID
    let ndt = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%S").ok()?;

    if let Some(tz_name) = tzid {
        if let Ok(tz) = tz_name.parse::<chrono_tz::Tz>() {
            let local = tz.from_local_datetime(&ndt).earliest()?;
            return Some((local.with_timezone(&Utc), false));
        }
    }

    // Assume UTC if no timezone info
    Some((Utc.from_utc_datetime(&ndt), false))
}
