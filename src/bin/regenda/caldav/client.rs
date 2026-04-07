use super::google_oauth;
use super::ical;
use super::parser;
use super::types::{CalendarInfo, Event, FetchStatus};
use crate::config::Config;
use anyhow::{bail, Context, Result};
use chrono::{Duration, NaiveDate, Utc};

const GOOGLE_CALDAV_BASE: &str = "https://apidata.googleusercontent.com/caldav/v2";

/// Auth method for a CalDAV request.
enum Auth {
    Basic { username: String, password: String },
    Bearer { token: String },
}

/// Fetch all calendars and events from configured CalDAV servers.
pub fn fetch_all(config: &Config) -> FetchStatus {
    let mut all_calendars = Vec::new();
    let mut all_events = Vec::new();
    let mut errors = Vec::new();
    let mut pending_oauth: Vec<String> = Vec::new();

    for (server_name, server_config) in &config.sources {
        log::info!(
            "Fetching from source: {} (type: {})",
            server_name,
            server_config.r#type
        );

        if server_config.is_google() {
            let client_id = match &server_config.client_id {
                Some(id) => id.clone(),
                None => {
                    errors.push(format!("{}: missing client_id", server_name));
                    continue;
                }
            };
            let client_secret = match &server_config.client_secret {
                Some(s) => s.clone(),
                None => {
                    errors.push(format!("{}: missing client_secret", server_name));
                    continue;
                }
            };

            match google_oauth::get_access_token(server_name, &client_id, &client_secret) {
                Ok(Some(access_token)) => {
                    let calendar_ids = server_config
                        .calendar_id
                        .clone()
                        .unwrap_or_else(|| vec!["primary".to_string()]);

                    match fetch_google(server_name, &access_token, &calendar_ids) {
                        Ok((cals, evts)) => {
                            log::info!(
                                "Google {}: fetched {} calendars, {} events",
                                server_name,
                                cals.len(),
                                evts.len()
                            );
                            all_calendars.extend(cals);
                            all_events.extend(evts);
                        }
                        Err(e) => {
                            log::error!("Failed to fetch from Google {}: {:?}", server_name, e);
                            errors.push(format!("{}: {}", server_name, e));
                        }
                    }
                }
                Ok(None) => {
                    log::info!("Google source {} needs OAuth authorization", server_name);
                    pending_oauth.push(server_name.clone());
                }
                Err(e) => {
                    log::error!("OAuth error for {}: {:?}", server_name, e);
                    errors.push(format!("{}: {}", server_name, e));
                }
            }
        } else {
            let url = match &server_config.url {
                Some(u) => u.clone(),
                None => {
                    errors.push(format!("{}: missing url", server_name));
                    continue;
                }
            };
            let user = server_config.user.clone().unwrap_or_default();
            let password = server_config.password.clone().unwrap_or_default();

            match fetch_server(server_name, &url, &user, &password) {
                Ok((cals, evts)) => {
                    all_calendars.extend(cals);
                    all_events.extend(evts);
                }
                Err(e) => {
                    log::error!("Failed to fetch from {}: {:?}", server_name, e);
                    errors.push(format!("{}: {}", server_name, e));
                }
            }
        }
    }

    if !pending_oauth.is_empty() {
        return FetchStatus::NeedsOAuth {
            server_names: pending_oauth,
        };
    }

    if all_calendars.is_empty() && !errors.is_empty() {
        FetchStatus::Error {
            message: errors.join("\n"),
        }
    } else {
        all_events.sort();
        FetchStatus::Done {
            calendars: all_calendars,
            events: all_events,
        }
    }
}

/// Fetch events from Google CalDAV using OAuth bearer token.
/// Google CalDAV endpoint: https://apidata.googleusercontent.com/caldav/v2/{calendarId}/events
fn fetch_google(
    server_name: &str,
    access_token: &str,
    calendar_ids: &[String],
) -> Result<(Vec<CalendarInfo>, Vec<Event>)> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("Failed to build HTTP client")?;

    let auth = Auth::Bearer {
        token: access_token.to_string(),
    };

    let mut calendars = Vec::new();
    let mut all_events = Vec::new();

    for cal_id in calendar_ids {
        // URL-encode the calendar ID (handles email addresses with @)
        let encoded_id = urlencoding::encode(cal_id);
        let cal_base_url = format!("{}/{}/", GOOGLE_CALDAV_BASE, encoded_id);
        let cal_events_url = format!("{}events/", cal_base_url);

        log::info!("Google: fetching calendar '{}' at {}", cal_id, cal_events_url);

        // Try PROPFIND to get calendar display name
        let cal_name = match propfind_displayname(&client, &cal_base_url, &auth) {
            Ok(Some(name)) => {
                log::info!("Google: calendar display name = '{}'", name);
                name
            }
            Ok(None) => {
                log::info!("Google: no display name found, using calendar ID");
                cal_id.clone()
            }
            Err(e) => {
                log::warn!("Google: PROPFIND for display name failed: {:?}", e);
                cal_id.clone()
            }
        };

        calendars.push(CalendarInfo {
            name: cal_name.clone(),
            path: cal_id.clone(),
            color: None,
            visible: true,
            server_name: server_name.to_string(),
        });

        // Fetch events — try REPORT first, fall back to PROPFIND listing
        match fetch_google_events(&client, &cal_events_url, &auth, &cal_name) {
            Ok(events) => {
                log::info!(
                    "Google: fetched {} events from '{}'",
                    events.len(),
                    cal_name
                );
                all_events.extend(events);
            }
            Err(e) => {
                log::warn!(
                    "Google REPORT failed for '{}': {:?}. Trying PROPFIND fallback.",
                    cal_name,
                    e
                );
                // Fallback: PROPFIND to list all event resources, then GET each
                match fetch_google_events_propfind(&client, &cal_events_url, &auth, &cal_name) {
                    Ok(events) => {
                        log::info!(
                            "Google PROPFIND fallback: fetched {} events from '{}'",
                            events.len(),
                            cal_name
                        );
                        all_events.extend(events);
                    }
                    Err(e2) => {
                        log::error!(
                            "Google: both REPORT and PROPFIND failed for '{}': {:?}",
                            cal_name,
                            e2
                        );
                    }
                }
            }
        }
    }

    Ok((calendars, all_events))
}

/// Fetch Google events via calendar-query REPORT.
fn fetch_google_events(
    client: &reqwest::blocking::Client,
    calendar_url: &str,
    auth: &Auth,
    calendar_name: &str,
) -> Result<Vec<Event>> {
    let now = Utc::now().date_naive();
    let start = now - Duration::days(7);
    let end = now + Duration::days(30);

    let report_xml = build_calendar_report(start, end);

    log::debug!("Google REPORT to: {}", calendar_url);

    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"REPORT").unwrap(),
            calendar_url,
        )
        .header("Depth", "1")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(report_xml);

    req = apply_auth(req, auth);

    let resp = req.send().context("REPORT request failed")?;
    let status = resp.status();
    let body = resp.text().context("Failed to read REPORT response")?;

    log::debug!(
        "Google REPORT response status: {}, body length: {}",
        status,
        body.len()
    );

    if !status.is_success() && status.as_u16() != 207 {
        log::warn!("Google REPORT non-success status {}: {}", status, &body[..body.len().min(500)]);
        bail!("REPORT returned status {}", status);
    }

    let parsed = parser::parse_report_events(&body)?;
    log::debug!("Google REPORT: parsed {} event items", parsed.len());

    let mut events = Vec::new();
    for item in &parsed {
        let mut parsed_events = ical::parse_ical_events(&item.ical_data, calendar_name);
        log::debug!(
            "Google: parsed {} events from iCal data ({} bytes)",
            parsed_events.len(),
            item.ical_data.len()
        );
        events.append(&mut parsed_events);
    }

    Ok(events)
}

/// Fallback: use PROPFIND to list events, then GET each .ics resource.
fn fetch_google_events_propfind(
    client: &reqwest::blocking::Client,
    calendar_url: &str,
    auth: &Auth,
    calendar_name: &str,
) -> Result<Vec<Event>> {
    let propfind_xml = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <d:getetag/>
    <c:calendar-data/>
  </d:prop>
</d:propfind>"#;

    log::debug!("Google PROPFIND events at: {}", calendar_url);

    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"PROPFIND").unwrap(),
            calendar_url,
        )
        .header("Depth", "1")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(propfind_xml);

    req = apply_auth(req, auth);

    let resp = req.send().context("PROPFIND for events failed")?;
    let status = resp.status();
    let body = resp.text().context("Failed to read PROPFIND response")?;

    log::debug!(
        "Google PROPFIND events status: {}, body length: {}",
        status,
        body.len()
    );

    if !status.is_success() && status.as_u16() != 207 {
        // If PROPFIND with calendar-data doesn't work, try GET on individual resources
        log::debug!("PROPFIND with calendar-data failed, trying resource listing + GET");
        return fetch_google_events_get(client, calendar_url, auth, calendar_name);
    }

    let parsed = parser::parse_report_events(&body)?;
    log::debug!(
        "Google PROPFIND events: parsed {} items from response",
        parsed.len()
    );

    let mut events = Vec::new();
    for item in &parsed {
        if item.ical_data.is_empty() {
            continue;
        }
        let mut parsed_events = ical::parse_ical_events(&item.ical_data, calendar_name);
        events.append(&mut parsed_events);
    }

    Ok(events)
}

/// Last resort: PROPFIND to list hrefs, then GET each .ics individually.
fn fetch_google_events_get(
    client: &reqwest::blocking::Client,
    calendar_url: &str,
    auth: &Auth,
    calendar_name: &str,
) -> Result<Vec<Event>> {
    // Simple PROPFIND to list resources
    let propfind_xml = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:getetag/>
    <d:getcontenttype/>
  </d:prop>
</d:propfind>"#;

    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"PROPFIND").unwrap(),
            calendar_url,
        )
        .header("Depth", "1")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(propfind_xml);

    req = apply_auth(req, auth);

    let resp = req.send().context("PROPFIND listing failed")?;
    let status = resp.status();
    let body = resp.text()?;

    log::debug!("Google PROPFIND listing status: {}, body length: {}", status, body.len());

    if !status.is_success() && status.as_u16() != 207 {
        bail!("PROPFIND listing returned status {}", status);
    }

    // Parse to get hrefs
    let cals = parser::parse_propfind_calendars(&body)?;
    let mut events = Vec::new();

    let now = Utc::now().date_naive();
    let start = now - Duration::days(7);
    let end = now + Duration::days(30);

    for cal in &cals {
        if cal.href.is_empty() || cal.href == calendar_url || cal.href.ends_with('/') {
            continue; // Skip the collection itself
        }

        let event_url = resolve_url(calendar_url, &cal.href);
        log::debug!("Google: GET {}", event_url);

        let mut get_req = client.get(&event_url);
        get_req = apply_auth(get_req, auth);

        if let Ok(resp) = get_req.send() {
            if resp.status().is_success() {
                if let Ok(ical_data) = resp.text() {
                    let mut parsed = ical::parse_ical_events(&ical_data, calendar_name);
                    // Filter by date range
                    parsed.retain(|e| {
                        let d = e.start.date_naive();
                        d >= start && d <= end
                    });
                    events.append(&mut parsed);
                }
            }
        }
    }

    log::info!(
        "Google GET fallback: fetched {} events from '{}'",
        events.len(),
        calendar_name
    );

    Ok(events)
}

fn propfind_displayname(
    client: &reqwest::blocking::Client,
    url: &str,
    auth: &Auth,
) -> Result<Option<String>> {
    let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname/>
  </d:prop>
</d:propfind>"#;

    let mut req = client
        .request(reqwest::Method::from_bytes(b"PROPFIND").unwrap(), url)
        .header("Depth", "0")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(xml);

    req = apply_auth(req, auth);

    let resp = req.send().context("PROPFIND for displayname failed")?;
    let body = resp.text()?;

    let parsed = parser::parse_propfind_calendars(&body)?;
    Ok(parsed.first().and_then(|c| c.display_name.clone()))
}

// ---- Standard CalDAV (basic auth) ----

fn fetch_server(
    server_name: &str,
    url: &str,
    username: &str,
    password: &str,
) -> Result<(Vec<CalendarInfo>, Vec<Event>)> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("Failed to build HTTP client")?;

    let auth = Auth::Basic {
        username: username.to_string(),
        password: password.to_string(),
    };

    let principal_url = discover_principal(&client, url, &auth)?;
    let calendar_home = discover_calendar_home(&client, &principal_url, &auth)?;

    let propfind_xml = build_calendar_propfind();
    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"PROPFIND").unwrap(),
            &calendar_home,
        )
        .header("Depth", "1")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(propfind_xml);

    req = apply_auth(req, &auth);

    let resp = req.send().context("PROPFIND for calendars failed")?;
    let body = resp.text().context("Failed to read PROPFIND response")?;
    let parsed = parser::parse_propfind_calendars(&body)?;

    let mut calendars = Vec::new();
    let mut all_events = Vec::new();

    for cal in &parsed {
        if !cal.is_calendar {
            continue;
        }
        let cal_name = cal
            .display_name
            .clone()
            .unwrap_or_else(|| cal.href.clone());

        let cal_url = resolve_url(&calendar_home, &cal.href);

        calendars.push(CalendarInfo {
            name: cal_name.clone(),
            path: cal.href.clone(),
            color: cal.color.clone(),
            visible: true,
            server_name: server_name.to_string(),
        });

        match fetch_calendar_events_with_auth(&client, &cal_url, &auth, &cal_name) {
            Ok(events) => all_events.extend(events),
            Err(e) => log::warn!("Failed to fetch events from {}: {:?}", cal_name, e),
        }
    }

    Ok((calendars, all_events))
}

fn discover_principal(
    client: &reqwest::blocking::Client,
    url: &str,
    auth: &Auth,
) -> Result<String> {
    let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:current-user-principal/>
  </d:prop>
</d:propfind>"#;

    let mut req = client
        .request(reqwest::Method::from_bytes(b"PROPFIND").unwrap(), url)
        .header("Depth", "0")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(xml);

    req = apply_auth(req, auth);

    let resp = req.send().context("PROPFIND for principal failed")?;
    let body = resp.text()?;

    if let Some(href) = extract_href_from_tag(&body, "current-user-principal") {
        Ok(resolve_url(url, &href))
    } else {
        Ok(url.to_string())
    }
}

fn discover_calendar_home(
    client: &reqwest::blocking::Client,
    principal_url: &str,
    auth: &Auth,
) -> Result<String> {
    let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:calendar-home-set/>
  </d:prop>
</d:propfind>"#;

    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"PROPFIND").unwrap(),
            principal_url,
        )
        .header("Depth", "0")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(xml);

    req = apply_auth(req, auth);

    let resp = req
        .send()
        .context("PROPFIND for calendar-home-set failed")?;
    let body = resp.text()?;

    if let Some(href) = extract_href_from_tag(&body, "calendar-home-set") {
        Ok(resolve_url(principal_url, &href))
    } else {
        Ok(principal_url.to_string())
    }
}

fn fetch_calendar_events_with_auth(
    client: &reqwest::blocking::Client,
    calendar_url: &str,
    auth: &Auth,
    calendar_name: &str,
) -> Result<Vec<Event>> {
    let now = Utc::now().date_naive();
    let start = now - Duration::days(7);
    let end = now + Duration::days(30);

    let report_xml = build_calendar_report(start, end);

    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"REPORT").unwrap(),
            calendar_url,
        )
        .header("Depth", "1")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(report_xml);

    req = apply_auth(req, auth);

    let resp = req.send().context("REPORT for calendar events failed")?;
    let body = resp.text().context("Failed to read REPORT response")?;

    let parsed = parser::parse_report_events(&body)?;

    let mut events = Vec::new();
    for item in &parsed {
        let mut parsed_events = ical::parse_ical_events(&item.ical_data, calendar_name);
        events.append(&mut parsed_events);
    }

    Ok(events)
}

// ---- Helpers ----

fn apply_auth(
    req: reqwest::blocking::RequestBuilder,
    auth: &Auth,
) -> reqwest::blocking::RequestBuilder {
    match auth {
        Auth::Basic { username, password } => req.basic_auth(username, Some(password)),
        Auth::Bearer { token } => req.bearer_auth(token),
    }
}

fn build_calendar_propfind() -> String {
    r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cs="http://calendarserver.org/ns/" xmlns:c="urn:ietf:params:xml:ns:caldav" xmlns:apple="http://apple.com/ns/ical/">
  <d:prop>
    <d:resourcetype/>
    <d:displayname/>
    <apple:calendar-color/>
    <cs:getctag/>
  </d:prop>
</d:propfind>"#
        .to_string()
}

fn build_calendar_report(start: NaiveDate, end: NaiveDate) -> String {
    let start_str = start.format("%Y%m%dT000000Z");
    let end_str = end.format("%Y%m%dT235959Z");

    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<c:calendar-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <d:getetag/>
    <c:calendar-data/>
  </d:prop>
  <c:filter>
    <c:comp-filter name="VCALENDAR">
      <c:comp-filter name="VEVENT">
        <c:time-range start="{}" end="{}"/>
      </c:comp-filter>
    </c:comp-filter>
  </c:filter>
</c:calendar-query>"#,
        start_str, end_str
    )
}

fn resolve_url(base: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        return href.to_string();
    }

    if let Some(scheme_end) = base.find("://") {
        let after_scheme = &base[scheme_end + 3..];
        if let Some(path_start) = after_scheme.find('/') {
            let origin = &base[..scheme_end + 3 + path_start];
            if href.starts_with('/') {
                return format!("{}{}", origin, href);
            }
        }
    }

    let base_trimmed = base.trim_end_matches('/');
    let href_trimmed = href.trim_start_matches('/');
    format!("{}/{}", base_trimmed, href_trimmed)
}

fn extract_href_from_tag(xml: &str, tag: &str) -> Option<String> {
    let tag_pattern = format!(":{}", tag);
    let tag_pattern2 = format!("<{}", tag);

    let tag_start = xml.find(&tag_pattern).or_else(|| xml.find(&tag_pattern2))?;

    let rest = &xml[tag_start..];
    let href_start = rest.find(":href>").or_else(|| rest.find("<href>"))?;
    let content_start = rest[href_start..].find('>')? + href_start + 1;
    let content_end = rest[content_start..].find('<')? + content_start;

    Some(rest[content_start..content_end].trim().to_string())
}

/// Minimal percent-encoding for URL path segments.
mod urlencoding {
    pub fn encode(input: &str) -> String {
        let mut result = String::with_capacity(input.len() * 3);
        for byte in input.bytes() {
            match byte {
                b'A'..=b'Z'
                | b'a'..=b'z'
                | b'0'..=b'9'
                | b'-'
                | b'_'
                | b'.'
                | b'~' => result.push(byte as char),
                _ => {
                    result.push('%');
                    result.push_str(&format!("{:02X}", byte));
                }
            }
        }
        result
    }
}
