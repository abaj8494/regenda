use super::google_oauth;
use super::ical;
use super::parser;
use super::types::{parse_hex_color, CalendarInfo, Event, FetchStatus};
use crate::canvas::color;
use crate::config::{Config, ServerConfig};
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

                    match fetch_google(server_name, &access_token, &calendar_ids, server_config) {
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
        } else if server_config.is_ics() {
            let url = match &server_config.url {
                Some(u) => u.clone(),
                None => {
                    errors.push(format!("{}: missing url", server_name));
                    continue;
                }
            };

            match fetch_ics(server_name, &url, server_config) {
                Ok((cals, evts)) => {
                    log::info!(
                        "ICS {}: fetched {} calendars, {} events",
                        server_name,
                        cals.len(),
                        evts.len()
                    );
                    all_calendars.extend(cals);
                    all_events.extend(evts);
                }
                Err(e) => {
                    log::error!("Failed to fetch ICS from {}: {:?}", server_name, e);
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
///
/// Google's CalDAV has quirks:
/// - REPORT (calendar-query) returns 403 Forbidden — not supported
/// - "primary" alias doesn't work as a CalDAV calendar ID — need real email
/// - PROPFIND with Depth:1 + calendar-data on /events/ is the working method
fn fetch_google(
    server_name: &str,
    access_token: &str,
    calendar_ids: &[String],
    server_config: &ServerConfig,
) -> Result<(Vec<CalendarInfo>, Vec<Event>)> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("Failed to build HTTP client")?;

    let auth = Auth::Bearer {
        token: access_token.to_string(),
    };

    // If "primary" is in the list, discover actual calendar IDs first
    // Store (cal_id, discovered_color) pairs
    use std::collections::HashMap;
    let mut discovered_colors: HashMap<String, Option<String>> = HashMap::new();

    let resolved_ids = if calendar_ids.iter().any(|id| id == "primary") {
        log::info!("Google: 'primary' specified, discovering actual calendar IDs...");
        match discover_google_calendars(&client, &auth) {
            Ok(discovered) => {
                log::info!("Google: discovered {} calendars", discovered.len());
                let mut ids: Vec<String> = calendar_ids
                    .iter()
                    .filter(|id| *id != "primary")
                    .cloned()
                    .collect();
                for (cal_id, _name, color_hex) in &discovered {
                    discovered_colors.insert(cal_id.clone(), color_hex.clone());
                    if !ids.contains(cal_id) {
                        ids.push(cal_id.clone());
                    }
                }
                ids
            }
            Err(e) => {
                log::warn!("Google: calendar discovery failed: {:?}. Using configured IDs.", e);
                calendar_ids.to_vec()
            }
        }
    } else {
        calendar_ids.to_vec()
    };

    let mut calendars = Vec::new();
    let mut all_events = Vec::new();

    for cal_id in &resolved_ids {
        // URL-encode the calendar ID (handles email addresses with @)
        let encoded_id = urlencoding::encode(cal_id);
        let cal_base_url = format!("{}/{}/", GOOGLE_CALDAV_BASE, encoded_id);
        let cal_events_url = format!("{}events/", cal_base_url);

        log::info!("Google: fetching calendar '{}' at {}", cal_id, cal_events_url);

        // Get calendar color: prefer discovered color, fallback to PROPFIND
        let (server_display_name, cal_color_str) =
            if let Some(disc_color) = discovered_colors.get(cal_id) {
                // We have discovery data — do a quick PROPFIND just for display name
                let (name, propfind_color) = propfind_calendar_props(&client, &cal_base_url, &auth);
                // Prefer discovery color (from listing), fall back to PROPFIND color
                (name, disc_color.clone().or(propfind_color))
            } else {
                propfind_calendar_props(&client, &cal_base_url, &auth)
            };

        let cal_name = if let Some(alias) = server_config.resolve_display_name(cal_id) {
            log::info!("Google: using config alias '{}' for '{}'", alias, cal_id);
            alias
        } else {
            match server_display_name {
                Some(name) if !name.trim().is_empty() => {
                    log::info!("Google: calendar display name = '{}'", name.trim());
                    name.trim().to_string()
                }
                _ => {
                    log::info!("Google: no display name, using calendar ID");
                    cal_id.clone()
                }
            }
        };

        let cal_color = cal_color_str.as_deref().and_then(parse_hex_color);
        log::info!("Google: calendar '{}' color = {:?}", cal_name, cal_color);

        calendars.push(CalendarInfo {
            name: cal_name.clone(),
            path: cal_id.clone(),
            color: cal_color_str,
            visible: true,
            server_name: server_name.to_string(),
        });

        // Google CalDAV: PROPFIND with calendar-data is the working method
        // (REPORT always returns 403)
        match fetch_google_events_propfind(&client, &cal_events_url, &auth, &cal_name, cal_color) {
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
                    "Google PROPFIND failed for '{}': {:?}. Trying GET fallback.",
                    cal_name,
                    e
                );
                match fetch_google_events_get(&client, &cal_events_url, &auth, &cal_name, cal_color) {
                    Ok(events) => {
                        log::info!(
                            "Google GET fallback: fetched {} events from '{}'",
                            events.len(),
                            cal_name
                        );
                        all_events.extend(events);
                    }
                    Err(e2) => {
                        log::error!(
                            "Google: all methods failed for '{}': {:?}",
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

/// Discover all calendars available to the authenticated Google user.
/// Does PROPFIND on the CalDAV principal to find calendar-home-set,
/// then lists calendars there.
/// Returned tuple: (calendar_id, display_name, color_hex)
fn discover_google_calendars(
    client: &reqwest::blocking::Client,
    auth: &Auth,
) -> Result<Vec<(String, String, Option<String>)>> {
    // Step 1: Find the principal
    let principal_xml = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:current-user-principal/>
  </d:prop>
</d:propfind>"#;

    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"PROPFIND").unwrap(),
            GOOGLE_CALDAV_BASE,
        )
        .header("Depth", "0")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(principal_xml);
    req = apply_auth(req, auth);

    let resp = req.send().context("Google principal discovery failed")?;
    let body = resp.text()?;
    log::debug!("Google principal response: {}", &body[..body.len().min(500)]);

    let principal_href = extract_href_from_tag(&body, "current-user-principal");
    let principal_url = match principal_href {
        Some(href) => resolve_url(GOOGLE_CALDAV_BASE, &href),
        None => {
            log::warn!("Google: no principal found, trying calendar-home-set directly");
            GOOGLE_CALDAV_BASE.to_string()
        }
    };

    log::info!("Google principal URL: {}", principal_url);

    // Step 2: Find calendar-home-set
    let home_xml = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:calendar-home-set/>
  </d:prop>
</d:propfind>"#;

    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"PROPFIND").unwrap(),
            &principal_url,
        )
        .header("Depth", "0")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(home_xml);
    req = apply_auth(req, auth);

    let resp = req.send().context("Google calendar-home-set discovery failed")?;
    let body = resp.text()?;
    log::debug!("Google calendar-home-set response: {}", &body[..body.len().min(500)]);

    let home_href = extract_href_from_tag(&body, "calendar-home-set");
    let home_url = match home_href {
        Some(href) => resolve_url(&principal_url, &href),
        None => principal_url.clone(),
    };

    log::info!("Google calendar home URL: {}", home_url);

    // Step 3: List calendars
    let list_xml = build_calendar_propfind();
    let mut req = client
        .request(
            reqwest::Method::from_bytes(b"PROPFIND").unwrap(),
            &home_url,
        )
        .header("Depth", "1")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(list_xml);
    req = apply_auth(req, auth);

    let resp = req.send().context("Google calendar listing failed")?;
    let status = resp.status();
    let body = resp.text()?;
    log::debug!("Google calendar listing status: {}, body length: {}", status, body.len());

    let parsed = parser::parse_propfind_calendars(&body)?;
    let mut result = Vec::new();

    for cal in &parsed {
        if !cal.is_calendar {
            continue;
        }
        // Google CalDAV URLs look like: /caldav/v2/{calendarId}/events/
        // Extract the calendarId segment (the one after /caldav/v2/)
        let segments: Vec<&str> = cal
            .href
            .trim_end_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        // Find the segment after "v2" in the path
        let cal_id = segments
            .iter()
            .position(|&s| s == "v2")
            .and_then(|i| segments.get(i + 1))
            .map(|s| percent_decode(s))
            .unwrap_or_else(|| {
                // Fallback: second-to-last segment
                if segments.len() >= 2 {
                    percent_decode(segments[segments.len() - 2])
                } else {
                    cal.href.clone()
                }
            });

        let name = cal
            .display_name
            .clone()
            .unwrap_or_else(|| cal_id.clone());

        log::info!("Google: discovered calendar '{}' (ID: {}, color: {:?})", name.trim(), cal_id, cal.color);
        result.push((cal_id, name.trim().to_string(), cal.color.clone()));
    }

    Ok(result)
}

/// Simple percent-decoding.
fn percent_decode(input: &str) -> String {
    let mut result = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                16,
            ) {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(result).unwrap_or_else(|_| input.to_string())
}

/// Fetch Google events via calendar-query REPORT.
fn fetch_google_events(
    client: &reqwest::blocking::Client,
    calendar_url: &str,
    auth: &Auth,
    calendar_name: &str,
    cal_color: Option<color>,
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
        let mut parsed_events = ical::parse_ical_events(&item.ical_data, calendar_name, cal_color);
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
    cal_color: Option<color>,
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
        return fetch_google_events_get(client, calendar_url, auth, calendar_name, cal_color);
    }

    let parsed = parser::parse_report_events(&body)?;
    log::debug!(
        "Google PROPFIND events: parsed {} items from response",
        parsed.len()
    );

    let now = Utc::now().date_naive();
    let range_start = now - Duration::days(7);
    let range_end = now + Duration::days(30);

    let mut events = Vec::new();
    for item in &parsed {
        if item.ical_data.is_empty() {
            continue;
        }
        let mut parsed_events = ical::parse_ical_events(&item.ical_data, calendar_name, cal_color);
        // Filter to date range (PROPFIND doesn't support time-range like REPORT)
        parsed_events.retain(|e| {
            let d = e.start.date_naive();
            d >= range_start && d <= range_end
        });
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
    cal_color: Option<color>,
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
                    let mut parsed = ical::parse_ical_events(&ical_data, calendar_name, cal_color);
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

/// Fetch display name and color for a calendar via PROPFIND.
fn propfind_calendar_props(
    client: &reqwest::blocking::Client,
    url: &str,
    auth: &Auth,
) -> (Option<String>, Option<String>) {
    let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:apple="http://apple.com/ns/ical/" xmlns:cs="http://calendarserver.org/ns/">
  <d:prop>
    <d:displayname/>
    <apple:calendar-color/>
  </d:prop>
</d:propfind>"#;

    let mut req = client
        .request(reqwest::Method::from_bytes(b"PROPFIND").unwrap(), url)
        .header("Depth", "0")
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(xml);

    req = apply_auth(req, auth);

    match req.send().and_then(|r| r.text()) {
        Ok(body) => {
            match parser::parse_propfind_calendars(&body) {
                Ok(parsed) => {
                    let first = parsed.first();
                    (
                        first.and_then(|c| c.display_name.clone()),
                        first.and_then(|c| c.color.clone()),
                    )
                }
                Err(_) => (None, None),
            }
        }
        Err(_) => (None, None),
    }
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

        let cal_color = cal.color.as_deref().and_then(parse_hex_color);
        match fetch_calendar_events_with_auth(&client, &cal_url, &auth, &cal_name, cal_color) {
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
    cal_color: Option<color>,
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
        let mut parsed_events = ical::parse_ical_events(&item.ical_data, calendar_name, cal_color);
        events.append(&mut parsed_events);
    }

    Ok(events)
}

// ---- Static iCalendar subscription (type: ics) ----

/// Fetch a static iCalendar feed (e.g. a `webcal://` subscription URL).
///
/// These are not CalDAV endpoints — they're just a single `.ics` file served
/// over HTTP(S). We do one GET, parse the body via `ical::parse_ical_events`,
/// and filter to the same date window as the CalDAV paths.
fn fetch_ics(
    server_name: &str,
    url: &str,
    server_config: &ServerConfig,
) -> Result<(Vec<CalendarInfo>, Vec<Event>)> {
    // `webcal://` is a pseudo-scheme meaning "fetch this as HTTPS".
    // `reqwest` can't speak it, so rewrite before sending.
    let http_url = if let Some(rest) = url.strip_prefix("webcal://") {
        format!("https://{}", rest)
    } else if let Some(rest) = url.strip_prefix("webcals://") {
        format!("https://{}", rest)
    } else {
        url.to_string()
    };

    log::info!("ICS {}: fetching {}", server_name, http_url);

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("Failed to build HTTP client")?;

    let resp = client
        .get(&http_url)
        .send()
        .with_context(|| format!("GET {} failed", http_url))?;

    let status = resp.status();
    if !status.is_success() {
        bail!("ICS fetch returned HTTP {}", status);
    }

    let ical_data = resp.text().context("Failed to read ICS body")?;
    log::info!("ICS {}: fetched {} bytes", server_name, ical_data.len());

    let cal_name = server_config
        .resolve_display_name(server_name)
        .unwrap_or_else(|| server_name.to_string());

    let calendar = CalendarInfo {
        name: cal_name.clone(),
        path: http_url.clone(),
        color: None,
        visible: true,
        server_name: server_name.to_string(),
    };

    let mut events = ical::parse_ical_events(&ical_data, &cal_name, None);

    // ICS feeds are static files — we already downloaded everything, so there's
    // no reason to clamp hard like the CalDAV paths do. Use a wide window that
    // covers any reasonable navigation (a full academic year ahead, a quarter back).
    let now = Utc::now().date_naive();
    let range_start = now - Duration::days(90);
    let range_end = now + Duration::days(365);
    events.retain(|e| {
        let d = e.start.date_naive();
        d >= range_start && d <= range_end
    });

    log::info!(
        "ICS {}: {} events in range",
        server_name,
        events.len()
    );

    Ok((vec![calendar], events))
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
