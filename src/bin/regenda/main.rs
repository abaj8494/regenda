#![allow(non_camel_case_types, non_snake_case, dead_code, unused_imports)]

#[macro_use]
extern crate downcast_rs;
#[macro_use]
extern crate log;

mod caldav;
mod canvas;
mod config;
mod i18n;
mod rmpp_hal;
mod scene;

use crate::caldav::FetchStatus;
use crate::canvas::Canvas;
use crate::config::Config;
use crate::rmpp_hal::input::start_input_threads;
use crate::rmpp_hal::types::InputEvent;
use crate::scene::*;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, SystemTime};

pub const DISPLAY_WIDTH: u32 = 1620;
pub const DISPLAY_HEIGHT: u32 = 2160;

fn main() {
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "INFO") };
    }
    env_logger::init();
    info!("reGenda starting");

    // Handle SIGTERM gracefully (AppLoad sends this on swipe-to-close)
    setup_signal_handler();

    // Load config
    let config = match Config::load() {
        Ok(c) => c,
        Err(e) => {
            error!("Config error: {:?}", e);
            // We'll show the error in the loading scene
            run_with_error(&format!("{:?}", e));
            return;
        }
    };

    let strings = i18n::get_strings(config.language_str());
    let tz: chrono_tz::Tz = config
        .timezone_str()
        .parse()
        .unwrap_or(chrono_tz::UTC);

    info!("Timezone: {}, Language: {}", tz, config.language_str());

    // Initialize display
    let mut canvas = Canvas::new();
    let (input_tx, input_rx) = std::sync::mpsc::channel::<InputEvent>();
    start_input_threads(input_tx, canvas.qtfb_fd());

    // Start CalDAV fetch on background thread
    let fetch_status = Arc::new(Mutex::new(FetchStatus::Loading {
        message: strings.loading.to_string(),
    }));

    {
        let status = fetch_status.clone();
        let config_clone = config.clone();
        std::thread::spawn(move || {
            let result = caldav::fetch_all(&config_clone);
            *status.lock().unwrap() = result;
        });
    }

    // Main loop
    const FPS: u16 = 10;
    const FRAME_DURATION: Duration = Duration::from_millis(1000 / FPS as u64);

    let mut current_scene: Box<dyn Scene> =
        Box::new(LoadingScene::new(fetch_status.clone(), strings));

    // Shared app state
    let mut all_events: Vec<caldav::Event> = Vec::new();
    let mut all_calendars: Vec<caldav::CalendarInfo> = Vec::new();

    loop {
        let before_input = SystemTime::now();
        for event in input_rx.try_iter() {
            current_scene.on_input(event);
        }

        current_scene.draw(&mut canvas);
        current_scene = update(
            current_scene,
            &mut canvas,
            &fetch_status,
            &config,
            strings,
            tz,
            &mut all_events,
            &mut all_calendars,
        );

        let elapsed = before_input.elapsed().unwrap();
        if elapsed < FRAME_DURATION {
            sleep(FRAME_DURATION - elapsed);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn update(
    scene: Box<dyn Scene>,
    canvas: &mut Canvas,
    fetch_status: &Arc<Mutex<FetchStatus>>,
    config: &Config,
    strings: &'static i18n::Strings,
    tz: chrono_tz::Tz,
    all_events: &mut Vec<caldav::Event>,
    all_calendars: &mut Vec<caldav::CalendarInfo>,
) -> Box<dyn Scene> {
    // Loading scene transitions
    if let Some(loading) = scene.downcast_ref::<LoadingScene>() {
        if loading.data_ready {
            // Extract data from fetch status
            let status = fetch_status.lock().unwrap().clone();
            if let FetchStatus::Done { calendars, events } = status {
                *all_calendars = calendars;
                *all_events = events;
            }
            let today = chrono::Local::now().date_naive();
            return Box::new(DayScene::new(
                today,
                all_events,
                all_calendars.clone(),
                strings,
                tz,
            ));
        }
        if !loading.needs_oauth.is_empty() {
            // Start OAuth flow for the first pending Google source
            let server_name = loading.needs_oauth[0].clone();
            if let Some(server_config) = config.sources.get(&server_name) {
                return Box::new(OAuthScene::new(
                    server_name,
                    server_config,
                    strings,
                ));
            }
        }
        if loading.retry_pressed {
            // Retry fetch
            let status_clone = fetch_status.clone();
            let config_clone = config.clone();
            *fetch_status.lock().unwrap() = FetchStatus::Loading {
                message: strings.loading.to_string(),
            };
            std::thread::spawn(move || {
                let result = caldav::fetch_all(&config_clone);
                *status_clone.lock().unwrap() = result;
            });
            return Box::new(LoadingScene::new(fetch_status.clone(), strings));
        }
    }

    // OAuth scene transitions
    if let Some(oauth) = scene.downcast_ref::<OAuthScene>() {
        if oauth.auth_complete || oauth.cancel_pressed {
            // Re-fetch all calendars (token is now stored)
            let status_clone = fetch_status.clone();
            let config_clone = config.clone();
            *fetch_status.lock().unwrap() = FetchStatus::Loading {
                message: strings.loading.to_string(),
            };
            std::thread::spawn(move || {
                let result = caldav::fetch_all(&config_clone);
                *status_clone.lock().unwrap() = result;
            });
            return Box::new(LoadingScene::new(fetch_status.clone(), strings));
        }
    }

    // Day scene transitions
    if let Some(day) = scene.downcast_ref::<DayScene>() {
        if day.go_to_month {
            return Box::new(MonthScene::new(
                day.current_date,
                all_events.clone(),
                strings,
                tz,
            ));
        }
        if day.go_to_settings {
            return Box::new(SettingsScene::new(
                all_calendars.clone(),
                strings,
            ));
        }
        if let Some(idx) = day.go_to_event {
            if idx < day.events.len() {
                return Box::new(EventScene::new(
                    day.events[idx].clone(),
                    strings,
                    tz,
                ));
            }
        }
        if day.refresh_pressed {
            let status_clone = fetch_status.clone();
            let config_clone = config.clone();
            *fetch_status.lock().unwrap() = FetchStatus::Loading {
                message: strings.refreshing.to_string(),
            };
            std::thread::spawn(move || {
                let result = caldav::fetch_all(&config_clone);
                *status_clone.lock().unwrap() = result;
            });
            return Box::new(LoadingScene::new(fetch_status.clone(), strings));
        }
        if day.exit_pressed {
            canvas.clear();
            canvas.update_full();
            std::process::exit(0);
        }
        // Day change: re-filter events
        // (handled internally by DayScene's draw)
    }

    // Month scene transitions
    if let Some(month) = scene.downcast_ref::<MonthScene>() {
        if month.back_pressed {
            let date = month
                .selected_date
                .unwrap_or(chrono::Local::now().date_naive());
            return Box::new(DayScene::new(
                date,
                all_events,
                all_calendars.clone(),
                strings,
                tz,
            ));
        }
        if let Some(date) = month.selected_date {
            return Box::new(DayScene::new(
                date,
                all_events,
                all_calendars.clone(),
                strings,
                tz,
            ));
        }
    }

    // Event scene transitions
    if let Some(event_scene) = scene.downcast_ref::<EventScene>() {
        if event_scene.back_pressed {
            let today = chrono::Local::now().date_naive();
            return Box::new(DayScene::new(
                today,
                all_events,
                all_calendars.clone(),
                strings,
                tz,
            ));
        }
    }

    // Settings scene transitions
    if let Some(settings) = scene.downcast_ref::<SettingsScene>() {
        if settings.back_pressed {
            // Update calendar visibility
            *all_calendars = settings.calendars.clone();
            let today = chrono::Local::now().date_naive();
            return Box::new(DayScene::new(
                today,
                all_events,
                all_calendars.clone(),
                strings,
                tz,
            ));
        }
    }

    scene
}

fn run_with_error(error: &str) {
    let mut canvas = Canvas::new();
    let (input_tx, input_rx) = std::sync::mpsc::channel::<InputEvent>();
    start_input_threads(input_tx, canvas.qtfb_fd());

    canvas.clear();

    let dw = canvas.display_width();
    let title = "reGenda - Configuration Error";
    let tr = canvas.measure_text(title, 52.0);
    let tx = (dw as f32 - tr.width as f32) / 2.0;
    canvas.draw_text_colored(
        canvas::Point2 { x: tx, y: 400.0 },
        title,
        52.0,
        canvas::color::BLACK,
    );

    canvas.draw_multi_line_text(
        Some(60),
        550,
        error,
        50,
        20,
        36.0,
        0.3,
        canvas::color::BLACK,
    );

    let hint = "Place config at /opt/etc/reGenda/config.yml";
    let hr = canvas.measure_text(hint, 36.0);
    let hx = (dw as f32 - hr.width as f32) / 2.0;
    canvas.draw_text_colored(
        canvas::Point2 { x: hx, y: 1000.0 },
        hint,
        36.0,
        canvas::color::MEDIUM_GRAY,
    );

    canvas.update_full();

    // Wait for input or SIGTERM
    loop {
        for _ in input_rx.try_iter() {}
        sleep(Duration::from_millis(500));
    }
}

fn setup_signal_handler() {
    unsafe {
        libc::signal(libc::SIGTERM, handle_sigterm as *const () as libc::sighandler_t);
    }
}

extern "C" fn handle_sigterm(_sig: libc::c_int) {
    info!("Received SIGTERM, exiting gracefully");
    std::process::exit(0);
}
