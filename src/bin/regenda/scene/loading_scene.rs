use super::Scene;
use crate::caldav::FetchStatus;
use crate::canvas::{color, mxcfb_rect, Canvas, Point2};
use crate::i18n::Strings;
use crate::rmpp_hal::types::{InputEvent, MultitouchEvent};
use std::sync::{Arc, Mutex};

pub struct LoadingScene {
    pub fetch_status: Arc<Mutex<FetchStatus>>,
    pub data_ready: bool,
    pub has_error: bool,
    pub error_message: String,
    pub retry_pressed: bool,
    /// Google sources that need OAuth device authorization.
    pub needs_oauth: Vec<String>,
    retry_hitbox: mxcfb_rect,
    strings: &'static Strings,
    drawn: bool,
}

impl LoadingScene {
    pub fn new(fetch_status: Arc<Mutex<FetchStatus>>, strings: &'static Strings) -> Self {
        LoadingScene {
            fetch_status,
            data_ready: false,
            has_error: false,
            error_message: String::new(),
            retry_pressed: false,
            needs_oauth: Vec::new(),
            retry_hitbox: mxcfb_rect::default(),
            strings,
            drawn: false,
        }
    }
}

impl Scene for LoadingScene {
    fn on_input(&mut self, event: InputEvent) {
        if let InputEvent::MultitouchEvent {
            event: MultitouchEvent::Release { finger },
        } = event
        {
            if self.has_error && Canvas::is_hitting(finger.pos, self.retry_hitbox) {
                self.retry_pressed = true;
            }
        }
    }

    fn draw(&mut self, canvas: &mut Canvas) {
        // Check fetch status
        let status = self.fetch_status.lock().unwrap().clone();
        match &status {
            FetchStatus::Done { .. } => {
                self.data_ready = true;
                return;
            }
            FetchStatus::Error { message } => {
                self.has_error = true;
                self.error_message = message.clone();
            }
            FetchStatus::NeedsOAuth { server_names } => {
                self.needs_oauth = server_names.clone();
                return;
            }
            FetchStatus::Loading { .. } => {}
        }

        if self.drawn && !self.has_error {
            return;
        }
        self.drawn = true;

        canvas.clear();

        let dw = canvas.display_width();

        // Title
        canvas.draw_text_colored(
            Point2 { x: 0.0, y: 300.0 },
            "",
            1.0,
            color::WHITE,
        );
        let title = "reGenda";
        let title_rect = canvas.measure_text(title, 120.0);
        let tx = (dw as f32 - title_rect.width as f32) / 2.0;
        canvas.draw_text_colored(
            Point2 { x: tx, y: 400.0 },
            title,
            120.0,
            color::BLACK,
        );

        // Subtitle
        let subtitle = "Calendar for reMarkable";
        let sub_rect = canvas.measure_text(subtitle, 48.0);
        let sx = (dw as f32 - sub_rect.width as f32) / 2.0;
        canvas.draw_text_colored(
            Point2 { x: sx, y: 560.0 },
            subtitle,
            48.0,
            color::MEDIUM_GRAY,
        );

        if self.has_error {
            // Error message
            let err_rect = canvas.measure_text(&self.error_message, 36.0);
            let ex = (dw as f32 - err_rect.width as f32) / 2.0;
            canvas.draw_text_colored(
                Point2 { x: ex.max(40.0), y: 800.0 },
                &self.error_message,
                36.0,
                color::BLACK,
            );

            // Retry button
            self.retry_hitbox = canvas.draw_button(
                Point2 {
                    x: None,
                    y: Some(950),
                },
                self.strings.retry,
                48.0,
                20,
                40,
            );
        } else {
            // Loading message
            let msg = match &status {
                FetchStatus::Loading { message } => message.as_str(),
                _ => self.strings.loading,
            };
            let load_rect = canvas.measure_text(msg, 48.0);
            let lx = (dw as f32 - load_rect.width as f32) / 2.0;
            canvas.draw_text_colored(
                Point2 { x: lx, y: 800.0 },
                msg,
                48.0,
                color::DARK_GRAY,
            );
        }

        canvas.update_full();
    }
}
