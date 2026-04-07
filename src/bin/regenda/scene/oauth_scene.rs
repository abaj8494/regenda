use super::Scene;
use crate::caldav::google_oauth::{self, DeviceAuthResponse, StoredToken};
use crate::canvas::{color, mxcfb_rect, Canvas, Point2};
use crate::config::ServerConfig;
use crate::i18n::Strings;
use crate::rmpp_hal::types::{InputEvent, MultitouchEvent};
use std::time::{Duration, Instant};

/// Phases of the OAuth device flow.
enum OAuthPhase {
    /// Starting device auth request.
    Starting,
    /// Showing user code, polling for authorization.
    WaitingForUser {
        device_auth: DeviceAuthResponse,
        last_poll: Instant,
        poll_interval: Duration,
    },
    /// Authorization complete.
    Complete,
    /// Fatal error.
    Failed { message: String },
}

pub struct OAuthScene {
    pub auth_complete: bool,
    pub cancel_pressed: bool,
    server_name: String,
    client_id: String,
    client_secret: String,
    phase: OAuthPhase,
    strings: &'static Strings,
    cancel_hitbox: mxcfb_rect,
    needs_redraw: bool,
}

impl OAuthScene {
    pub fn new(
        server_name: String,
        config: &ServerConfig,
        strings: &'static Strings,
    ) -> Self {
        OAuthScene {
            auth_complete: false,
            cancel_pressed: false,
            server_name,
            client_id: config.client_id.clone().unwrap_or_default(),
            client_secret: config.client_secret.clone().unwrap_or_default(),
            phase: OAuthPhase::Starting,
            strings,
            cancel_hitbox: mxcfb_rect::default(),
            needs_redraw: true,
        }
    }
}

impl Scene for OAuthScene {
    fn on_input(&mut self, event: InputEvent) {
        if let InputEvent::MultitouchEvent {
            event: MultitouchEvent::Release { finger },
        } = event
        {
            if Canvas::is_hitting(finger.pos, self.cancel_hitbox) {
                self.cancel_pressed = true;
            }
        }
    }

    fn draw(&mut self, canvas: &mut Canvas) {
        // Handle phase transitions
        match &self.phase {
            OAuthPhase::Starting => {
                // Initiate device auth
                match google_oauth::start_device_auth(&self.client_id) {
                    Ok(device_auth) => {
                        let interval = Duration::from_secs(device_auth.interval.max(5));
                        self.phase = OAuthPhase::WaitingForUser {
                            device_auth,
                            last_poll: Instant::now(),
                            poll_interval: interval,
                        };
                        self.needs_redraw = true;
                    }
                    Err(e) => {
                        self.phase = OAuthPhase::Failed {
                            message: format!("{:?}", e),
                        };
                        self.needs_redraw = true;
                    }
                }
            }
            OAuthPhase::WaitingForUser {
                device_auth,
                last_poll,
                poll_interval,
            } => {
                // Poll if enough time has passed
                if last_poll.elapsed() >= *poll_interval {
                    match google_oauth::poll_for_token(
                        &self.client_id,
                        &self.client_secret,
                        &device_auth.device_code,
                    ) {
                        Ok(Some(token_resp)) => {
                            // Save the token
                            let refresh_token = token_resp
                                .refresh_token
                                .unwrap_or_default();

                            let stored = StoredToken {
                                access_token: token_resp.access_token,
                                refresh_token,
                                client_id: self.client_id.clone(),
                                client_secret: self.client_secret.clone(),
                            };

                            if let Err(e) =
                                google_oauth::save_stored_token(&self.server_name, &stored)
                            {
                                log::error!("Failed to save token: {:?}", e);
                            }

                            self.phase = OAuthPhase::Complete;
                            self.auth_complete = true;
                            self.needs_redraw = true;
                        }
                        Ok(None) => {
                            // Still pending, update last_poll
                            // We need mutable access, so reconstruct
                            let da = device_auth.clone();
                            let pi = *poll_interval;
                            self.phase = OAuthPhase::WaitingForUser {
                                device_auth: da,
                                last_poll: Instant::now(),
                                poll_interval: pi,
                            };
                        }
                        Err(e) => {
                            self.phase = OAuthPhase::Failed {
                                message: format!("{}", e),
                            };
                            self.needs_redraw = true;
                        }
                    }
                }
            }
            OAuthPhase::Complete => {
                // Already handled
            }
            OAuthPhase::Failed { .. } => {}
        }

        if !self.needs_redraw {
            return;
        }
        self.needs_redraw = false;

        canvas.clear();
        let dw = canvas.display_width();

        // Title
        let title = "Google Calendar Authorization";
        let tr = canvas.measure_text(title, 52.0);
        let tx = (dw as f32 - tr.width as f32) / 2.0;
        canvas.draw_text_colored(Point2 { x: tx, y: 200.0 }, title, 52.0, color::BLACK);

        // Server name
        let server_str = format!("Source: {}", self.server_name);
        let sr = canvas.measure_text(&server_str, 36.0);
        let sx = (dw as f32 - sr.width as f32) / 2.0;
        canvas.draw_text_colored(
            Point2 { x: sx, y: 290.0 },
            &server_str,
            36.0,
            color::MEDIUM_GRAY,
        );

        match &self.phase {
            OAuthPhase::Starting => {
                let msg = "Connecting to Google...";
                let mr = canvas.measure_text(msg, 44.0);
                let mx = (dw as f32 - mr.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: mx, y: 500.0 },
                    msg,
                    44.0,
                    color::DARK_GRAY,
                );
            }
            OAuthPhase::WaitingForUser { device_auth, .. } => {
                // Instructions
                let inst1 = "On your phone or computer, go to:";
                let i1r = canvas.measure_text(inst1, 40.0);
                let i1x = (dw as f32 - i1r.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: i1x, y: 450.0 },
                    inst1,
                    40.0,
                    color::BLACK,
                );

                // Verification URL (large, prominent)
                let url = &device_auth.verification_url;
                let ur = canvas.measure_text(url, 52.0);
                let ux = (dw as f32 - ur.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: ux, y: 540.0 },
                    url,
                    52.0,
                    color::ACCENT,
                );

                // "and enter this code:"
                let inst2 = "and enter this code:";
                let i2r = canvas.measure_text(inst2, 40.0);
                let i2x = (dw as f32 - i2r.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: i2x, y: 660.0 },
                    inst2,
                    40.0,
                    color::BLACK,
                );

                // User code (very large)
                let code = &device_auth.user_code;
                let cr = canvas.measure_text(code, 100.0);
                let cx = (dw as f32 - cr.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: cx, y: 760.0 },
                    code,
                    100.0,
                    color::BLACK,
                );

                // Waiting message
                let wait = "Waiting for authorization...";
                let wr = canvas.measure_text(wait, 36.0);
                let wx = (dw as f32 - wr.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: wx, y: 950.0 },
                    wait,
                    36.0,
                    color::MEDIUM_GRAY,
                );

                // Hint
                let hint = "This screen will update automatically once authorized.";
                let hr = canvas.measure_text(hint, 30.0);
                let hx = (dw as f32 - hr.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: hx, y: 1020.0 },
                    hint,
                    30.0,
                    color::MEDIUM_GRAY,
                );
            }
            OAuthPhase::Complete => {
                let msg = "Authorization successful!";
                let mr = canvas.measure_text(msg, 52.0);
                let mx = (dw as f32 - mr.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: mx, y: 500.0 },
                    msg,
                    52.0,
                    color::ACCENT,
                );

                let msg2 = "Loading your calendars...";
                let m2r = canvas.measure_text(msg2, 40.0);
                let m2x = (dw as f32 - m2r.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: m2x, y: 600.0 },
                    msg2,
                    40.0,
                    color::DARK_GRAY,
                );
            }
            OAuthPhase::Failed { message } => {
                let msg = "Authorization failed:";
                let mr = canvas.measure_text(msg, 44.0);
                let mx = (dw as f32 - mr.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 { x: mx, y: 500.0 },
                    msg,
                    44.0,
                    color::BLACK,
                );

                canvas.draw_multi_line_text(
                    Some(60),
                    600,
                    message,
                    50,
                    10,
                    34.0,
                    0.3,
                    color::BLACK,
                );
            }
        }

        // Cancel button (always visible)
        self.cancel_hitbox = canvas.draw_button(
            Point2 {
                x: None,
                y: Some(1800),
            },
            "Cancel",
            44.0,
            20,
            40,
        );

        canvas.update_full();
    }
}
