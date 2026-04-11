use super::Scene;
use crate::caldav::Event;
use crate::canvas::{color, mxcfb_rect, Canvas, Point2, Vector2};
use crate::i18n::Strings;
use crate::rmpp_hal::types::{InputEvent, MultitouchEvent};

const HEADER_HEIGHT: u32 = 120;
const MARGIN: u32 = 60;
const CONTENT_TOP: u32 = 160;

pub struct EventScene {
    pub back_pressed: bool,
    event: Event,
    strings: &'static Strings,
    tz: chrono_tz::Tz,
    back_hitbox: mxcfb_rect,
    drawn: bool,
}

impl EventScene {
    pub fn new(event: Event, strings: &'static Strings, tz: chrono_tz::Tz) -> Self {
        EventScene {
            back_pressed: false,
            event,
            strings,
            tz,
            back_hitbox: mxcfb_rect::default(),
            drawn: false,
        }
    }
}

impl Scene for EventScene {
    fn on_input(&mut self, event: InputEvent) {
        if let InputEvent::MultitouchEvent {
            event: MultitouchEvent::Release { finger },
        } = event
        {
            if Canvas::is_hitting(finger.pos, self.back_hitbox) {
                self.back_pressed = true;
            }
        }
    }

    fn draw(&mut self, canvas: &mut Canvas) {
        if self.drawn {
            return;
        }
        self.drawn = true;

        canvas.clear();
        let dw = canvas.display_width();

        // === Header ===
        canvas.fill_rect(
            Point2 {
                x: Some(0),
                y: Some(0),
            },
            Vector2 {
                x: dw,
                y: HEADER_HEIGHT,
            },
            color::HEADER_BG,
        );

        // Back button
        self.back_hitbox = canvas.draw_text_colored(
            Point2 {
                x: 40.0,
                y: 30.0,
            },
            self.strings.back,
            42.0,
            color::WHITE,
        );
        self.back_hitbox.width += 20;
        self.back_hitbox.height += 20;

        // Title text
        let title = self.strings.event_details;
        let tr = canvas.measure_text(title, 46.0);
        let tx = (dw as f32 - tr.width as f32) / 2.0;
        canvas.draw_text_colored(
            Point2 { x: tx, y: 30.0 },
            title,
            46.0,
            color::WHITE,
        );

        // === Content ===
        let mut y = CONTENT_TOP as f32;

        // Event title
        canvas.draw_text_colored(
            Point2 {
                x: MARGIN as f32,
                y,
            },
            &self.event.summary,
            60.0,
            color::BLACK,
        );
        y += 90.0;

        // Divider
        canvas.fill_rect(
            Point2 {
                x: Some(MARGIN as i32),
                y: Some(y as i32),
            },
            Vector2 {
                x: dw - 2 * MARGIN,
                y: 2,
            },
            color::LIGHT_GRAY,
        );
        y += 30.0;

        // Time
        if self.event.all_day {
            canvas.draw_text_colored(
                Point2 {
                    x: MARGIN as f32,
                    y,
                },
                self.strings.allday,
                42.0,
                color::BLACK,
            );
            y += 60.0;
        } else {
            let start_str = format!(
                "{}{}",
                self.strings.start_label,
                self.event.start_datetime_str(&self.tz)
            );
            canvas.draw_text_colored(
                Point2 {
                    x: MARGIN as f32,
                    y,
                },
                &start_str,
                38.0,
                color::BLACK,
            );
            y += 55.0;

            if let Some(end_str) = self.event.end_datetime_str(&self.tz) {
                let end_display = format!("{}{}", self.strings.end_label, end_str);
                canvas.draw_text_colored(
                    Point2 {
                        x: MARGIN as f32,
                        y,
                    },
                    &end_display,
                    38.0,
                    color::BLACK,
                );
                y += 55.0;
            }
        }

        y += 10.0;

        // Location
        if let Some(ref loc) = self.event.location {
            let loc_str = format!("{}{}", self.strings.location_label, loc);
            canvas.draw_text_colored(
                Point2 {
                    x: MARGIN as f32,
                    y,
                },
                &loc_str,
                38.0,
                color::BLACK,
            );
            y += 55.0;
        }

        // Calendar name (use calendar's own color if available)
        let cal_color = self.event.calendar_color.unwrap_or(color::ACCENT);
        let cal_str = format!("{}{}", self.strings.calendar_label, self.event.calendar_name);
        canvas.draw_text_colored(
            Point2 {
                x: MARGIN as f32,
                y,
            },
            &cal_str,
            34.0,
            cal_color,
        );
        y += 55.0;

        // Divider
        canvas.fill_rect(
            Point2 {
                x: Some(MARGIN as i32),
                y: Some(y as i32),
            },
            Vector2 {
                x: dw - 2 * MARGIN,
                y: 2,
            },
            color::LIGHT_GRAY,
        );
        y += 30.0;

        // Description
        if let Some(ref desc) = self.event.description {
            canvas.draw_multi_line_text(
                Some(MARGIN as i32),
                y as i32,
                desc,
                55,
                30,
                34.0,
                0.3,
                color::BLACK,
            );
        }

        canvas.update_full();
    }
}
