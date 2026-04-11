use super::Scene;
use crate::caldav::{CalendarInfo, Event};
use crate::canvas::{color, mxcfb_rect, Canvas, Point2, Vector2};
use crate::i18n::Strings;
use crate::rmpp_hal::types::{InputEvent, MultitouchEvent};
use chrono::{Datelike, NaiveDate};

const HEADER_HEIGHT: u32 = 120;
const NAV_HEIGHT: u32 = 100;
const BOTTOM_HEIGHT: u32 = 100;
const EVENT_ROW_HEIGHT: u32 = 160;
const MARGIN: u32 = 40;

pub struct DayScene {
    pub current_date: NaiveDate,
    pub events: Vec<Event>,
    all_events: Vec<Event>,
    pub calendars: Vec<CalendarInfo>,
    pub go_to_month: bool,
    pub go_to_settings: bool,
    pub go_to_event: Option<usize>,
    pub refresh_pressed: bool,
    pub exit_pressed: bool,
    strings: &'static Strings,
    tz: chrono_tz::Tz,
    page: usize,
    events_per_page: usize,
    // Hitboxes
    prev_hitbox: mxcfb_rect,
    next_hitbox: mxcfb_rect,
    today_hitbox: mxcfb_rect,
    month_hitbox: mxcfb_rect,
    settings_hitbox: mxcfb_rect,
    refresh_hitbox: mxcfb_rect,
    event_hitboxes: Vec<mxcfb_rect>,
    page_prev_hitbox: mxcfb_rect,
    page_next_hitbox: mxcfb_rect,
    needs_redraw: bool,
}

impl DayScene {
    pub fn new(
        date: NaiveDate,
        all_events: &[Event],
        calendars: Vec<CalendarInfo>,
        strings: &'static Strings,
        tz: chrono_tz::Tz,
    ) -> Self {
        let events = filter_events(all_events, date, &calendars, &tz);
        let list_height = 2160 - HEADER_HEIGHT - NAV_HEIGHT - BOTTOM_HEIGHT;
        let events_per_page = (list_height / EVENT_ROW_HEIGHT) as usize;

        DayScene {
            current_date: date,
            events,
            all_events: all_events.to_vec(),
            calendars,
            go_to_month: false,
            go_to_settings: false,
            go_to_event: None,
            refresh_pressed: false,
            exit_pressed: false,
            strings,
            tz,
            page: 0,
            events_per_page,
            prev_hitbox: mxcfb_rect::default(),
            next_hitbox: mxcfb_rect::default(),
            today_hitbox: mxcfb_rect::default(),
            month_hitbox: mxcfb_rect::default(),
            settings_hitbox: mxcfb_rect::default(),
            refresh_hitbox: mxcfb_rect::default(),
            event_hitboxes: Vec::new(),
            page_prev_hitbox: mxcfb_rect::default(),
            page_next_hitbox: mxcfb_rect::default(),
            needs_redraw: true,
        }
    }

    pub fn update_events(&mut self, all_events: &[Event]) {
        self.events = filter_events(all_events, self.current_date, &self.calendars, &self.tz);
        self.page = 0;
        self.needs_redraw = true;
    }

    fn total_pages(&self) -> usize {
        if self.events.is_empty() {
            1
        } else {
            (self.events.len() + self.events_per_page - 1) / self.events_per_page
        }
    }
}

impl Scene for DayScene {
    fn on_input(&mut self, event: InputEvent) {
        if let InputEvent::MultitouchEvent {
            event: MultitouchEvent::Release { finger },
        } = event
        {
            let pos = finger.pos;

            if Canvas::is_hitting(pos, self.prev_hitbox) {
                self.current_date -= chrono::Duration::days(1);
                self.page = 0;
                self.needs_redraw = true;
            } else if Canvas::is_hitting(pos, self.next_hitbox) {
                self.current_date += chrono::Duration::days(1);
                self.page = 0;
                self.needs_redraw = true;
            } else if Canvas::is_hitting(pos, self.today_hitbox) {
                self.current_date = chrono::Local::now().date_naive();
                self.page = 0;
                self.needs_redraw = true;
            } else if Canvas::is_hitting(pos, self.month_hitbox) {
                self.go_to_month = true;
            } else if Canvas::is_hitting(pos, self.settings_hitbox) {
                self.go_to_settings = true;
            } else if Canvas::is_hitting(pos, self.refresh_hitbox) {
                self.refresh_pressed = true;
            } else if Canvas::is_hitting(pos, self.page_prev_hitbox) && self.page > 0 {
                self.page -= 1;
                self.needs_redraw = true;
            } else if Canvas::is_hitting(pos, self.page_next_hitbox)
                && self.page + 1 < self.total_pages()
            {
                self.page += 1;
                self.needs_redraw = true;
            } else {
                // Check event hitboxes
                for (i, hitbox) in self.event_hitboxes.iter().enumerate() {
                    if Canvas::is_hitting(pos, *hitbox) {
                        let event_idx = self.page * self.events_per_page + i;
                        if event_idx < self.events.len() {
                            self.go_to_event = Some(event_idx);
                        }
                        break;
                    }
                }
            }
        }
    }

    fn draw(&mut self, canvas: &mut Canvas) {
        if !self.needs_redraw {
            return;
        }
        self.needs_redraw = false;

        // Re-filter events for current date
        self.events = filter_events(&self.all_events, self.current_date, &self.calendars, &self.tz);

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

        // Date string
        let today = chrono::Local::now().date_naive();
        let dow_idx = self.current_date.weekday().num_days_from_monday() as usize;
        let day_name = self.strings.days_of_week[dow_idx];
        let month_idx = self.current_date.month0() as usize;
        let month_name = self.strings.months[month_idx];
        let date_str = format!(
            "{}, {} {}",
            day_name,
            self.current_date.day(),
            month_name
        );

        let is_today = self.current_date == today;
        let date_display = if is_today {
            format!("{} ({})", date_str, self.strings.today)
        } else {
            date_str
        };

        canvas.draw_text_colored(
            Point2 {
                x: MARGIN as f32,
                y: 25.0,
            },
            &date_display,
            52.0,
            color::WHITE,
        );

        // Settings button (top right)
        let settings_text = self.strings.settings;
        let settings_rect = canvas.measure_text(settings_text, 36.0);
        let sx = dw as f32 - settings_rect.width as f32 - MARGIN as f32;
        self.settings_hitbox = canvas.draw_text_colored(
            Point2 { x: sx, y: 35.0 },
            settings_text,
            36.0,
            color::WHITE,
        );
        self.settings_hitbox.width += 20;
        self.settings_hitbox.height += 20;

        // Refresh button
        let refresh_text = self.strings.refresh;
        let refresh_rect = canvas.measure_text(refresh_text, 36.0);
        let rx = sx - refresh_rect.width as f32 - 40.0;
        self.refresh_hitbox = canvas.draw_text_colored(
            Point2 { x: rx, y: 35.0 },
            refresh_text,
            36.0,
            color::WHITE,
        );
        self.refresh_hitbox.width += 20;
        self.refresh_hitbox.height += 20;

        // === Event list ===
        let list_top = HEADER_HEIGHT;
        let list_bottom = 2160 - NAV_HEIGHT - BOTTOM_HEIGHT;
        self.event_hitboxes.clear();

        if self.events.is_empty() {
            let no_evt = self.strings.no_events;
            let nr = canvas.measure_text(no_evt, 44.0);
            let nx = (dw as f32 - nr.width as f32) / 2.0;
            canvas.draw_text_colored(
                Point2 {
                    x: nx,
                    y: (list_top + 200) as f32,
                },
                no_evt,
                44.0,
                color::MEDIUM_GRAY,
            );
        } else {
            let start_idx = self.page * self.events_per_page;
            let end_idx = (start_idx + self.events_per_page).min(self.events.len());

            for (i, event) in self.events[start_idx..end_idx].iter().enumerate() {
                let y = list_top + (i as u32) * EVENT_ROW_HEIGHT;

                // Event row hitbox
                let hitbox = mxcfb_rect {
                    top: y,
                    left: 0,
                    width: dw,
                    height: EVENT_ROW_HEIGHT,
                };
                self.event_hitboxes.push(hitbox);

                // Time column
                let time_str = if event.all_day {
                    self.strings.allday.to_string()
                } else {
                    let start = event.start_time_str(&self.tz);
                    match event.end_time_str(&self.tz) {
                        Some(end) => format!("{}\n{}", start, end),
                        None => start,
                    }
                };

                canvas.draw_text_colored(
                    Point2 {
                        x: MARGIN as f32,
                        y: (y + 20) as f32,
                    },
                    &time_str.lines().next().unwrap_or(""),
                    36.0,
                    color::DARK_GRAY,
                );
                if let Some(end_line) = time_str.lines().nth(1) {
                    canvas.draw_text_colored(
                        Point2 {
                            x: MARGIN as f32,
                            y: (y + 65) as f32,
                        },
                        end_line,
                        32.0,
                        color::MEDIUM_GRAY,
                    );
                }

                // Title + details column
                let text_x = 240.0;
                canvas.draw_text_colored(
                    Point2 {
                        x: text_x,
                        y: (y + 20) as f32,
                    },
                    &event.summary,
                    42.0,
                    color::BLACK,
                );

                // Location
                if let Some(ref loc) = event.location {
                    canvas.draw_text_colored(
                        Point2 {
                            x: text_x,
                            y: (y + 72) as f32,
                        },
                        loc,
                        30.0,
                        color::MEDIUM_GRAY,
                    );
                }

                // Calendar name (use calendar's own color if available)
                let cal_color = event.calendar_color.unwrap_or(color::ACCENT);
                canvas.draw_text_colored(
                    Point2 {
                        x: text_x,
                        y: (y + 110) as f32,
                    },
                    &event.calendar_name,
                    28.0,
                    cal_color,
                );

                // Divider line
                if i + 1 < end_idx - start_idx {
                    canvas.fill_rect(
                        Point2 {
                            x: Some(MARGIN as i32),
                            y: Some((y + EVENT_ROW_HEIGHT - 2) as i32),
                        },
                        Vector2 {
                            x: dw - 2 * MARGIN,
                            y: 1,
                        },
                        color::LIGHT_GRAY,
                    );
                }
            }

            // Page indicator if needed
            if self.total_pages() > 1 {
                let page_str = format!(
                    "{}{}/{}",
                    self.strings.page,
                    self.page + 1,
                    self.total_pages()
                );
                let pr = canvas.measure_text(&page_str, 32.0);
                let px = (dw as f32 - pr.width as f32) / 2.0;
                canvas.draw_text_colored(
                    Point2 {
                        x: px,
                        y: (list_bottom - 50) as f32,
                    },
                    &page_str,
                    32.0,
                    color::MEDIUM_GRAY,
                );

                // Page nav hitboxes
                self.page_prev_hitbox = mxcfb_rect {
                    top: list_bottom - 60,
                    left: 0,
                    width: dw / 3,
                    height: 60,
                };
                self.page_next_hitbox = mxcfb_rect {
                    top: list_bottom - 60,
                    left: dw * 2 / 3,
                    width: dw / 3,
                    height: 60,
                };
            }
        }

        // === Navigation bar ===
        let nav_y = (2160 - NAV_HEIGHT - BOTTOM_HEIGHT) as i32;

        // Divider
        canvas.fill_rect(
            Point2 {
                x: Some(0),
                y: Some(nav_y),
            },
            Vector2 { x: dw, y: 2 },
            color::LIGHT_GRAY,
        );

        let third = dw / 3;

        // Prev button
        let prev_text = self.strings.prev_day;
        let prev_r = canvas.measure_text(prev_text, 44.0);
        canvas.draw_text_colored(
            Point2 {
                x: (third as f32 - prev_r.width as f32) / 2.0,
                y: (nav_y + 25) as f32,
            },
            prev_text,
            44.0,
            color::BLACK,
        );
        self.prev_hitbox = mxcfb_rect {
            top: nav_y as u32,
            left: 0,
            width: third,
            height: NAV_HEIGHT,
        };

        // Today button
        let today_text = self.strings.today;
        let today_r = canvas.measure_text(today_text, 44.0);
        canvas.draw_text_colored(
            Point2 {
                x: third as f32 + (third as f32 - today_r.width as f32) / 2.0,
                y: (nav_y + 25) as f32,
            },
            today_text,
            44.0,
            if is_today {
                color::ACCENT
            } else {
                color::BLACK
            },
        );
        self.today_hitbox = mxcfb_rect {
            top: nav_y as u32,
            left: third,
            width: third,
            height: NAV_HEIGHT,
        };

        // Next button
        let next_text = self.strings.next_day;
        let next_r = canvas.measure_text(next_text, 44.0);
        canvas.draw_text_colored(
            Point2 {
                x: 2.0 * third as f32 + (third as f32 - next_r.width as f32) / 2.0,
                y: (nav_y + 25) as f32,
            },
            next_text,
            44.0,
            color::BLACK,
        );
        self.next_hitbox = mxcfb_rect {
            top: nav_y as u32,
            left: 2 * third,
            width: third,
            height: NAV_HEIGHT,
        };

        // === Bottom bar ===
        let bottom_y = (2160 - BOTTOM_HEIGHT) as i32;
        canvas.fill_rect(
            Point2 {
                x: Some(0),
                y: Some(bottom_y),
            },
            Vector2 { x: dw, y: 2 },
            color::LIGHT_GRAY,
        );

        let month_text = self.strings.month_view;
        let mr = canvas.measure_text(month_text, 40.0);
        canvas.draw_text_colored(
            Point2 {
                x: (dw as f32 - mr.width as f32) / 2.0,
                y: (bottom_y + 25) as f32,
            },
            month_text,
            40.0,
            color::BLACK,
        );
        self.month_hitbox = mxcfb_rect {
            top: bottom_y as u32,
            left: 0,
            width: dw,
            height: BOTTOM_HEIGHT,
        };

        canvas.update_full();
    }
}

fn filter_events(
    all_events: &[Event],
    date: NaiveDate,
    calendars: &[CalendarInfo],
    tz: &chrono_tz::Tz,
) -> Vec<Event> {
    let visible_cals: Vec<&str> = calendars
        .iter()
        .filter(|c| c.visible)
        .map(|c| c.name.as_str())
        .collect();

    let mut filtered: Vec<Event> = all_events
        .iter()
        .filter(|e| {
            e.date_in_tz(tz) == date
                && (visible_cals.is_empty() || visible_cals.contains(&e.calendar_name.as_str()))
        })
        .cloned()
        .collect();

    filtered.sort();
    filtered
}
