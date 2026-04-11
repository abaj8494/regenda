use crate::rmpp_hal::display::QtfbDisplay;
pub use crate::rmpp_hal::types::{color, mxcfb_rect, vec2, Point2, Vector2};
pub use image;
use std::os::unix::io::RawFd;

pub struct Canvas {
    display: QtfbDisplay,
}

impl Canvas {
    pub fn new() -> Self {
        Canvas {
            display: QtfbDisplay::new(),
        }
    }

    pub fn qtfb_fd(&self) -> RawFd {
        self.display.socket_fd()
    }

    pub fn display_width(&self) -> u32 {
        self.display.width()
    }

    pub fn display_height(&self) -> u32 {
        self.display.height()
    }

    pub fn clear(&mut self) {
        self.display.clear();
    }

    pub fn update_full(&mut self) -> u32 {
        self.display.full_refresh()
    }

    pub fn update_partial(&mut self, region: &mxcfb_rect) -> u32 {
        self.display.partial_refresh(region)
    }

    pub fn wait_for_update(&mut self, _marker: u32) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    /// Draw text in a specific color at absolute position.
    pub fn draw_text_colored(
        &mut self,
        pos: Point2<f32>,
        text: &str,
        size: f32,
        c: color,
    ) -> mxcfb_rect {
        self.display.draw_text(pos, text, size, c, false)
    }

    /// Measure text without drawing it.
    pub fn measure_text(&mut self, text: &str, size: f32) -> mxcfb_rect {
        self.display
            .draw_text(Point2 { x: 0.0, y: 0.0 }, text, size, color::BLACK, true)
    }

    pub fn draw_text(&mut self, pos: Point2<Option<i32>>, text: &str, size: f32) -> mxcfb_rect {
        let mut pos = pos;
        let dw = self.display_width();
        let dh = self.display_height();

        if pos.x.is_none() || pos.y.is_none() {
            let rect = self.display.draw_text(
                Point2 {
                    x: 0.0,
                    y: dh as f32,
                },
                text,
                size,
                color::BLACK,
                true,
            );

            if pos.x.is_none() {
                pos.x = Some(dw as i32 / 2 - rect.width as i32 / 2);
            }
            if pos.y.is_none() {
                pos.y = Some(dh as i32 / 2 - rect.height as i32 / 2);
            }
        }
        let pos = Point2 {
            x: pos.x.unwrap() as f32,
            y: pos.y.unwrap() as f32,
        };

        self.display.draw_text(pos, text, size, color::BLACK, false)
    }

    pub fn draw_text_at(
        &mut self,
        pos: Point2<Option<i32>>,
        text: &str,
        size: f32,
        c: color,
    ) -> mxcfb_rect {
        let mut pos = pos;
        let dw = self.display_width();
        let dh = self.display_height();

        if pos.x.is_none() || pos.y.is_none() {
            let rect = self.display.draw_text(
                Point2 {
                    x: 0.0,
                    y: dh as f32,
                },
                text,
                size,
                c,
                true,
            );

            if pos.x.is_none() {
                pos.x = Some(dw as i32 / 2 - rect.width as i32 / 2);
            }
            if pos.y.is_none() {
                pos.y = Some(dh as i32 / 2 - rect.height as i32 / 2);
            }
        }
        let pos = Point2 {
            x: pos.x.unwrap() as f32,
            y: pos.y.unwrap() as f32,
        };

        self.display.draw_text(pos, text, size, c, false)
    }

    pub fn draw_multi_line_text(
        &mut self,
        x_pos: Option<i32>,
        y_pos: i32,
        text: &str,
        max_chars_per_line: usize,
        max_lines: usize,
        size: f32,
        line_spacing: f32,
        c: color,
    ) -> mxcfb_rect {
        if text.is_empty() {
            return mxcfb_rect {
                top: 0,
                left: 0,
                width: 0,
                height: 0,
            };
        }

        let x = x_pos.unwrap_or(40);
        let mut text_rects = Vec::new();
        let mut last_text_height = 0;
        let mut last_text_y = y_pos;
        let line_height_extra = (size * line_spacing) as i32;

        // Split on real newlines first, then word-wrap each paragraph
        let paragraphs: Vec<&str> = text.split('\n').collect();

        'outer: for paragraph in &paragraphs {
            let paragraph = paragraph.trim_end_matches('\r');
            if paragraph.is_empty() {
                // Blank line — just advance Y
                last_text_y += last_text_height.max((size as i32) + line_height_extra);
                last_text_height = 0;
                continue;
            }

            let words: Vec<&str> = paragraph.split_whitespace().collect();
            let mut line = String::new();

            for word in &words {
                let candidate = if line.is_empty() {
                    word.to_string()
                } else {
                    format!("{} {}", line, word)
                };

                if candidate.chars().count() > max_chars_per_line && !line.is_empty() {
                    // Flush current line
                    let y = last_text_y + last_text_height;
                    let text_rect = self.draw_text_colored(
                        Point2 { x: x as f32, y: y as f32 },
                        &line,
                        size,
                        c,
                    );
                    last_text_height = text_rect.height as i32 + line_height_extra;
                    last_text_y = text_rect.top as i32;
                    text_rects.push(text_rect);
                    if text_rects.len() >= max_lines {
                        break 'outer;
                    }
                    line = word.to_string();
                } else {
                    line = candidate;
                }
            }

            // Flush remaining text in paragraph
            if !line.is_empty() {
                let y = last_text_y + last_text_height;
                let text_rect = self.draw_text_colored(
                    Point2 { x: x as f32, y: y as f32 },
                    &line,
                    size,
                    c,
                );
                last_text_height = text_rect.height as i32 + line_height_extra;
                last_text_y = text_rect.top as i32;
                text_rects.push(text_rect);
                if text_rects.len() >= max_lines {
                    break 'outer;
                }
            }
        }

        if text_rects.is_empty() {
            mxcfb_rect { top: 0, left: 0, width: 0, height: 0 }
        } else {
            mxcfb_rect {
                top: text_rects.first().unwrap().top,
                left: text_rects.iter().map(|&rec| rec.left).min().unwrap(),
                width: text_rects.iter().map(|&rec| rec.width).max().unwrap(),
                height: text_rects.iter().map(|&rec| rec.height).sum(),
            }
        }
    }

    pub fn fill_rect(
        &mut self,
        pos: Point2<Option<i32>>,
        size: Vector2<u32>,
        clr: color,
    ) -> mxcfb_rect {
        let mut pos = pos;
        let dw = self.display_width();
        let dh = self.display_height();

        if pos.x.is_none() {
            pos.x = Some(dw as i32 / 2 - size.x as i32 / 2);
        }
        if pos.y.is_none() {
            pos.y = Some(dh as i32 / 2 - size.y as i32 / 2);
        }
        let pos = Point2 {
            x: pos.x.unwrap(),
            y: pos.y.unwrap(),
        };

        self.display.fill_rect(pos, size, clr);
        mxcfb_rect {
            top: pos.y as u32,
            left: pos.x as u32,
            width: size.x,
            height: size.y,
        }
    }

    pub fn draw_rect(
        &mut self,
        pos: Point2<Option<i32>>,
        size: Vector2<u32>,
        border_px: u32,
    ) -> mxcfb_rect {
        let mut pos = pos;
        let dw = self.display_width();
        let dh = self.display_height();

        if pos.x.is_none() {
            pos.x = Some(dw as i32 / 2 - size.x as i32 / 2);
        }
        if pos.y.is_none() {
            pos.y = Some(dh as i32 / 2 - size.y as i32 / 2);
        }
        let pos = Point2 {
            x: pos.x.unwrap(),
            y: pos.y.unwrap(),
        };

        self.display
            .draw_rect(pos, size, border_px, color::BLACK);
        mxcfb_rect {
            top: pos.y as u32,
            left: pos.x as u32,
            width: size.x,
            height: size.y,
        }
    }

    pub fn draw_button(
        &mut self,
        pos: Point2<Option<i32>>,
        text: &str,
        font_size: f32,
        vgap: u32,
        hgap: u32,
    ) -> mxcfb_rect {
        let text_rect = self.draw_text(pos, text, font_size);
        self.draw_rect(
            Point2 {
                x: Some((text_rect.left - hgap) as i32),
                y: Some((text_rect.top - vgap) as i32),
            },
            Vector2 {
                x: hgap + text_rect.width + hgap,
                y: vgap + text_rect.height + vgap,
            },
            5,
        )
    }

    pub fn draw_box_button(
        &mut self,
        y_pos: i32,
        y_height: u32,
        text: &str,
        font_size: f32,
    ) -> mxcfb_rect {
        let dw = self.display_width();
        // Draw top and bottom lines
        self.display.draw_line(
            Point2 { x: 0, y: y_pos },
            Point2 {
                x: dw as i32,
                y: y_pos,
            },
            3,
            color::BLACK,
        );
        self.display.draw_line(
            Point2 {
                x: 0,
                y: y_pos + y_height as i32,
            },
            Point2 {
                x: dw as i32,
                y: y_pos + y_height as i32,
            },
            3,
            color::BLACK,
        );
        self.draw_text(
            Point2 {
                x: None,
                y: Some(y_pos + y_height as i32 / 2),
            },
            text,
            font_size,
        );
        mxcfb_rect {
            top: y_pos as u32,
            left: 0,
            width: dw,
            height: y_height,
        }
    }

    pub fn fill_circle(&mut self, cx: i32, cy: i32, radius: i32, c: color) {
        self.display.fill_circle(cx, cy, radius, c);
    }

    pub fn is_hitting(pos: Point2<u16>, hitbox: mxcfb_rect) -> bool {
        (pos.x as u32) >= hitbox.left
            && (pos.x as u32) < (hitbox.left + hitbox.width)
            && (pos.y as u32) >= hitbox.top
            && (pos.y as u32) < (hitbox.top + hitbox.height)
    }
}
