use memchr::memchr;

use std::io::{self, BufRead, BufReader, Read};

use crate::utils::trim_trailing_cr;

pub struct LineBuffer<R> {
    buffer: BufReader<R>,
    scratch: Vec<u8>,
    actual_buffer_position: Option<usize>,
}

impl<R: Read> LineBuffer<R> {
    pub fn new(inner: R) -> Self {
        Self {
            buffer: BufReader::new(inner),
            scratch: Vec::new(),
            actual_buffer_position: None,
        }
    }

    pub fn with_capacity(capacity: usize, inner: R) -> Self {
        Self {
            buffer: BufReader::with_capacity(capacity, inner),
            scratch: Vec::with_capacity(capacity),
            actual_buffer_position: None,
        }
    }

    pub fn count_lines(&mut self) -> io::Result<u64> {
        let mut count: u64 = 0;
        let mut current_is_empty = true;

        loop {
            let input = self.buffer.fill_buf()?;
            let len = input.len();

            if len == 0 {
                if !current_is_empty {
                    count += 1;
                }

                return Ok(count);
            }

            match memchr(b'\n', input) {
                None => {
                    self.buffer.consume(len);
                    current_is_empty = false;
                }
                Some(pos) => {
                    count += 1;
                    self.buffer.consume(pos + 1);
                    current_is_empty = true;
                }
            };
        }
    }

    pub fn read_line(&mut self) -> io::Result<Option<&[u8]>> {
        self.scratch.clear();

        if let Some(last_pos) = self.actual_buffer_position.take() {
            self.buffer.consume(last_pos);
        }

        loop {
            let input = self.buffer.fill_buf()?;
            let len = input.len();

            if len == 0 {
                if !self.scratch.is_empty() {
                    return Ok(Some(trim_trailing_cr(&self.scratch)));
                }

                return Ok(None);
            }

            match memchr(b'\n', input) {
                None => {
                    self.scratch.extend_from_slice(input);
                    self.buffer.consume(len);
                }
                Some(pos) => {
                    if self.scratch.is_empty() {
                        self.actual_buffer_position = Some(pos + 1);
                        return Ok(Some(trim_trailing_cr(&self.buffer.buffer()[..pos])));
                    } else {
                        self.scratch.extend_from_slice(&input[..pos]);
                        self.buffer.consume(pos + 1);

                        return Ok(Some(trim_trailing_cr(&self.scratch)));
                    }
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_line() -> io::Result<()> {
        let tests: &[(&[u8], Vec<&[u8]>)] = &[
            (b"", vec![]),
            (b"test", vec![b"test"]),
            (
                b"hello\nwhatever\r\nbye!",
                vec![b"hello", b"whatever", b"bye!"],
            ),
            (
                b"hello\nwhatever\nbye!\n",
                vec![b"hello", b"whatever", b"bye!"],
            ),
            (
                b"hello\nwhatever\r\nbye!\n\n\r\n\n",
                vec![b"hello", b"whatever", b"bye!", b"", b"", b""],
            ),
        ];

        for (data, expected) in tests {
            let mut reader = LineBuffer::new(Cursor::new(data));

            let mut lines = Vec::new();

            while let Some(line) = reader.read_line()? {
                lines.push(line.to_vec());
            }

            assert_eq!(lines, *expected);

            let mut reader = LineBuffer::new(Cursor::new(data));

            assert_eq!(reader.count_lines()?, expected.len() as u64);
        }

        Ok(())
    }
}
