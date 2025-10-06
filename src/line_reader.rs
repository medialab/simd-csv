use memchr::memchr;

use std::io::{self, Read};

use crate::buffer::ScratchBuffer;
use crate::utils::trim_trailing_crlf;

pub struct LineReader<R> {
    inner: ScratchBuffer<R>,
}

impl<R: Read> LineReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: ScratchBuffer::new(inner),
        }
    }

    pub fn with_capacity(capacity: usize, inner: R) -> Self {
        Self {
            inner: ScratchBuffer::with_capacity(capacity, inner),
        }
    }

    pub fn count_lines(&mut self) -> io::Result<u64> {
        let mut count: u64 = 0;
        let mut current_is_empty = true;

        loop {
            let input = self.inner.fill_buf()?;
            let len = input.len();

            if len == 0 {
                if !current_is_empty {
                    count += 1;
                }

                return Ok(count);
            }

            match memchr(b'\n', input) {
                None => {
                    self.inner.consume(len);
                    current_is_empty = false;
                }
                Some(pos) => {
                    count += 1;
                    self.inner.consume(pos + 1);
                    current_is_empty = true;
                }
            };
        }
    }

    pub fn read_line(&mut self) -> io::Result<Option<&[u8]>> {
        self.inner.reset();

        loop {
            let input = self.inner.fill_buf()?;
            let len = input.len();

            if len == 0 {
                if self.inner.has_something_saved() {
                    return Ok(Some(trim_trailing_crlf(self.inner.saved())));
                }

                return Ok(None);
            }

            match memchr(b'\n', input) {
                None => {
                    self.inner.save();
                }
                Some(pos) => {
                    let bytes = self.inner.flush(pos + 1);
                    return Ok(Some(trim_trailing_crlf(bytes)));
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
            let mut reader = LineReader::new(Cursor::new(data));

            let mut lines = Vec::new();

            while let Some(line) = reader.read_line()? {
                lines.push(line.to_vec());
            }

            assert_eq!(lines, *expected);

            let mut reader = LineReader::new(Cursor::new(data));

            assert_eq!(reader.count_lines()?, expected.len() as u64);
        }

        Ok(())
    }
}
