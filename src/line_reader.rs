use memchr::memchr;

use std::io::{self, BufReader, Read};

use crate::buffer::ScratchBuffer;
use crate::utils::trim_trailing_crlf;

/// A zero-copy & optimized line reader.
///
/// This reader recognizes both `LF` & `CRLF` line terminators, but not single
/// `CR`.
pub struct LineReader<R> {
    inner: ScratchBuffer<R>,
}

impl<R: Read> LineReader<R> {
    /// Create a new reader with default using the provided reader implementing
    /// [`std::io::Read`].
    ///
    /// Avoid providing a buffered reader because buffering will be handled for
    /// you by the [`LineReader`].
    pub fn from_reader(inner: R) -> Self {
        Self {
            inner: ScratchBuffer::new(inner),
        }
    }

    /// Create a new reader with provided buffer capacity and using the provided
    /// reader implementing [`std::io::Read`].
    ///
    /// Avoid providing a buffered reader because buffering will be handled for
    /// you by the [`LineReader`].
    pub fn with_capacity(capacity: usize, inner: R) -> Self {
        Self {
            inner: ScratchBuffer::with_capacity(capacity, inner),
        }
    }

    /// Consume the reader to count the number of lines as fast as possible.
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

    /// Attempt to read the next line from underlying reader.
    ///
    /// Will return `None` if the end of stream was reached.
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

    /// Return the current byte offset of the reader.
    #[inline(always)]
    pub fn position(&self) -> u64 {
        self.inner.position()
    }

    /// Return the underlying [`BufReader`].
    #[inline(always)]
    pub fn into_bufreader(self) -> BufReader<R> {
        self.inner.into_bufreader()
    }

    /// Return the underlying reader.
    ///
    /// **BEWARE**: Already buffered data will be lost!
    #[inline(always)]
    pub fn into_inner(self) -> R {
        self.inner.into_bufreader().into_inner()
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
            let mut reader = LineReader::from_reader(Cursor::new(data));

            let mut lines = Vec::new();

            while let Some(line) = reader.read_line()? {
                lines.push(line.to_vec());
            }

            assert_eq!(lines, *expected);

            let mut reader = LineReader::from_reader(Cursor::new(data));

            assert_eq!(reader.count_lines()?, expected.len() as u64);
        }

        Ok(())
    }
}
