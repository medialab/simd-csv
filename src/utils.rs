use std::borrow::Cow;
use std::io::{self, Read, Seek, SeekFrom};

use memchr::memchr;

#[inline]
pub fn trim_trailing_crlf(slice: &[u8]) -> &[u8] {
    let mut len = slice.len();

    let has_lf = len >= 1 && slice[len - 1] == b'\n';
    let has_crlf = has_lf && len >= 2 && slice[len - 2] == b'\r';

    len -= (has_lf as usize) + (has_crlf as usize);

    &slice[..len]
}

#[inline(always)]
pub fn trim_bom(slice: &[u8]) -> usize {
    if slice.len() >= 3 && &slice[..3] == b"\xef\xbb\xbf" {
        3
    } else {
        0
    }
}

#[inline]
pub fn unquoted(cell: &[u8], quote: u8) -> Option<&[u8]> {
    let len = cell.len();

    if len >= 2 && cell[0] == quote && cell[len - 1] == quote {
        Some(&cell[1..len - 1])
    } else {
        None
    }
}

pub fn unescape(cell: &[u8], quote: u8) -> Cow<[u8]> {
    let len = cell.len();
    let mut output = Vec::new();

    let mut pos: usize = 0;

    while pos < len {
        if let Some(offset) = memchr(quote, &cell[pos..]) {
            if output.is_empty() {
                output.reserve_exact(len);
            }

            output.extend_from_slice(&cell[pos..pos + offset + 1]);

            // NOTE: we assume, next character MUST be a quote
            pos += offset + 2;
        } else {
            break;
        }
    }

    if output.is_empty() {
        Cow::Borrowed(cell)
    } else {
        output.extend_from_slice(&cell[pos..]);
        Cow::Owned(output)
    }
}

pub fn unescape_to(cell: &[u8], quote: u8, out: &mut Vec<u8>) {
    let len = cell.len();
    let mut pos: usize = 0;

    while pos < len {
        if let Some(offset) = memchr(quote, &cell[pos..]) {
            out.extend_from_slice(&cell[pos..pos + offset + 1]);

            // NOTE: we assume, next character MUST be a quote
            pos += offset + 2;
        } else {
            break;
        }
    }

    out.extend_from_slice(&cell[pos..]);
}

pub struct ReverseReader<R> {
    input: R,
    offset: u64,
    ptr: u64,
}

impl<R: Seek + Read> ReverseReader<R> {
    pub fn new(input: R, filesize: u64, offset: u64) -> Self {
        Self {
            input,
            offset,
            ptr: filesize,
        }
    }
}

impl<R: Seek + Read> Read for ReverseReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let buff_size = buf.len() as u64;

        if self.ptr == self.offset {
            return Ok(0);
        }

        if self.offset + buff_size > self.ptr {
            let e = (self.ptr - self.offset) as usize;

            self.input.seek(SeekFrom::Start(self.offset))?;
            self.input.read_exact(&mut buf[0..e])?;

            buf[0..e].reverse();

            self.ptr = self.offset;

            Ok(e)
        } else {
            let new_position = self.ptr - buff_size;

            self.input.seek(SeekFrom::Start(new_position))?;
            self.input.read_exact(buf)?;
            buf.reverse();

            self.ptr -= buff_size;

            Ok(buff_size as usize)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unescape() {
        assert_eq!(unescape(b"test", b'"'), Cow::Borrowed(b"test"));
        assert_eq!(
            unescape(b"\"\"hello\"\"", b'"'),
            Cow::<[u8]>::Owned(b"\"hello\"".to_vec())
        );
        assert_eq!(
            unescape(b"this is \"\"hello\"\" then?", b'"'),
            Cow::<[u8]>::Owned(b"this is \"hello\" then?".to_vec())
        );
    }

    #[test]
    fn test_unescape_to() {
        let mut scratch = Vec::new();

        unescape_to(b"test", b'"', &mut scratch);
        assert_eq!(scratch, b"test");

        scratch.clear();
        unescape_to(b"\"\"hello\"\"", b'"', &mut scratch);
        assert_eq!(scratch, b"\"hello\"");

        scratch.clear();
        unescape_to(b"this is \"\"hello\"\" then?", b'"', &mut scratch);
        assert_eq!(scratch, b"this is \"hello\" then?");
    }
}
