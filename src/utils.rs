use std::borrow::Cow;

use memchr::memchr;

#[inline]
pub fn trim_trailing_cr(line: &[u8]) -> &[u8] {
    let len = line.len();

    if !line.is_empty() && line[len - 1] == b'\r' {
        &line[..len - 1]
    } else {
        line
    }
}

pub fn trim_trailing_crlf(slice: &[u8]) -> &[u8] {
    let len = slice.len();

    match len {
        0 => slice,
        1 => {
            if slice[0] == b'\n' {
                b""
            } else {
                slice
            }
        }
        _ => {
            if &slice[len - 2..] == b"\r\n" {
                &slice[..len - 2]
            } else if slice[len - 1] == b'\n' {
                &slice[..len - 1]
            } else {
                slice
            }
        }
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
}
