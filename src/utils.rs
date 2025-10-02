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
