use std::fmt;
use std::ops::Index;

use crate::debug;

fn trim_end(slice: &[u8]) -> &[u8] {
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

pub struct ZeroCopyByteRecord<'a> {
    slice: &'a [u8],
    seps: &'a [usize],
}

impl<'a> ZeroCopyByteRecord<'a> {
    #[inline]
    pub(crate) fn new(slice: &'a [u8], seps: &'a [usize]) -> Self {
        Self {
            slice: trim_end(slice),
            seps,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.seps.len() + 1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.slice
    }

    #[inline]
    pub fn iter(&self) -> ZeroCopyRecordIter<'_> {
        ZeroCopyRecordIter {
            record: self,
            current_sep_index: 0,
            offset: 0,
        }
    }
}

impl<'a> fmt::Debug for ZeroCopyByteRecord<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZeroCopyByteRecord(")?;
        f.debug_list()
            .entries(self.iter().map(debug::Bytes))
            .finish()?;
        write!(f, ")")?;
        Ok(())
    }
}

pub struct ZeroCopyRecordIter<'a> {
    record: &'a ZeroCopyByteRecord<'a>,
    current_sep_index: usize,
    offset: usize,
}

impl<'a> Iterator for ZeroCopyRecordIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_sep_index > self.record.seps.len() {
            return None;
        }

        // Terminal flush
        if self.current_sep_index == self.record.seps.len() {
            let slice = &self.record.slice[self.offset..];
            self.current_sep_index += 1;

            return Some(slice);
        }

        let sep = self.record.seps[self.current_sep_index];
        let offset = self.offset;
        self.current_sep_index += 1;
        self.offset = sep + 1;

        Some(&self.record.slice[offset..sep])
    }
}

#[derive(Default, Clone, PartialEq)]
pub struct ByteRecord {
    data: Vec<u8>,
    ends: Vec<usize>,
}

impl ByteRecord {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
        self.ends.clear();
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn iter(&self) -> ByteRecordIter<'_> {
        ByteRecordIter {
            record: self,
            current: 0,
        }
    }

    #[inline]
    pub fn push_field(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
        self.finalize_field();
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if index >= self.ends.len() {
            None
        } else if index == 0 {
            let end = self.ends[index];
            Some(&self.data[0..end])
        } else {
            let start = self.ends[index - 1];
            let end = self.ends[index];
            Some(&self.data[start..end])
        }
    }

    #[inline(always)]
    pub(crate) fn extend_from_slice(&mut self, slice: &[u8]) {
        self.data.extend_from_slice(slice);
    }

    #[inline(always)]
    pub(crate) fn push_byte(&mut self, byte: u8) {
        self.data.push(byte);
    }

    #[inline(always)]
    pub(crate) fn pop_trailing_carriage_return(&mut self) {
        if matches!(self.data.last(), Some(c) if *c == b'\r') {
            self.data.pop();
        }
    }

    #[inline(always)]
    pub(crate) fn finalize_field(&mut self) {
        self.ends.push(self.data.len());
    }
}

impl Index<usize> for ByteRecord {
    type Output = [u8];

    #[inline]
    fn index(&self, i: usize) -> &[u8] {
        self.get(i).unwrap()
    }
}

impl fmt::Debug for ByteRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ByteRecord(")?;
        f.debug_list()
            .entries(self.iter().map(debug::Bytes))
            .finish()?;
        write!(f, ")")?;
        Ok(())
    }
}

pub struct ByteRecordIter<'a> {
    record: &'a ByteRecord,
    current: usize,
}

impl<'a> Iterator for ByteRecordIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.record.ends.len() {
            None
        } else {
            let (start, end) = if self.current == 0 {
                (0, self.record.ends[self.current])
            } else {
                (
                    self.record.ends[self.current - 1],
                    self.record.ends[self.current],
                )
            };

            self.current += 1;

            Some(&self.record.data[start..end])
        }
    }
}

#[macro_export]
macro_rules! brec {
    () => {{
        ByteRecord::new()
    }};

    ($($x: expr),*) => {{
        let mut r = ByteRecord::new();

        $(
            r.push_field($x.as_bytes());
        )*

        r
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_byte_record() {
        let record = ZeroCopyByteRecord::new(b"name,surname,age", &[4, 12]);

        assert_eq!(record.len(), 3);

        let expected: Vec<&[u8]> = vec![b"name", b"surname", b"age"];
        assert_eq!(record.iter().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn test_byte_record() {
        let mut record = ByteRecord::new();

        assert_eq!(record.len(), 0);
        assert_eq!(record.is_empty(), true);
        assert_eq!(record.get(0), None);

        record.push_field(b"name");
        record.push_field(b"surname");
        record.push_field(b"age");

        let expected: Vec<&[u8]> = vec![b"name", b"surname", b"age"];
        assert_eq!(record.iter().collect::<Vec<_>>(), expected);

        assert_eq!(record.get(0), Some::<&[u8]>(b"name"));
        assert_eq!(record.get(1), Some::<&[u8]>(b"surname"));
        assert_eq!(record.get(2), Some::<&[u8]>(b"age"));
        assert_eq!(record.get(3), None);
    }
}
