use std::fmt;
use std::ops::Index;

use crate::debug;
use crate::utils::trim_trailing_crlf;

pub struct ZeroCopyByteRecord<'a> {
    slice: &'a [u8],
    seps: &'a [usize],
}

impl<'a> ZeroCopyByteRecord<'a> {
    #[inline]
    pub(crate) fn new(slice: &'a [u8], seps: &'a [usize]) -> Self {
        Self {
            slice: trim_trailing_crlf(slice),
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
        let seps = &self.record.seps;
        let len = seps.len();

        if self.current_sep_index > len {
            return None;
        }

        let offset = self.offset;

        let end = if self.current_sep_index < len {
            let sep = seps[self.current_sep_index];
            self.offset = sep + 1;
            sep
        } else {
            // Last field
            self.offset = self.record.slice.len();
            self.offset
        };

        self.current_sep_index += 1;

        Some(&self.record.slice[offset..end])
    }
}

#[derive(Default, Clone)]
pub struct ByteRecord {
    pub(crate) data: Vec<u8>,
    pub(crate) bounds: Vec<(usize, usize)>,
    start: usize,
}

impl ByteRecord {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bounds.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
        self.bounds.clear();
        self.start = 0;
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
        self.bounds
            .get(index)
            .copied()
            .map(|(start, end)| &self.data[start..end])
    }

    #[inline(always)]
    pub(crate) fn extend_from_slice(&mut self, slice: &[u8]) {
        self.data.extend_from_slice(slice);
    }

    #[inline(always)]
    pub(crate) fn push_byte(&mut self, byte: u8) {
        self.data.push(byte);
    }

    #[inline]
    pub(crate) fn finalize_field(&mut self) {
        let start = self.start;
        self.start = self.data.len();

        self.bounds.push((start, self.start));
    }

    #[inline]
    pub(crate) fn finalize_field_including_delimiter(&mut self, offset: usize) {
        let start = self.start;
        self.start = self.data.len() + offset;

        self.bounds.push((start, self.start));

        self.start += 1;
    }

    #[inline(always)]
    pub(crate) fn bump(&mut self) {
        self.start += 1;
    }
}

impl PartialEq for ByteRecord {
    fn eq(&self, other: &Self) -> bool {
        if self.bounds.len() != other.bounds.len() {
            return false;
        }

        self.iter()
            .zip(other.iter())
            .all(|(self_cell, other_cell)| self_cell == other_cell)
    }
}

impl Index<usize> for ByteRecord {
    type Output = [u8];

    #[inline]
    fn index(&self, i: usize) -> &[u8] {
        self.get(i).unwrap()
    }
}

impl<I, T> From<I> for ByteRecord
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    fn from(value: I) -> Self {
        let mut record = Self::new();

        for cell in value.into_iter() {
            record.push_field(cell.as_ref());
        }

        record
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

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.record.bounds.len() {
            None
        } else {
            let (start, end) = self.record.bounds[self.current];

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
