use std::borrow::Cow;
use std::fmt;
use std::ops::Index;

use crate::debug;
use crate::utils::{trim_trailing_crlf, unescape};

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

    #[inline(always)]
    pub fn len(&self) -> usize {
        // NOTE: an empty zero copy record cannot be constructed,
        // by definition.
        self.seps.len() + 1
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        false
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        self.slice
    }

    #[inline]
    pub fn iter(&self) -> ZeroCopyRecordIter<'_> {
        ZeroCopyRecordIter {
            record: self,
            current: 0,
        }
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        let len = self.seps.len();

        if index > len {
            return None;
        }

        let start = if index == 0 {
            0
        } else {
            self.seps[index - 1] + 1
        };

        let end = if index == len {
            self.slice.len()
        } else {
            self.seps[index]
        };

        Some(&self.slice[start..end])
    }

    #[inline]
    pub fn is_quoted(&self, index: usize, quote: u8) -> bool {
        let cell = self.get(index).unwrap();
        cell.len() > 1 && cell[0] == quote
    }

    #[inline]
    pub fn unquote(&self, index: usize, quote: u8) -> Option<&[u8]> {
        self.get(index).map(|cell| {
            let len = cell.len();

            if len > 1 && cell[0] == quote {
                &cell[1..len - 1]
            } else {
                cell
            }
        })
    }

    #[inline]
    pub fn unescape(&self, index: usize, quote: u8) -> Option<Cow<[u8]>> {
        self.unquote(index, quote).map(|cell| unescape(cell, quote))
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
    current: usize,
}

impl<'a> Iterator for ZeroCopyRecordIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let cell = self.record.get(self.current);

        if cell.is_some() {
            self.current += 1;
        }

        cell
    }
}

impl<'a> Index<usize> for ZeroCopyByteRecord<'a> {
    type Output = [u8];

    #[inline]
    fn index(&self, i: usize) -> &[u8] {
        self.get(i).unwrap()
    }
}

#[derive(Default, Clone)]
pub struct ByteRecord {
    data: Vec<u8>,
    bounds: Vec<(usize, usize)>,
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
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn iter(&self) -> ByteRecordIter<'_> {
        ByteRecordIter {
            record: self,
            current_forward: 0,
            current_reverse: self.len(),
        }
    }

    #[inline(always)]
    pub fn push_field(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);

        let bounds_len = self.bounds.len();

        let start = if bounds_len == 0 {
            0
        } else {
            self.bounds[bounds_len - 1].1
        };

        self.bounds.push((start, self.data.len()));
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        self.bounds
            .get(index)
            .copied()
            .map(|(start, end)| &self.data[start..end])
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

impl<T: AsRef<[u8]>> Extend<T> for ByteRecord {
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for x in iter {
            self.push_field(x.as_ref());
        }
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

impl<'r> IntoIterator for &'r ByteRecord {
    type IntoIter = ByteRecordIter<'r>;
    type Item = &'r [u8];

    #[inline]
    fn into_iter(self) -> ByteRecordIter<'r> {
        self.iter()
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
    current_forward: usize,
    current_reverse: usize,
}

impl<'a> ExactSizeIterator for ByteRecordIter<'a> {}

impl<'a> Iterator for ByteRecordIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_forward == self.current_reverse {
            None
        } else {
            let (start, end) = self.record.bounds[self.current_forward];

            self.current_forward += 1;

            Some(&self.record.data[start..end])
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.current_reverse - self.current_forward;

        (size, Some(size))
    }

    #[inline]
    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.len()
    }
}

impl<'a> DoubleEndedIterator for ByteRecordIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_forward == self.current_reverse {
            None
        } else {
            self.current_reverse -= 1;

            let (start, end) = self.record.bounds[self.current_reverse];

            Some(&self.record.data[start..end])
        }
    }
}

pub(crate) struct ByteRecordBuilder<'r> {
    record: &'r mut ByteRecord,
    start: usize,
}

impl<'r> ByteRecordBuilder<'r> {
    #[inline(always)]
    pub(crate) fn wrap(record: &'r mut ByteRecord) -> Self {
        Self { record, start: 0 }
    }

    #[inline(always)]
    pub(crate) fn extend_from_slice(&mut self, slice: &[u8]) {
        self.record.data.extend_from_slice(slice);
    }

    #[inline(always)]
    pub(crate) fn push_byte(&mut self, byte: u8) {
        self.record.data.push(byte);
    }

    #[inline]
    pub(crate) fn finalize_field(&mut self) {
        let start = self.start;
        self.start = self.record.data.len();

        self.record.bounds.push((start, self.start));
    }

    #[inline]
    pub(crate) fn finalize_field_preemptively(&mut self, offset: usize) {
        let start = self.start;
        self.start = self.record.data.len() + offset;

        self.record.bounds.push((start, self.start));

        self.start += 1;
    }

    #[inline(always)]
    pub(crate) fn bump(&mut self) {
        self.start += 1;
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

        for i in 0..expected.len() {
            assert_eq!(record.get(i), Some(expected[i]));
        }

        assert_eq!(record.get(4), None);
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
