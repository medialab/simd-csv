use std::borrow::Cow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Index;

use crate::debug;
use crate::utils::{trim_trailing_crlf, unescape, unescape_to, unquoted};

/// A view of a CSV record into a [`ZeroCopyReader`](crate::ZeroCopyReader) buffer.
pub struct ZeroCopyByteRecord<'a> {
    slice: &'a [u8],
    seps: &'a [usize],
    pub(crate) quote: u8,
}

impl<'a> ZeroCopyByteRecord<'a> {
    #[inline]
    pub(crate) fn new(slice: &'a [u8], seps: &'a [usize], quote: u8) -> Self {
        Self {
            slice: trim_trailing_crlf(slice),
            seps,
            quote,
        }
    }

    #[inline]
    pub(crate) fn to_parts(&self) -> (Vec<usize>, Vec<u8>) {
        (self.seps.to_vec(), self.slice.to_vec())
    }

    /// Number of fields of the record. Cannot be less than 1 since a CSV with no
    /// columns does not make sense.
    #[inline(always)]
    pub fn len(&self) -> usize {
        // NOTE: an empty zero copy record cannot be constructed,
        // by definition.
        self.seps.len() + 1
    }

    /// Returns whether the record has no fields.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Returns the underlying byte slice, delimiters and all.
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        self.slice
    }

    /// Returns an iterator over the record's fields, as-is.
    ///
    /// This means fields might or might not be quoted and
    /// field bytes have not been unescaped at all.
    #[inline]
    pub fn iter(&self) -> ZeroCopyByteRecordIter<'_> {
        ZeroCopyByteRecordIter {
            record: self,
            current_forward: 0,
            current_backward: self.len(),
        }
    }

    /// Returns an iterator over the record's fields, unquoted.
    ///
    /// See [`Self::unquote`] for more detail.
    #[inline]
    pub fn unquoted_iter(&self) -> ZeroCopyByteRecordUnquotedIter<'_> {
        ZeroCopyByteRecordUnquotedIter {
            record: self,
            current_forward: 0,
            current_backward: self.len(),
        }
    }

    /// Returns an iterator over the record's fields, unescaped.
    ///
    /// See [`Self::unescape`] for more detail.
    #[inline]
    pub fn unescaped_iter(&self) -> ZeroCopyByteRecordUnescapedIter<'_> {
        ZeroCopyByteRecordUnescapedIter {
            record: self,
            current_forward: 0,
            current_backward: self.len(),
        }
    }

    /// Returns the nth field of the zero copy byte record, if it is not
    /// out-of-bounds.
    ///
    /// The field's bytes will be given as-is, quoted or unquoted, and won't be
    /// unescaped at all.
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

    /// Returns the nth field of the zero copy byte record, if it is not
    /// out-of-bounds.
    ///
    /// The field's bytes will be given unquoted (i.e. without surrounding
    /// quotes), but not unescaped (i.e. doubled double quotes will still be
    /// there).
    ///
    /// The overhead vs. [`Self::get`] is only constant (we trim a leading and
    /// trailing quote if required).
    #[inline]
    pub fn unquote(&self, index: usize) -> Option<&[u8]> {
        self.get(index)
            .map(|cell| unquoted(cell, self.quote).unwrap_or(cell))
    }

    /// Returns the nth field of the zero copy byte record, if it is not
    /// out-of-bounds.
    ///
    /// The field's bytes will be completely unescaped.
    ///
    /// The overhead vs. [`Self::get`] is linear in the field's number of bytes.
    ///
    /// A [`Cow::Owned`] will be returned if the field actually needed
    /// unescaping, else a [`Cow::Borrowed`] will be returned.
    #[inline]
    pub fn unescape(&self, index: usize) -> Option<Cow<[u8]>> {
        self.unquote(index).map(|cell| {
            if let Some(trimmed) = unquoted(cell, self.quote) {
                unescape(trimmed, self.quote)
            } else {
                Cow::Borrowed(cell)
            }
        })
    }

    fn read_byte_record(&self, record: &mut ByteRecord) {
        record.clear();

        for cell in self.iter() {
            if let Some(trimmed) = unquoted(cell, self.quote) {
                unescape_to(trimmed, self.quote, &mut record.data);

                let bounds_len = record.bounds.len();

                let start = if bounds_len == 0 {
                    0
                } else {
                    record.bounds[bounds_len - 1].1
                };

                record.bounds.push((start, record.data.len()));
            } else {
                record.push_field(cell);
            }
        }
    }

    /// Converts the zero copy byte record into a proper, owned [`ByteRecord`].
    #[inline]
    pub fn to_byte_record(&self) -> ByteRecord {
        let mut record = ByteRecord::new();
        self.read_byte_record(&mut record);
        record
    }

    #[inline]
    pub(crate) fn to_byte_record_in_reverse(&self) -> ByteRecord {
        let mut record = ByteRecord::new();

        for cell in self.unescaped_iter().rev() {
            record.push_field_in_reverse(&cell);
        }

        record
    }
}

impl fmt::Debug for ZeroCopyByteRecord<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZeroCopyByteRecord(")?;
        f.debug_list()
            .entries(self.iter().map(debug::Bytes))
            .finish()?;
        write!(f, ")")?;
        Ok(())
    }
}

macro_rules! make_zero_copy_iterator {
    ($name:ident, $method: ident, $out_type: ty) => {
        pub struct $name<'a> {
            record: &'a ZeroCopyByteRecord<'a>,
            current_forward: usize,
            current_backward: usize,
        }

        impl ExactSizeIterator for $name<'_> {}

        impl<'a> Iterator for $name<'a> {
            type Item = $out_type;

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                if self.current_forward == self.current_backward {
                    None
                } else {
                    let cell = self.record.$method(self.current_forward);

                    self.current_forward += 1;

                    cell
                }
            }

            #[inline]
            fn size_hint(&self) -> (usize, Option<usize>) {
                let size = self.current_backward - self.current_forward;

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

        impl DoubleEndedIterator for $name<'_> {
            #[inline]
            fn next_back(&mut self) -> Option<Self::Item> {
                if self.current_forward == self.current_backward {
                    None
                } else {
                    self.current_backward -= 1;

                    self.record.$method(self.current_backward)
                }
            }
        }
    };
}

make_zero_copy_iterator!(ZeroCopyByteRecordIter, get, &'a [u8]);
make_zero_copy_iterator!(ZeroCopyByteRecordUnquotedIter, unquote, &'a [u8]);
make_zero_copy_iterator!(ZeroCopyByteRecordUnescapedIter, unescape, Cow<'a, [u8]>);

impl Index<usize> for ZeroCopyByteRecord<'_> {
    type Output = [u8];

    #[inline]
    fn index(&self, i: usize) -> &[u8] {
        self.get(i).unwrap()
    }
}

/// An owned, unescaped representation of a CSV record.
#[derive(Default, Clone, Eq)]
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
    pub fn truncate(&mut self, len: usize) {
        self.bounds.truncate(len);

        if let Some((_, end)) = self.bounds.last() {
            self.data.truncate(*end);
        } else {
            self.data.clear();
        }
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
            current_backward: self.len(),
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
    fn push_field_in_reverse(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);

        let bounds_len = self.bounds.len();

        let start = if bounds_len == 0 {
            0
        } else {
            self.bounds[bounds_len - 1].1
        };

        let bounds = (start, self.data.len());
        self.data[bounds.0..bounds.1].reverse();

        self.bounds.push(bounds);
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        self.bounds
            .get(index)
            .copied()
            .map(|(start, end)| &self.data[start..end])
    }

    pub(crate) fn reverse(&mut self) {
        self.data.reverse();
        self.bounds.reverse();

        let len = self.data.len();

        for (start, end) in self.bounds.iter_mut() {
            let new_end = len - *start;
            let new_start = len - *end;

            *start = new_start;
            *end = new_end;
        }
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

impl Hash for ByteRecord {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.len());

        for cell in self.iter() {
            state.write(cell);
        }
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

impl<T: AsRef<[u8]>> FromIterator<T> for ByteRecord {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut record = Self::new();
        record.extend(iter);
        record
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
    current_backward: usize,
}

impl ExactSizeIterator for ByteRecordIter<'_> {}

impl<'a> Iterator for ByteRecordIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_forward == self.current_backward {
            None
        } else {
            let (start, end) = self.record.bounds[self.current_forward];

            self.current_forward += 1;

            Some(&self.record.data[start..end])
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.current_backward - self.current_forward;

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

impl DoubleEndedIterator for ByteRecordIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_forward == self.current_backward {
            None
        } else {
            self.current_backward -= 1;

            let (start, end) = self.record.bounds[self.current_backward];

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
    pub(crate) fn finalize_record(&mut self) {
        if let Some(b'\r') = self.record.data.last() {
            self.record.data.pop();
        }

        self.finalize_field();
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
        self.start +=
            (self.record.bounds.last().map(|(s, _)| *s).unwrap_or(0) != self.start) as usize;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_byte_record() {
        let record = ZeroCopyByteRecord::new(b"name,surname,age", &[4, 12], b'"');

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

    #[test]
    fn test_mutate_record_after_read() {
        let mut record = ByteRecord::new();
        let mut builder = ByteRecordBuilder::wrap(&mut record);
        builder.extend_from_slice(b"test\r");
        builder.finalize_record();

        assert_eq!(record.iter().collect::<Vec<_>>(), vec![b"test"]);

        record.push_field(b"next");

        assert_eq!(record.iter().collect::<Vec<_>>(), vec![b"test", b"next"]);
    }

    #[test]
    fn test_reverse_byte_record() {
        let record = brec!["name", "surname", "age"];
        let mut reversed = record.clone();
        reversed.reverse();

        assert_eq!(reversed, brec!["ega", "emanrus", "eman"]);
        reversed.reverse();
        assert_eq!(record, reversed);
    }
}
