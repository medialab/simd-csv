use std::fmt;

use crate::debug;

pub struct ZeroCopyRecord<'a> {
    slice: &'a [u8],
    seps: &'a [usize],
}

impl<'a> ZeroCopyRecord<'a> {
    pub fn new(slice: &'a [u8], seps: &'a [usize]) -> Self {
        Self { slice, seps }
    }

    pub fn len(&self) -> usize {
        self.seps.len() + 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> ZeroCopyRecordIter<'_> {
        ZeroCopyRecordIter {
            record: self,
            current_sep_index: 0,
            offset: 0,
        }
    }
}

impl<'a> fmt::Debug for ZeroCopyRecord<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZeroCopyRecord(")?;
        f.debug_list()
            .entries(self.iter().map(debug::Bytes))
            .finish()?;
        write!(f, ")")?;
        Ok(())
    }
}

pub struct ZeroCopyRecordIter<'a> {
    record: &'a ZeroCopyRecord<'a>,
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
            let slice = &self.record.slice[self.offset..].trim_ascii_end();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_record() {
        let record = ZeroCopyRecord::new(b"name,surname,age", &[4, 12]);

        assert_eq!(record.len(), 3);

        let expected: Vec<&[u8]> = vec![b"name", b"surname", b"age"];
        assert_eq!(record.iter().collect::<Vec<_>>(), expected);
    }
}
