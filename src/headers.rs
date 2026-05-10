use std::collections::BTreeMap;
use std::fmt;
use std::ops::Index;

use crate::debug;
use crate::records::ByteRecord;

#[derive(Debug, PartialEq, Clone)]
pub enum ColumIndexationBy<'b> {
    Name(&'b [u8]),
    NameAndNth(&'b [u8], isize),
    Pos(isize),
}

impl ColumIndexationBy<'_> {
    pub fn has_name(&self) -> bool {
        matches!(self, Self::Name(_) | Self::NameAndNth(_, _))
    }
}

pub struct ByteHeadersIndex {
    inner: ByteRecord,
    map: Option<BTreeMap<Vec<u8>, Vec<usize>>>,
}

impl fmt::Debug for ByteHeadersIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ByteHeadersIndex(")?;
        f.debug_list()
            .entries(self.inner.iter().map(debug::Bytes))
            .finish()?;
        write!(f, ")")?;
        Ok(())
    }
}

impl ByteHeadersIndex {
    pub fn new(record: ByteRecord, has_names: bool) -> Self {
        let map = if !has_names {
            None
        } else {
            let mut map = BTreeMap::new();

            for (i, name) in record.iter().enumerate() {
                let indices = map.entry(name.to_vec()).or_insert_with(Vec::new);
                indices.push(i);
            }

            Some(map)
        };

        Self { inner: record, map }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn has_names(&self) -> bool {
        self.map.is_some()
    }

    pub fn first_column_index_by_name(&self, name: impl AsRef<[u8]>) -> Option<usize> {
        self.map
            .as_ref()?
            .get(name.as_ref())
            .map(|indices| indices[0])
    }

    pub fn find_column_index(&self, indexation: ColumIndexationBy) -> Option<usize> {
        match indexation {
            ColumIndexationBy::Name(name) => self
                .map
                .as_ref()?
                .get(name)
                .and_then(|positions| positions.first())
                .copied(),
            ColumIndexationBy::Pos(pos) => {
                let len = self.inner.len();

                if pos < 0 {
                    // Negative indexing
                    let pos = pos.unsigned_abs();

                    if pos > len {
                        None
                    } else {
                        Some(len - pos)
                    }
                } else {
                    let pos = pos as usize;

                    if pos >= len {
                        None
                    } else {
                        Some(pos)
                    }
                }
            }
            ColumIndexationBy::NameAndNth(name, pos) => self
                .map
                .as_ref()?
                .get(name)
                .and_then(|positions| {
                    if pos < 0 {
                        let pos = pos.unsigned_abs();
                        let len = positions.len();

                        if pos > len {
                            None
                        } else {
                            positions.get(len - pos)
                        }
                    } else {
                        positions.get(pos as usize)
                    }
                })
                .copied(),
        }
    }
}

impl AsRef<ByteRecord> for ByteHeadersIndex {
    fn as_ref(&self) -> &ByteRecord {
        &self.inner
    }
}

impl Index<usize> for ByteHeadersIndex {
    type Output = [u8];

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_headers_index() {
        let headers = brec!["name", "surname", "age", "name"];
        let index = ByteHeadersIndex::new(headers.clone(), true);

        assert_eq!(&headers, index.as_ref());
        assert_eq!(index.has_names(), true);
        assert_eq!(index.len(), 4);
        assert_eq!(index.is_empty(), false);
        assert_eq!(index.first_column_index_by_name("surname"), Some(1));
    }
}
