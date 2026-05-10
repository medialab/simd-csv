use std::collections::BTreeMap;

use crate::records::ByteRecord;

pub struct ByteHeadersIndex {
    inner: ByteRecord,
    map: Option<BTreeMap<Vec<u8>, Vec<usize>>>,
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
}

impl AsRef<ByteRecord> for ByteHeadersIndex {
    fn as_ref(&self) -> &ByteRecord {
        &self.inner
    }
}
