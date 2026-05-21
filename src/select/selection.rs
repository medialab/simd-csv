use std::ops::Index;

/// A selection of column indices.
pub struct Selection {
    indices: Vec<usize>,
    alignment: usize,
}

impl Selection {
    pub(crate) fn new(indices: Vec<usize>, alignment: usize) -> Self {
        Self { indices, alignment }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

impl Index<usize> for Selection {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        &self.indices[index]
    }
}
