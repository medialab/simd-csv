use std::iter::Copied;
use std::ops::Index;
use std::slice::Iter;

type IndicesIter<'a> = Copied<Iter<'a, usize>>;

/// A selection of column indices.
pub struct Selection {
    indices: Vec<usize>,
    alignment: usize,
}

impl Selection {
    pub(crate) fn new(indices: Vec<usize>, alignment: usize) -> Self {
        debug_assert!(indices.iter().all(|i| *i < alignment));

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

    #[inline]
    pub fn select<'a, 'b, T: 'b + ?Sized>(
        &'a self,
        row: &'b impl Index<usize, Output = T>,
    ) -> impl Iterator<Item = &'b T>
    where
        'a: 'b,
    {
        self.indices.iter().map(|i| &row[*i])
    }

    #[inline]
    pub fn iter(&self) -> IndicesIter<'_> {
        self.indices.iter().copied()
    }

    #[inline]
    pub fn indexed_mask(&self) -> Vec<Option<usize>> {
        let mut mask = vec![None; self.alignment];

        for (j, i) in self.iter().enumerate() {
            if i < self.alignment {
                mask[i] = Some(j);
            }
        }

        mask
    }

    #[inline]
    pub fn mask(&self) -> Vec<bool> {
        let mut mask = vec![false; self.alignment];

        for i in self {
            if i < self.alignment {
                mask[i] = true;
            }
        }

        mask
    }
}

impl<'a> IntoIterator for &'a Selection {
    type Item = usize;
    type IntoIter = IndicesIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Index<usize> for Selection {
    type Output = usize;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.indices[index]
    }
}
