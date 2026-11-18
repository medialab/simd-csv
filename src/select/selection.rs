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

    pub fn full(len: usize) -> Self {
        Self::new((0..len).collect(), len)
    }

    pub fn without(i: usize, len: usize) -> Self {
        Self::new((0..len).filter(|j| i != *j).collect(), len)
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    #[inline(always)]
    pub fn sort(&mut self) {
        self.indices.sort();
    }

    pub fn sort_and_dedup(&mut self) {
        self.sort();
        self.indices.dedup();
    }

    pub fn dedup(&mut self) {
        let mut seen = Vec::with_capacity(self.len());

        self.indices.retain(|i| {
            if !seen.contains(i) {
                seen.push(*i);
                true
            } else {
                false
            }
        });
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

    #[inline(always)]
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

    #[inline]
    pub fn into_inner(self) -> Vec<usize> {
        self.indices
    }
}

impl<'a> IntoIterator for &'a Selection {
    type Item = usize;
    type IntoIter = IndicesIter<'a>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Index<usize> for Selection {
    type Output = usize;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        &self.indices[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort() {
        let mut selection = Selection::new(vec![3, 1, 1, 2, 6, 0], 7);
        selection.sort();

        assert_eq!(selection.into_inner(), &[0, 1, 1, 2, 3, 6]);
    }

    #[test]
    fn test_sort_and_dedup() {
        let mut selection = Selection::new(vec![3, 1, 1, 2, 6, 0], 7);
        selection.sort_and_dedup();

        assert_eq!(selection.into_inner(), &[0, 1, 2, 3, 6]);
    }

    #[test]
    fn test_dedup() {
        let mut selection = Selection::new(vec![3, 1, 1, 2, 6, 0, 1, 3], 7);
        selection.dedup();

        assert_eq!(selection.into_inner(), &[3, 1, 2, 6, 0]);
    }
}
