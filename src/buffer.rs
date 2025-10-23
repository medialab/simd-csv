use std::io::{BufRead, BufReader, Read, Result};

pub struct ScratchBuffer<R> {
    inner: BufReader<R>,
    scratch: Vec<u8>,
    next_consume: Option<usize>,
}

impl<R: Read> ScratchBuffer<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
            scratch: Vec::new(),
            next_consume: None,
        }
    }

    pub fn with_capacity(capacity: usize, reader: R) -> Self {
        Self {
            inner: BufReader::with_capacity(capacity, reader),
            scratch: Vec::with_capacity(capacity),
            next_consume: None,
        }
    }

    pub(crate) fn with_optional_capacity(capacity: Option<usize>, reader: R) -> Self {
        match capacity {
            None => Self::new(reader),
            Some(capacity) => Self::with_capacity(capacity, reader),
        }
    }

    #[inline(always)]
    pub fn consume(&mut self, amt: usize) {
        self.inner.consume(amt);
    }

    #[inline(always)]
    pub fn fill_buf(&mut self) -> Result<&[u8]> {
        self.inner.fill_buf()
    }

    #[inline(always)]
    pub fn save(&mut self) {
        let bytes = self.inner.buffer();

        self.scratch.extend_from_slice(bytes);
        self.inner.consume(bytes.len());
    }

    #[inline(always)]
    pub fn has_something_saved(&self) -> bool {
        !self.scratch.is_empty()
    }

    #[inline(always)]
    pub fn saved(&self) -> &[u8] {
        &self.scratch
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        self.scratch.clear();

        if let Some(amt) = self.next_consume.take() {
            self.inner.consume(amt);
        }
    }

    #[inline]
    pub fn flush(&mut self, amt: usize) -> &[u8] {
        let bytes = self.inner.buffer();

        if self.scratch.is_empty() {
            self.next_consume = Some(amt);

            &self.inner.buffer()[..amt]
        } else {
            self.scratch.extend_from_slice(&bytes[..amt]);
            self.inner.consume(amt);

            &self.scratch
        }
    }

    pub fn into_bufreader(mut self) -> BufReader<R> {
        self.reset();
        self.inner
    }
}
