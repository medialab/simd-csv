use std::io::{BufRead, BufReader, Read, Result};

pub struct BufReaderWithPosition<R> {
    inner: BufReader<R>,
    pos: u64,
}

impl<R: Read> BufReaderWithPosition<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
            pos: 0,
        }
    }

    pub fn with_capacity(capacity: usize, reader: R) -> Self {
        Self {
            inner: BufReader::with_capacity(capacity, reader),
            pos: 0,
        }
    }

    #[inline(always)]
    pub fn position(&self) -> u64 {
        self.pos
    }

    #[inline(always)]
    pub fn consume(&mut self, amt: usize) {
        self.pos += amt as u64;
        self.inner.consume(amt);
    }

    #[inline(always)]
    pub fn fill_buf(&mut self) -> Result<&[u8]> {
        self.inner.fill_buf()
    }

    #[inline(always)]
    pub fn get_mut(&mut self) -> &mut R {
        self.inner.get_mut()
    }

    #[inline(always)]
    pub fn get_ref(&self) -> &R {
        self.inner.get_ref()
    }

    #[inline(always)]
    pub fn buffer(&self) -> &[u8] {
        self.inner.buffer()
    }

    pub fn into_inner(self) -> BufReader<R> {
        self.inner
    }
}

pub struct ScratchBuffer<R> {
    inner: BufReaderWithPosition<R>,
    scratch: Vec<u8>,
    next_consume: Option<usize>,
}

impl<R: Read> ScratchBuffer<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReaderWithPosition::new(reader),
            scratch: Vec::new(),
            next_consume: None,
        }
    }

    pub fn with_capacity(capacity: usize, reader: R) -> Self {
        Self {
            inner: BufReaderWithPosition::with_capacity(capacity, reader),
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

    #[inline(always)]
    pub fn position(&self) -> u64 {
        let offset = self.next_consume.unwrap_or(0) as u64;
        self.inner.position() + offset
    }

    pub fn into_bufreader(mut self) -> BufReader<R> {
        self.reset();
        self.inner.into_inner()
    }
}
