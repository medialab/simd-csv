use std::io::{BufRead, Result};

pub trait Pointer {
    unsafe fn distance(self, origin: Self) -> usize;
}

impl<T> Pointer for *const T {
    #[inline(always)]
    unsafe fn distance(self, origin: *const T) -> usize {
        usize::try_from(self.offset_from(origin)).unwrap_unchecked()
    }
}

pub trait StripBom: BufRead {
    #[inline]
    fn strip_bom(&mut self) -> Result<()> {
        let input = self.fill_buf()?;

        if input.len() >= 3 && &input[..3] == b"\xef\xbb\xbf" {
            self.consume(3);
        }

        Ok(())
    }
}

impl<R: BufRead> StripBom for R {}
