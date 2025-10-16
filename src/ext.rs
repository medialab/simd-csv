pub trait Pointer {
    unsafe fn distance(self, origin: Self) -> usize;
}

impl<T> Pointer for *const T {
    #[inline(always)]
    unsafe fn distance(self, origin: *const T) -> usize {
        usize::try_from(self.offset_from(origin)).unwrap_unchecked()
    }
}
