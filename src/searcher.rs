use std::iter::FusedIterator;

#[cfg(target_arch = "x86_64")]
mod x86_64 {
    use std::marker::PhantomData;

    use crate::ext::Pointer;

    #[inline(always)]
    fn get_for_offset(mask: u32) -> u32 {
        #[cfg(target_endian = "big")]
        {
            mask.swap_bytes()
        }
        #[cfg(target_endian = "little")]
        {
            mask
        }
    }

    #[inline(always)]
    fn first_offset(mask: u32) -> usize {
        get_for_offset(mask).trailing_zeros() as usize
    }

    #[inline(always)]
    fn clear_least_significant_bit(mask: u32) -> u32 {
        mask & (mask - 1)
    }

    pub struct MaskIter(u32);

    impl Iterator for MaskIter {
        type Item = usize;

        #[inline(always)]
        fn next(&mut self) -> Option<Self::Item> {
            if self.0 == 0 {
                None
            } else {
                let offset = first_offset(self.0);
                self.0 = clear_least_significant_bit(self.0);
                Some(offset)
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    pub struct Mask(u32);

    impl Mask {
        #[inline(always)]
        pub fn into_first_offset(self) -> Option<usize> {
            if self.0 != 0 {
                Some(first_offset(self.0))
            } else {
                None
            }
        }

        #[inline(always)]
        pub fn into_iter(self) -> MaskIter {
            MaskIter(self.0)
        }
    }

    pub mod sse2 {
        use super::*;

        use core::arch::x86_64::{
            __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_or_si128,
            _mm_set1_epi8,
        };

        const CHUNK_SIZE: usize = 16;
        const FULL_MASK: u32 = (1 << CHUNK_SIZE) - 1;

        #[derive(Debug)]
        pub struct SSE2Searcher {
            delimiter: u8,
            newline: u8,
            quote: u8,
            delimiter_splat: __m128i,
            newline_splat: __m128i,
            quote_splat: __m128i,
        }

        impl SSE2Searcher {
            #[inline]
            pub unsafe fn new(delimiter: u8, newline: u8, quote: u8) -> Self {
                Self {
                    delimiter,
                    newline,
                    quote,
                    delimiter_splat: _mm_set1_epi8(delimiter as i8),
                    newline_splat: _mm_set1_epi8(newline as i8),
                    quote_splat: _mm_set1_epi8(quote as i8),
                }
            }

            #[inline(always)]
            pub fn iter<'s, 'h>(&'s self, haystack: &'h [u8]) -> SSE2Indices<'s, 'h> {
                SSE2Indices::new(self, haystack)
            }

            #[inline(always)]
            unsafe fn process_loaded_chunk(&self, chunk: __m128i, valid_mask: u32) -> (Mask, Mask) {
                let delimiter_cmp = _mm_cmpeq_epi8(chunk, self.delimiter_splat);
                let newline_cmp = _mm_cmpeq_epi8(chunk, self.newline_splat);
                let quote_cmp = _mm_cmpeq_epi8(chunk, self.quote_splat);

                let structural_cmp = _mm_or_si128(newline_cmp, quote_cmp);

                // Ignore invalid lanes before looking for the first structural byte.
                let structural_mask = (_mm_movemask_epi8(structural_cmp) as u32) & valid_mask;
                let mut delimiter_mask = (_mm_movemask_epi8(delimiter_cmp) as u32) & valid_mask;

                delimiter_mask &= (1u32 << ((structural_mask | 0x1_0000).trailing_zeros())) - 1;

                (Mask(delimiter_mask), Mask(structural_mask))
            }

            #[inline]
            pub unsafe fn process_chunk(&self, haystack: &[u8]) -> (usize, (Mask, Mask)) {
                if haystack.len() >= CHUNK_SIZE {
                    let chunk = _mm_loadu_si128(haystack.as_ptr() as *const __m128i);
                    return (CHUNK_SIZE, self.process_loaded_chunk(chunk, FULL_MASK));
                }

                let mut tail = [0u8; CHUNK_SIZE];
                tail[..haystack.len()].copy_from_slice(haystack);

                let chunk = _mm_loadu_si128(tail.as_ptr() as *const __m128i);

                let valid_mask = match haystack.len() {
                    0 => 0,
                    n => (1u32 << n) - 1,
                };

                (haystack.len(), self.process_loaded_chunk(chunk, valid_mask))
            }
        }

        #[derive(Debug)]
        pub struct SSE2Indices<'s, 'h> {
            searcher: &'s SSE2Searcher,
            haystack: PhantomData<&'h [u8]>,
            start: *const u8,
            end: *const u8,
            current: *const u8,
            mask: u32,
        }

        impl<'s, 'h> SSE2Indices<'s, 'h> {
            #[inline]
            fn new(searcher: &'s SSE2Searcher, haystack: &'h [u8]) -> Self {
                let ptr = haystack.as_ptr();

                Self {
                    searcher,
                    haystack: PhantomData,
                    start: ptr,
                    end: ptr.wrapping_add(haystack.len()),
                    current: ptr,
                    mask: 0,
                }
            }
        }

        impl SSE2Indices<'_, '_> {
            #[inline]
            pub unsafe fn _next_in_current_mask(&mut self) -> Option<usize> {
                let mask = self.mask;
                let current = self.current;

                if mask != 0 {
                    let offset = current.sub(CHUNK_SIZE).add(first_offset(mask));
                    self.mask = clear_least_significant_bit(mask);
                    self.current = current;

                    Some(offset.distance(self.start))
                } else {
                    None
                }
            }

            pub unsafe fn next(&mut self) -> Option<usize> {
                if self.start >= self.end {
                    return None;
                }

                let mut mask = self.mask;

                let mut current = self.current;
                let start = self.start;
                let len = self.end.distance(start);
                let delimiter_splat = self.searcher.delimiter_splat;
                let newline_splat = self.searcher.newline_splat;
                let quote_splat = self.searcher.quote_splat;

                'main: loop {
                    // Processing current move mask
                    if mask != 0 {
                        let offset = current.sub(CHUNK_SIZE).add(first_offset(mask));
                        self.mask = clear_least_significant_bit(mask);
                        self.current = current;

                        return Some(offset.distance(start));
                    }

                    // Main loop of unaligned loads
                    if len >= CHUNK_SIZE {
                        let vectorized_end = self.end.sub(CHUNK_SIZE);

                        while current <= vectorized_end {
                            let chunk = _mm_loadu_si128(current as *const __m128i);
                            let cmp1 = _mm_cmpeq_epi8(chunk, delimiter_splat);
                            let cmp2 = _mm_cmpeq_epi8(chunk, newline_splat);
                            let cmp3 = _mm_cmpeq_epi8(chunk, quote_splat);
                            let cmp = _mm_or_si128(cmp1, cmp2);
                            let cmp = _mm_or_si128(cmp, cmp3);

                            mask = _mm_movemask_epi8(cmp) as u32;

                            current = current.add(CHUNK_SIZE);

                            if mask != 0 {
                                continue 'main;
                            }
                        }
                    }

                    // Processing remaining bytes linearly
                    while current < self.end {
                        if *current == self.searcher.delimiter
                            || *current == self.searcher.newline
                            || *current == self.searcher.quote
                        {
                            let offset = current.distance(start);
                            self.current = current.add(1);
                            return Some(offset);
                        }
                        current = current.add(1);
                    }

                    return None;
                }
            }
        }
    }

    pub mod avx2 {
        use super::*;

        use core::arch::x86_64::{
            __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_or_si256,
            _mm256_set1_epi8,
        };
        use std::marker::PhantomData;

        const CHUNK_SIZE: usize = 32;

        #[derive(Debug)]
        pub struct AVX2Searcher {
            delimiter: u8,
            newline: u8,
            quote: u8,
            delimiter_splat: __m256i,
            newline_splat: __m256i,
            quote_splat: __m256i,
        }

        impl AVX2Searcher {
            #[inline]
            #[target_feature(enable = "avx2")]
            pub unsafe fn new(delimiter: u8, newline: u8, quote: u8) -> Self {
                Self {
                    delimiter,
                    newline,
                    quote,
                    delimiter_splat: _mm256_set1_epi8(delimiter as i8),
                    newline_splat: _mm256_set1_epi8(newline as i8),
                    quote_splat: _mm256_set1_epi8(quote as i8),
                }
            }

            #[inline(always)]
            pub fn iter<'s, 'h>(&'s self, haystack: &'h [u8]) -> AVX2Indices<'s, 'h> {
                AVX2Indices::new(self, haystack)
            }

            #[inline]
            #[target_feature(enable = "avx2")]
            unsafe fn process_loaded_chunk(&self, chunk: __m256i, valid_mask: u32) -> (Mask, Mask) {
                let delimiter_cmp = _mm256_cmpeq_epi8(chunk, self.delimiter_splat);
                let newline_cmp = _mm256_cmpeq_epi8(chunk, self.newline_splat);
                let quote_cmp = _mm256_cmpeq_epi8(chunk, self.quote_splat);

                let structural_cmp = _mm256_or_si256(newline_cmp, quote_cmp);

                let structural_mask = (_mm256_movemask_epi8(structural_cmp) as u32) & valid_mask;

                let mut delimiter_mask = (_mm256_movemask_epi8(delimiter_cmp) as u32) & valid_mask;

                // Keep delimiters before the first quote or newline.
                //
                // For zero, this produces u32::MAX.
                let first_structural_bit = structural_mask & structural_mask.wrapping_neg();

                let before_first_structural = first_structural_bit.wrapping_sub(1);

                delimiter_mask &= before_first_structural;

                (Mask(delimiter_mask), Mask(structural_mask))
            }

            #[inline]
            #[target_feature(enable = "avx2")]
            pub unsafe fn process_chunk(&self, haystack: &[u8]) -> (usize, (Mask, Mask)) {
                if haystack.len() >= CHUNK_SIZE {
                    let chunk = _mm256_loadu_si256(haystack.as_ptr() as *const __m256i);

                    return (CHUNK_SIZE, self.process_loaded_chunk(chunk, u32::MAX));
                }

                let mut tail = [0u8; CHUNK_SIZE];
                tail[..haystack.len()].copy_from_slice(haystack);

                let chunk = _mm256_loadu_si256(tail.as_ptr() as *const __m256i);

                let valid_mask = match haystack.len() {
                    0 => 0,
                    n => u32::MAX >> (CHUNK_SIZE - n),
                };

                (haystack.len(), self.process_loaded_chunk(chunk, valid_mask))
            }
        }

        #[derive(Debug)]
        pub struct AVX2Indices<'s, 'h> {
            searcher: &'s AVX2Searcher,
            haystack: PhantomData<&'h [u8]>,
            start: *const u8,
            end: *const u8,
            current: *const u8,
            mask: u32,
        }

        impl<'s, 'h> AVX2Indices<'s, 'h> {
            #[inline]
            fn new(searcher: &'s AVX2Searcher, haystack: &'h [u8]) -> Self {
                let ptr = haystack.as_ptr();

                Self {
                    searcher,
                    haystack: PhantomData,
                    start: ptr,
                    end: ptr.wrapping_add(haystack.len()),
                    current: ptr,
                    mask: 0,
                }
            }
        }

        impl AVX2Indices<'_, '_> {
            #[inline]
            pub unsafe fn _next_in_current_mask(&mut self) -> Option<usize> {
                let mask = self.mask;
                let current = self.current;

                if mask != 0 {
                    let offset = current.sub(CHUNK_SIZE).add(first_offset(mask));

                    self.mask = clear_least_significant_bit(mask);
                    self.current = current;

                    Some(offset.distance(self.start))
                } else {
                    None
                }
            }

            #[target_feature(enable = "avx2")]
            pub unsafe fn next(&mut self) -> Option<usize> {
                if self.start >= self.end {
                    return None;
                }

                let mut mask = self.mask;
                let mut current = self.current;
                let start = self.start;
                let len = self.end.distance(start);

                let delimiter_splat = self.searcher.delimiter_splat;
                let newline_splat = self.searcher.newline_splat;
                let quote_splat = self.searcher.quote_splat;

                'main: loop {
                    if mask != 0 {
                        let offset = current.sub(CHUNK_SIZE).add(first_offset(mask));

                        self.mask = clear_least_significant_bit(mask);
                        self.current = current;

                        return Some(offset.distance(start));
                    }

                    if len >= CHUNK_SIZE {
                        let vectorized_end = self.end.sub(CHUNK_SIZE);

                        while current <= vectorized_end {
                            let chunk = _mm256_loadu_si256(current as *const __m256i);

                            let cmp1 = _mm256_cmpeq_epi8(chunk, delimiter_splat);
                            let cmp2 = _mm256_cmpeq_epi8(chunk, newline_splat);
                            let cmp3 = _mm256_cmpeq_epi8(chunk, quote_splat);

                            let cmp = _mm256_or_si256(cmp1, cmp2);
                            let cmp = _mm256_or_si256(cmp, cmp3);

                            mask = _mm256_movemask_epi8(cmp) as u32;
                            current = current.add(CHUNK_SIZE);

                            if mask != 0 {
                                continue 'main;
                            }
                        }
                    }

                    while current < self.end {
                        if *current == self.searcher.delimiter
                            || *current == self.searcher.newline
                            || *current == self.searcher.quote
                        {
                            let offset = current.distance(start);
                            self.current = current.add(1);

                            return Some(offset);
                        }

                        current = current.add(1);
                    }

                    return None;
                }
            }
        }
    }
}

#[cfg(target_arch = "aarch64")]
mod aarch64 {
    use core::arch::aarch64::{
        uint8x16_t, vceqq_u8, vdupq_n_u8, vget_lane_u64, vld1q_u8, vorrq_u8, vreinterpret_u64_u8,
        vreinterpretq_u16_u8, vshrn_n_u16,
    };
    use std::marker::PhantomData;

    use crate::ext::Pointer;

    const CHUNK_SIZE: usize = 16;

    #[inline(always)]
    unsafe fn neon_movemask(v: uint8x16_t) -> u64 {
        let asu16s = vreinterpretq_u16_u8(v);
        let mask = vshrn_n_u16(asu16s, 4);
        let asu64 = vreinterpret_u64_u8(mask);
        let scalar64 = vget_lane_u64(asu64, 0);

        scalar64 & 0x8888888888888888
    }

    #[inline(always)]
    fn first_offset(mask: u64) -> usize {
        (mask.trailing_zeros() >> 2) as usize
    }

    #[inline(always)]
    fn clear_least_significant_bit(mask: u64) -> u64 {
        mask & (mask - 1)
    }

    #[derive(Debug)]
    pub struct NeonSearcher {
        delimiter: u8,
        newline: u8,
        quote: u8,
        delimiter_splat: uint8x16_t,
        newline_splat: uint8x16_t,
        quote_splat: uint8x16_t,
    }

    impl NeonSearcher {
        #[inline]
        pub unsafe fn new(delimiter: u8, newline: u8, quote: u8) -> Self {
            Self {
                delimiter,
                newline,
                quote,
                delimiter_splat: vdupq_n_u8(delimiter),
                newline_splat: vdupq_n_u8(newline),
                quote_splat: vdupq_n_u8(quote),
            }
        }

        #[inline(always)]
        pub fn iter<'s, 'h>(&'s self, haystack: &'h [u8]) -> NeonIndices<'s, 'h> {
            NeonIndices::new(self, haystack)
        }
    }

    #[derive(Debug)]
    pub struct NeonIndices<'s, 'h> {
        searcher: &'s NeonSearcher,
        haystack: PhantomData<&'h [u8]>,
        start: *const u8,
        end: *const u8,
        current: *const u8,
        mask: u64,
    }

    impl<'s, 'h> NeonIndices<'s, 'h> {
        #[inline]
        fn new(searcher: &'s NeonSearcher, haystack: &'h [u8]) -> Self {
            let ptr = haystack.as_ptr();

            Self {
                searcher,
                haystack: PhantomData,
                start: ptr,
                end: ptr.wrapping_add(haystack.len()),
                current: ptr,
                mask: 0,
            }
        }
    }

    impl NeonIndices<'_, '_> {
        #[inline]
        pub unsafe fn _next_in_current_mask(&mut self) -> Option<usize> {
            let mask = self.mask;
            let current = self.current;

            if mask != 0 {
                let offset = current.sub(CHUNK_SIZE).add(first_offset(mask));
                self.mask = clear_least_significant_bit(mask);
                self.current = current;

                Some(offset.distance(self.start))
            } else {
                None
            }
        }

        pub unsafe fn next(&mut self) -> Option<usize> {
            if self.start >= self.end {
                return None;
            }

            let mut mask = self.mask;
            let mut current = self.current;
            let start = self.start;
            let len = self.end.distance(start);
            let delimiter_splat = self.searcher.delimiter_splat;
            let newline_splat = self.searcher.newline_splat;
            let quote_splat = self.searcher.quote_splat;

            'main: loop {
                // Processing current move mask
                if mask != 0 {
                    let offset = current.sub(CHUNK_SIZE).add(first_offset(mask));
                    self.mask = clear_least_significant_bit(mask);
                    self.current = current;

                    return Some(offset.distance(start));
                }

                // Main loop of unaligned loads
                if len >= CHUNK_SIZE {
                    let vectorized_end = self.end.sub(CHUNK_SIZE);

                    while current <= vectorized_end {
                        let chunk = vld1q_u8(current);
                        let cmp1 = vceqq_u8(chunk, delimiter_splat);
                        let cmp2 = vceqq_u8(chunk, newline_splat);
                        let cmp3 = vceqq_u8(chunk, quote_splat);
                        let cmp = vorrq_u8(cmp1, cmp2);
                        let cmp = vorrq_u8(cmp, cmp3);

                        mask = neon_movemask(cmp);

                        current = current.add(CHUNK_SIZE);

                        if mask != 0 {
                            continue 'main;
                        }
                    }
                }

                // Processing remaining bytes linearly
                while current < self.end {
                    if *current == self.searcher.delimiter
                        || *current == self.searcher.newline
                        || *current == self.searcher.quote
                    {
                        let offset = current.distance(start);
                        self.current = current.add(1);
                        return Some(offset);
                    }
                    current = current.add(1);
                }

                return None;
            }
        }
    }
}

/// Returns the SIMD instructions used by this crate's amortized `memchr`-like
/// searcher.
///
/// Note that `memchr` routines, also used by this crate might use different
/// instruction sets.
pub fn searcher_simd_instructions() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        "sse2"
    }

    #[cfg(target_arch = "aarch64")]
    {
        "neon"
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        "none"
    }
}

#[cfg(target_arch = "x86_64")]
pub use x86_64::Mask;

#[derive(Debug)]
pub struct Searcher {
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    inner: x86_64::sse2::SSE2Searcher,

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    inner: x86_64::avx2::AVX2Searcher,

    #[cfg(target_arch = "aarch64")]
    inner: aarch64::NeonSearcher,

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    inner: memchr::arch::all::memchr::Three,
}

impl Searcher {
    #[inline(always)]
    pub fn new(delimiter: u8, newline: u8, quote: u8) -> Self {
        #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
        {
            unsafe {
                Self {
                    inner: x86_64::sse2::SSE2Searcher::new(delimiter, newline, quote),
                }
            }
        }

        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        {
            unsafe {
                Self {
                    inner: x86_64::avx2::AVX2Searcher::new(delimiter, newline, quote),
                }
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            unsafe {
                Self {
                    inner: aarch64::NeonSearcher::new(delimiter, newline, quote),
                }
            }
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self {
                inner: memchr::arch::all::memchr::Three::new(delimiter, newline, quote),
            }
        }
    }

    #[inline(always)]
    pub fn search<'s, 'h>(&'s self, haystack: &'h [u8]) -> Indices<'s, 'h> {
        #[cfg(target_arch = "x86_64")]
        {
            Indices {
                inner: self.inner.iter(haystack),
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            Indices {
                inner: self.inner.iter(haystack),
            }
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Indices {
                inner: self.inner.iter(haystack),
            }
        }
    }

    #[inline(always)]
    pub fn process_chunk(&self, haystack: &[u8]) -> (usize, (Mask, Mask)) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.inner.process_chunk(haystack)
        }

        #[cfg(target_arch = "aarch64")]
        {
            unimplemented!()
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            unimplemented!()
        }
    }
}

#[derive(Debug)]
pub struct Indices<'s, 'h> {
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    inner: x86_64::sse2::SSE2Indices<'s, 'h>,

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    inner: x86_64::avx2::AVX2Indices<'s, 'h>,

    #[cfg(target_arch = "aarch64")]
    inner: aarch64::NeonIndices<'s, 'h>,

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    inner: memchr::arch::all::memchr::ThreeIter<'s, 'h>,
}

impl FusedIterator for Indices<'_, '_> {}

impl Iterator for Indices<'_, '_> {
    type Item = usize;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        #[cfg(target_arch = "x86_64")]
        {
            unsafe { self.inner.next() }
        }

        #[cfg(target_arch = "aarch64")]
        {
            unsafe { self.inner.next() }
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            self.inner.next()
        }
    }
}

impl Indices<'_, '_> {
    #[inline(always)]
    pub fn _next_in_current_mask(&mut self) -> Option<usize> {
        #[cfg(target_arch = "x86_64")]
        {
            unsafe { self.inner._next_in_current_mask() }
        }

        #[cfg(target_arch = "aarch64")]
        {
            unsafe { self.inner._next_in_current_mask() }
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use memchr::arch::all::memchr::Three;

    static TEST_STRING: &[u8] = b"name,\"surname\",age,color,oper\n,\n,\nation,punctuation\nname,surname,age,color,operation,punctuation";
    static TEST_STRING_OFFSETS: &[usize; 18] = &[
        4, 5, 13, 14, 18, 24, 29, 30, 31, 32, 33, 39, 51, 56, 64, 68, 74, 84,
    ];

    #[test]
    fn test_scalar_searcher() {
        fn split(haystack: &[u8]) -> Vec<usize> {
            let searcher = Three::new(b',', b'"', b'\n');
            searcher.iter(haystack).collect()
        }

        let offsets = split(TEST_STRING);
        assert_eq!(offsets, TEST_STRING_OFFSETS);

        // Not found at all
        assert!(split("b".repeat(75).as_bytes()).is_empty());

        // Regular
        assert_eq!(split("b,".repeat(75).as_bytes()).len(), 75);

        // Exactly 64
        assert_eq!(split("b,".repeat(64).as_bytes()).len(), 64);

        // Less than 32
        assert_eq!(split("b,".repeat(25).as_bytes()).len(), 25);

        // Less than 16
        assert_eq!(split("b,".repeat(13).as_bytes()).len(), 13);
    }

    #[test]
    fn test_searcher() {
        fn split(haystack: &[u8]) -> Vec<usize> {
            let searcher = Searcher::new(b',', b'"', b'\n');
            searcher.search(haystack).collect()
        }

        let offsets = split(TEST_STRING);
        assert_eq!(offsets, TEST_STRING_OFFSETS);

        // Not found at all
        assert!(split("b".repeat(75).as_bytes()).is_empty());

        // Regular
        assert_eq!(split("b,".repeat(75).as_bytes()).len(), 75);

        // Exactly 64
        assert_eq!(split("b,".repeat(64).as_bytes()).len(), 64);

        // Less than 32
        assert_eq!(split("b,".repeat(25).as_bytes()).len(), 25);

        // Less than 16
        assert_eq!(split("b,".repeat(13).as_bytes()).len(), 13);

        // Complex input
        let complex = b"name,surname,age\n\"john\",\"landy, the \"\"everlasting\"\" bastard\",45\nlucy,rose,\"67\"\njermaine,jackson,\"89\"\n\nkarine,loucan,\"52\"\nrose,\"glib\",12\n\"guillaume\",\"plique\",\"42\"\r\n";
        let complex_indices = split(complex);

        assert!(complex_indices
            .iter()
            .copied()
            .all(|c| complex[c] == b',' || complex[c] == b'\n' || complex[c] == b'"'));

        assert_eq!(
            complex_indices,
            Three::new(b',', b'\n', b'"')
                .iter(complex)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    fn test_process_chunk() {
        let searcher = Searcher::new(b',', b'\n', b'"');

        // Only delimiters
        let buffer: &[u8; 16] = b"name,surname,las";

        let (read, (delimiter_mask, structural_mask)) = searcher.process_chunk(buffer);

        assert_eq!(read, 16);
        assert_eq!(structural_mask.into_first_offset(), None);
        assert_eq!(delimiter_mask.into_iter().collect::<Vec<_>>(), vec![4, 12]);

        // Newline
        let buffer: &[u8; 16] = b"name,sur\nname,la";

        let (read, (delimiter_mask, structural_mask)) = searcher.process_chunk(buffer);

        assert_eq!(read, 16);
        assert_eq!(structural_mask.into_first_offset(), Some(8));
        assert_eq!(delimiter_mask.into_iter().collect::<Vec<_>>(), vec![4]);

        // Quote
        let buffer: &[u8; 16] = b"name,sur\"name,la";

        let (read, (delimiter_mask, structural_mask)) = searcher.process_chunk(buffer);

        assert_eq!(read, 16);
        assert_eq!(structural_mask.into_first_offset(), Some(8));
        assert_eq!(delimiter_mask.into_iter().collect::<Vec<_>>(), vec![4]);

        // Comma in first position
        let buffer: &[u8; 16] = b",name,sur\"name,l";

        let (read, (delimiter_mask, structural_mask)) = searcher.process_chunk(buffer);

        assert_eq!(read, 16);
        assert_eq!(structural_mask.into_first_offset(), Some(9));
        assert_eq!(delimiter_mask.into_iter().collect::<Vec<_>>(), vec![0, 5]);

        // All commas neutralized
        let buffer: &[u8; 16] = b"\",ame,sur\"name,l";

        let (read, (delimiter_mask, structural_mask)) = searcher.process_chunk(buffer);

        assert_eq!(read, 16);
        assert_eq!(structural_mask.into_first_offset(), Some(0));
        assert_eq!(
            delimiter_mask.into_iter().collect::<Vec<usize>>(),
            Vec::<usize>::new()
        );

        // Longer buffers
        let buffer = b"name,surname,lasbuthasotherstuffi,nthetailofthebuffer,";

        let (read, (delimiter_mask, structural_mask)) = searcher.process_chunk(buffer);

        assert_eq!(read, 16);
        assert_eq!(structural_mask.into_first_offset(), None);
        assert_eq!(delimiter_mask.into_iter().collect::<Vec<_>>(), vec![4, 12]);

        // Empty buffer
        let buffer = b"";

        let (read, (delimiter_mask, structural_mask)) = searcher.process_chunk(buffer);

        assert_eq!(read, 0);
        assert_eq!(structural_mask.into_first_offset(), None);
        assert_eq!(
            delimiter_mask.into_iter().collect::<Vec<_>>(),
            Vec::<usize>::new()
        );

        // Tail buffer
        let buffer = b"a,b,\nc,";

        let (read, (delimiter_mask, structural_mask)) = searcher.process_chunk(buffer);

        assert_eq!(read, 7);
        assert_eq!(structural_mask.into_first_offset(), Some(4));
        assert_eq!(delimiter_mask.into_iter().collect::<Vec<_>>(), vec![1, 3]);
    }
}
