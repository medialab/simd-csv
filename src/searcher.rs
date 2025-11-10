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

    pub mod sse2 {
        use super::*;

        use core::arch::x86_64::{
            __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_or_si128,
            _mm_set1_epi8,
        };

        #[derive(Debug)]
        pub struct SSE2Searcher {
            n1: u8,
            n2: u8,
            n3: u8,
            v1: __m128i,
            v2: __m128i,
            v3: __m128i,
        }

        impl SSE2Searcher {
            #[inline]
            pub unsafe fn new(n1: u8, n2: u8, n3: u8) -> Self {
                Self {
                    n1,
                    n2,
                    n3,
                    v1: _mm_set1_epi8(n1 as i8),
                    v2: _mm_set1_epi8(n2 as i8),
                    v3: _mm_set1_epi8(n3 as i8),
                }
            }

            #[inline(always)]
            pub fn iter<'s, 'h>(&'s self, haystack: &'h [u8]) -> SSE2Indices<'s, 'h> {
                SSE2Indices::new(self, haystack)
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

        const SSE2_STEP: usize = 16;

        impl SSE2Indices<'_, '_> {
            pub unsafe fn next(&mut self) -> Option<usize> {
                if self.start >= self.end {
                    return None;
                }

                let mut mask = self.mask;
                let vectorized_end = self.end.sub(SSE2_STEP);
                let mut current = self.current;
                let start = self.start;
                let v1 = self.searcher.v1;
                let v2 = self.searcher.v2;
                let v3 = self.searcher.v3;

                'main: loop {
                    // Processing current move mask
                    if mask != 0 {
                        let offset = current.sub(SSE2_STEP).add(first_offset(mask));
                        self.mask = clear_least_significant_bit(mask);
                        self.current = current;

                        return Some(offset.distance(start));
                    }

                    // Main loop of unaligned loads
                    while current <= vectorized_end {
                        let chunk = _mm_loadu_si128(current as *const __m128i);
                        let cmp1 = _mm_cmpeq_epi8(chunk, v1);
                        let cmp2 = _mm_cmpeq_epi8(chunk, v2);
                        let cmp3 = _mm_cmpeq_epi8(chunk, v3);
                        let cmp = _mm_or_si128(cmp1, cmp2);
                        let cmp = _mm_or_si128(cmp, cmp3);

                        mask = _mm_movemask_epi8(cmp) as u32;

                        current = current.add(SSE2_STEP);

                        if mask != 0 {
                            continue 'main;
                        }
                    }

                    // Processing remaining bytes linearly
                    while current < self.end {
                        if *current == self.searcher.n1
                            || *current == self.searcher.n2
                            || *current == self.searcher.n3
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
        n1: u8,
        n2: u8,
        n3: u8,
        v1: uint8x16_t,
        v2: uint8x16_t,
        v3: uint8x16_t,
    }

    impl NeonSearcher {
        #[inline]
        pub unsafe fn new(n1: u8, n2: u8, n3: u8) -> Self {
            Self {
                n1,
                n2,
                n3,
                v1: vdupq_n_u8(n1),
                v2: vdupq_n_u8(n2),
                v3: vdupq_n_u8(n3),
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

    const SSE2_STEP: usize = 16;

    impl NeonIndices<'_, '_> {
        pub unsafe fn next(&mut self) -> Option<usize> {
            if self.start >= self.end {
                return None;
            }

            let mut mask = self.mask;
            let vectorized_end = self.end.sub(SSE2_STEP);
            let mut current = self.current;
            let start = self.start;
            let v1 = self.searcher.v1;
            let v2 = self.searcher.v2;
            let v3 = self.searcher.v3;

            'main: loop {
                // Processing current move mask
                if mask != 0 {
                    let offset = current.sub(SSE2_STEP).add(first_offset(mask));
                    self.mask = clear_least_significant_bit(mask);
                    self.current = current;

                    return Some(offset.distance(start));
                }

                // Main loop of unaligned loads
                while current <= vectorized_end {
                    let chunk = vld1q_u8(current);
                    let cmp1 = vceqq_u8(chunk, v1);
                    let cmp2 = vceqq_u8(chunk, v2);
                    let cmp3 = vceqq_u8(chunk, v3);
                    let cmp = vorrq_u8(cmp1, cmp2);
                    let cmp = vorrq_u8(cmp, cmp3);

                    mask = neon_movemask(cmp);

                    current = current.add(SSE2_STEP);

                    if mask != 0 {
                        continue 'main;
                    }
                }

                // Processing remaining bytes linearly
                while current < self.end {
                    if *current == self.searcher.n1
                        || *current == self.searcher.n2
                        || *current == self.searcher.n3
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

/// Returns the SIMD instructions set used by this crate's amortized
/// `memchr`-like searcher.
///
/// Note that `memchr` routines, also used by this crate might use
/// different instruction sets.
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

#[derive(Debug)]
pub struct Searcher {
    #[cfg(target_arch = "x86_64")]
    inner: x86_64::sse2::SSE2Searcher,

    #[cfg(target_arch = "aarch64")]
    inner: aarch64::NeonSearcher,

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    inner: memchr::arch::all::memchr::Three,
}

impl Searcher {
    #[inline(always)]
    pub fn new(n1: u8, n2: u8, n3: u8) -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            unsafe {
                Self {
                    inner: x86_64::sse2::SSE2Searcher::new(n1, n2, n3),
                }
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            unsafe {
                Self {
                    inner: aarch64::NeonSearcher::new(n1, n2, n3),
                }
            }
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self {
                inner: memchr::arch::all::memchr::Three::new(n1, n2, n3),
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
}

#[derive(Debug)]
pub struct Indices<'s, 'h> {
    #[cfg(target_arch = "x86_64")]
    inner: x86_64::sse2::SSE2Indices<'s, 'h>,

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
}
