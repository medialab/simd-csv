pub fn simd_features_enabled() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            "avx2"
        } else {
            "sse2"
        }
    }
    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
    {
        "simd128"
    }

    #[cfg(target_arch = "aarch64")]
    {
        "neon"
    }

    #[cfg(not(any(
        target_arch = "x86_64",
        all(target_arch = "wasm32", target_feature = "simd128"),
        target_arch = "aarch64"
    )))]
    {
        "none"
    }
}

#[cfg(test)]
mod tests {
    use memchr::arch::all::memchr::Three;

    static TEST_STRING: &[u8]  = b"name,\"surname\",age,color,oper\n,\n,\nation,punctuation\nname,surname,age,color,operation,punctuation";
    static TEST_STRING_OFFSETS: &[usize; 18] = &[
        4, 5, 13, 14, 18, 24, 29, 30, 31, 32, 33, 39, 51, 56, 64, 68, 74, 84,
    ];

    #[test]
    fn test_scalar_splitter() {
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
    }
}
