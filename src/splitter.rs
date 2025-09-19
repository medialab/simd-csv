#[cfg(test)]
mod tests {
    use memchr::arch::all::memchr::Three;

    static TEST_STRING: &[u8]  = b"name,\"surname\",age,color,oper\n,\n,\nation,punctuation\nname,surname,age,color,operation,punctuation";

    #[test]
    fn test_scalar_splitter() {
        fn split(haystack: &[u8]) -> Vec<usize> {
            let searcher = Three::new(b',', b'"', b'\n');
            searcher.iter(haystack).collect()
        }

        let offsets = split(TEST_STRING);
        dbg!(
            &offsets,
            offsets
                .iter()
                .copied()
                .map(|i| bstr::BStr::new(&TEST_STRING[i..i + 1]))
                .collect::<Vec<_>>()
        );

        // Not found at all
        assert!(split("b".repeat(75).as_bytes()).is_empty());

        // Regular
        assert_eq!(split("b,".repeat(75).as_bytes()).len(), 75);

        // Exactly 64
        assert_eq!(split("b,".repeat(64).as_bytes()).len(), 64);
    }
}
