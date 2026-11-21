#![no_main]

use libfuzzer_sys::fuzz_target;

use simd_csv::{ErrorKind, ReaderBuilder, StringRecord};

fuzz_target!(|data: &[u8]| {
    let mut reader = ReaderBuilder::new().flexible(true).from_reader(data);
    let mut record = StringRecord::new();

    loop {
        match reader.read_record(&mut record) {
            Ok(true) => for _ in record.iter() {},
            Ok(false) => {
                break;
            }
            Err(err) => assert!(matches!(err.into_kind(), ErrorKind::Utf8Error)),
        }
    }
});
