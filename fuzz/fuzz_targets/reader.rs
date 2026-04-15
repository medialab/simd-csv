#![no_main]

use libfuzzer_sys::fuzz_target;

use simd_csv::{ByteRecord, ErrorKind, ReaderBuilder};

fuzz_target!(|data: &[u8]| {
    let mut reader = ReaderBuilder::new().flexible(true).from_reader(data);
    let mut record = ByteRecord::new();

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => for _ in record.iter() {},
            Ok(false) => {
                break;
            }
            Err(err) => unreachable!(),
        }
    }
});
