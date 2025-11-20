#![no_main]

use libfuzzer_sys::fuzz_target;

use simd_csv::{ByteRecord, ReaderBuilder};

fuzz_target!(|data: &[u8]| {
    let mut reader = ReaderBuilder::new().flexible(true).from_reader(data);
    let mut record = ByteRecord::new();

    while reader.read_byte_record(&mut record).unwrap() {}
});
