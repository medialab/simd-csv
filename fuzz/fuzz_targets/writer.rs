#![no_main]

use libfuzzer_sys::fuzz_target;

use simd_csv::{ByteRecord, WriterBuilder};
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = WriterBuilder::new().flexible(true).from_writer(cursor);
    let mut record = ByteRecord::new();
    record.push_field(data);

    writer.write_byte_record(&record).unwrap();
});
