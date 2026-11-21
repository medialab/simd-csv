#![no_main]

use libfuzzer_sys::fuzz_target;

use simd_csv::Splitter;

fuzz_target!(|data: &[u8]| {
    let mut reader = Splitter::from_reader(data);

    loop {
        if let Some(record) = reader.split_record().unwrap() {
            for _ in record.iter() {}
        } else {
            break;
        }
    }
});
