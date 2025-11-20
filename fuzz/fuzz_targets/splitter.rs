#![no_main]

use libfuzzer_sys::fuzz_target;

use simd_csv::Splitter;

fuzz_target!(|data: &[u8]| {
    let mut reader = Splitter::from_reader(data);

    while let Some(_) = reader.split_record().unwrap() {}
});
