use clap::Parser;
use csv::{ByteRecord, ReaderBuilder};

#[derive(Parser, Debug)]
struct Args {
    /// Path to target CSV file
    path: String
}

fn main() -> csv::Result<()> {
    let args = Args::parse();

    let reader_builder = ReaderBuilder::new();
    let mut reader = reader_builder.from_path(&args.path)?;

    let mut count: u64 = 0;
    let mut record = ByteRecord::new();

    while reader.read_byte_record(&mut record)? {
        count += 1;
    }

    println!("{}", count);

    Ok(())
}