use std::fs::File;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Path to target CSV file
    path: String,
}

impl Args {
    fn delimiter(&self) -> u8 {
        if self.path.ends_with(".tsv") {
            b'\t'
        } else {
            b','
        }
    }
}

const DEFAULT_CAPACITY: usize = 1024 * (1 << 10);

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let delimiter = args.delimiter();
    let file = File::open(&args.path)?;

    let mut reader = simd_csv::ReaderBuilder::with_capacity(DEFAULT_CAPACITY)
        .has_headers(false)
        .delimiter(delimiter)
        .from_reader(file);

    let mut record = simd_csv::ByteRecord::new();
    let mut data = Vec::new();
    let mut count: u64 = 0;
    let mut count_quoted: u64 = 0;
    let mut cell_len_sum: u64 = 0;

    while reader.read_byte_record(&mut record)? {
        let mut any_quoted = false;

        for cell in record.iter() {
            cell_len_sum += cell.len() as u64;

            let i = cell
                .iter()
                .position(|&c| c == b'"' || c == delimiter || c == b'\r' || c == b'\n');

            any_quoted |= i.is_some();

            data.push(i);
        }

        count += 1;

        if any_quoted {
            count_quoted += 1;
        }
    }

    let should_quote_count = data.iter().flatten().count();
    let percentage_of_should_quote = should_quote_count as f64 / data.len() as f64;

    let position_sum = data.iter().flatten().sum::<usize>();

    let mut first_char_positions = data.iter().flatten().copied().collect::<Vec<_>>();
    first_char_positions.sort();

    let median = first_char_positions[first_char_positions.len() / 2];

    let max_char_position = *first_char_positions.iter().max().unwrap();

    eprintln!("Number of rows: {}", count);
    eprintln!(
        "Ratio of rows with at least one column quoted: {}",
        count_quoted as f64 / count as f64
    );

    eprintln!(
        "Ratio of cells needing quote: {}",
        percentage_of_should_quote
    );

    eprintln!(
        "Mean position of first char needing to be quoted: {}",
        position_sum as f64 / should_quote_count as f64
    );

    eprintln!(
        "Median position of first char needing to be quoted: {}",
        median
    );

    eprintln!(
        "Max position of first char needing to be quoted: {}",
        max_char_position
    );

    eprintln!(
        "Mean cell length: {}",
        cell_len_sum as f64 / data.len() as f64
    );

    Ok(())
}
