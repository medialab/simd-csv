use std::fs::File;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Path to target CSV file
    path: String,

    /// Seek with offset
    #[arg(long)]
    offset: Option<u64>,

    /// Last record
    #[arg(long)]
    last: bool,

    /// Approx count
    #[arg(long)]
    approx_count: bool,

    /// Segments
    #[arg(long)]
    segments: Option<usize>,

    /// No headers?
    #[arg(short, long)]
    no_headers: bool,
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

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let delimiter = args.delimiter();
    let file = File::open(&args.path)?;

    let mut seeker = simd_csv::SeekerBuilder::new()
        .delimiter(delimiter)
        .has_headers(!args.no_headers)
        .from_reader(file)?
        .unwrap();

    if let Some(offset) = args.offset {
        dbg!(seeker.seek(offset)?);
    } else if args.approx_count {
        println!("{}", seeker.approx_count());
    } else if args.last {
        println!("{:?}", seeker.last_byte_record()?);
    } else if let Some(count) = args.segments {
        println!("from,to");
        for (from, to) in seeker.segments(count)? {
            println!("{},{}", from, to);
        }
    } else {
        unimplemented!()
    }

    Ok(())
}
