/*!
The `simd-csv` crate provides specialized readers & writers of CSV data able
to leverage [SIMD](https://en.wikipedia.org/wiki/Single_instruction,_multiple_data) instructions.

It has been designed to fit the [xan](https://github.com/medialab/xan) command line tool's
requirements, but can be used by anyone to speed up CSV parsing if needed.

Is is less flexible and user-friendly than the [`csv`](https://docs.rs/csv/) crate
so one should make sure the performance gain is worth it before going further.

This crate is not a port of [simdjson](https://arxiv.org/abs/1902.08318) branchless logic
applied to CSV parsing. It uses a somewhat novel approach instead, mixing traditional state
machine logic with [`memchr`](https://docs.rs/memchr/latest/memchr/)-like SIMD-accelerated
string searching. See the [design notes](#design-notes) for more details.

# Readers

From least to most performant. Also from most integrated to most barebone.

- [`Reader`], [`ReaderBuilder`]: a streaming copy reader, unescaping quoted data on the fly.
This is the closest thing you will find to the [`csv`](https://docs.rs/csv/) crate `Reader`.
- [`ZeroCopyReader`], [`ZeroCopyReaderBuilder`]: a streaming zero-copy reader that only find cell
delimiters and does not unescape quoted data.
- [`Splitter`], [`SplitterBuilder`]: a streaming zero-copy splitter that will only
find record delimitations, but not cell delimiters at all.
- [`LineReader`]: a streaming zero-copy line splitter that does not handle quoting at all.

You can also find more exotic readers like:

- [`TotalReader`], [`TotalReaderBuilder`]: a reader optimized to work for uses-cases when
CSV data is fully loaded into memory or with memory maps.
- [`Seeker`], [`SeekerBuilder`]: a reader able to find record start positions in a seekable CSV stream.
This can be very useful for parallelization, or more creative uses like performing binary
search in a sorted file.
- [`ReverseReader`], [`ReaderBuilder`]: a reader able to read a seekable CSV stream in reverse, in amortized linear time.

# Writers

- [`Writer`], [`WriterBuilder`]: a typical CSV writer.

# Supported targets

# Design notes

Targeting streaming parsers, minimally quoted data

reasonably fast, depend on the data

# Caveats

CRLF, quoting issues

*/
mod buffer;
mod core;
mod debug;
mod error;
mod ext;
mod line_reader;
mod reader;
mod records;
mod searcher;
mod seeker;
mod splitter;
mod total_reader;
mod utils;
mod writer;
mod zero_copy_reader;

pub use error::{Error, ErrorKind, Result};
pub use line_reader::LineReader;
pub use reader::{Reader, ReaderBuilder, ReverseReader};
pub use records::{ByteRecord, ZeroCopyByteRecord};
pub use searcher::Searcher;
pub use seeker::{Seeker, SeekerBuilder};
pub use splitter::{Splitter, SplitterBuilder};
pub use total_reader::{TotalReader, TotalReaderBuilder};
pub use utils::unescape;
pub use writer::{Writer, WriterBuilder};
pub use zero_copy_reader::{ZeroCopyReader, ZeroCopyReaderBuilder};
