use std::{error, fmt, io, result};

/// The specific type of an error.
#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Wrap a [std::io::Error].
    Io(io::Error),

    /// Indicate that a non-flexible reader or writer attempted to read/write a
    /// unaligned record having an incorrect number of fields.
    UnequalLengths {
        /// Expected number of fields
        expected_len: usize,
        /// Actual and incorrect number of fields observed
        len: usize,
        /// Optional position `(byte_offset, record_index)`
        pos: Option<(u64, u64)>,
    },

    /// Indicate that a [`Seeker`](crate::Seeker) attempted to find a record in
    /// a position that is out of bounds
    OutOfBounds {
        /// Desired position
        pos: u64,
        /// Byte offset of the first record
        start: u64,
        /// Byte length of the considered stream
        end: u64,
    },
}

/// An error occurring when reading/writing CSV data.
#[derive(Debug)]
pub struct Error(ErrorKind);

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Self {
        Self(kind)
    }

    /// Return whether the wrapped error is a [`std::io::Error`].
    pub fn is_io_error(&self) -> bool {
        matches!(self.0, ErrorKind::Io(_))
    }

    /// Return a reference to the underlying [`ErrorKind`].
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }

    /// Unwraps the error into its underlying [`ErrorKind`].
    pub fn into_kind(self) -> ErrorKind {
        self.0
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self(ErrorKind::Io(err))
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        Self::new(io::ErrorKind::Other, err)
    }
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            ErrorKind::Io(ref err) => err.fmt(f),
            ErrorKind::UnequalLengths {
                expected_len,
                len,
                pos: Some((byte, index))
            } => write!(
                f,
                "CSV error: record {} (byte: {}): found record with {} fields, but the previous record has {} fields",
                index, byte, len, expected_len
            ),
             ErrorKind::UnequalLengths {
                expected_len,
                len,
                pos: None
            } => write!(
                f,
                "CSV error: found record with {} fields, but the previous record has {} fields",
                len, expected_len
            ),
            ErrorKind::OutOfBounds { pos, start, end } => {
                write!(f, "pos {} is out of bounds (should be >= {} and < {})", pos, start, end)
            }
        }
    }
}

/// A type alias for `Result<T, simd_csv::Error>`.
pub type Result<T> = result::Result<T, Error>;
