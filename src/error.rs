use std::{error, fmt, io, result};

#[derive(Debug)]
pub enum ErrorKind {
    Io(io::Error),
    UnequalLengths {
        expected_len: usize,
        len: usize,
        pos: Option<(u64, u64)>,
    },
}

#[derive(Debug)]
pub struct Error(ErrorKind);

impl Error {
    pub fn new(kind: ErrorKind) -> Self {
        Self(kind)
    }

    pub fn is_io_error(&self) -> bool {
        matches!(self.0, ErrorKind::Io(_))
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }

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
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
