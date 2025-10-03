use std::{error, fmt, io, result};

#[derive(Debug)]
enum ErrorKind {
    Io(io::Error),
    UnequalLengths { expected_len: usize, len: usize },
    InvalidHeaders,
}

#[derive(Debug)]
pub struct Error(ErrorKind);

impl Error {
    pub(crate) fn unequal_lengths(expected_len: usize, len: usize) -> Self {
        Self(ErrorKind::UnequalLengths { expected_len, len })
    }

    pub(crate) fn invalid_headers() -> Self {
        Self(ErrorKind::InvalidHeaders)
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
            ErrorKind::UnequalLengths { expected_len, len } => write!(
                f,
                "CSV error: found record with {} fields, but the previous record has {} fields",
                len, expected_len
            ),
            ErrorKind::InvalidHeaders => {
                write!(f, "invalid headers or headers too long for buffer")
            }
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
