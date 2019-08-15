use std::convert::From;

#[derive(Debug, Fail)]
pub enum DatabaseError {
    #[fail(display = "Transaction log error")]
    TransactionLogError,
    #[fail(display = "IO Error: {:?}", error)]
    IOError { error: std::io::Error },
    #[fail(display = "IO Error: {:?}", error)]
    PersistError { error: tempfile::PersistError },
    #[fail(display = "Invalid json format: {:?}", error)]
    JSONError { error: serde_json::Error },
    #[fail(display = "Invalid format: {:?}", error)]
    NumberFormatError { error: std::num::ParseIntError },
    #[fail(display = "Invalid log format: {:?}", message)]
    InvalidLogError { message: String },
    #[fail(display = "Key Duplication")]
    KeyDuplicationError,
    #[fail(display = "Key Not Found")]
    KeyNotFoundError,
}

impl From<std::io::Error> for DatabaseError {
    fn from(error: std::io::Error) -> Self {
        DatabaseError::IOError { error }
    }
}

impl From<serde_json::Error> for DatabaseError {
    fn from(error: serde_json::Error) -> Self {
        DatabaseError::JSONError { error }
    }
}

impl From<std::num::ParseIntError> for DatabaseError {
    fn from(error: std::num::ParseIntError) -> Self {
        DatabaseError::NumberFormatError { error }
    }
}

impl From<std::string::FromUtf8Error> for DatabaseError {
    fn from(_: std::string::FromUtf8Error) -> Self {
        DatabaseError::InvalidLogError {
            message: "Invalid UTF-8 format".to_string(),
        }
    }
}

impl From<tempfile::PersistError> for DatabaseError {
    fn from(error: tempfile::PersistError) -> Self {
        DatabaseError::PersistError { error }
    }
}
