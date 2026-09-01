use std::fmt;

#[derive(Debug, Clone)]
pub enum Error {
    // can't reach the database
    PostgresConnectionError(String),

    // pg_recvlogical subprocess error
    PgRecvLogicalError(String),

    // issue loading subscriptions
    Subscriptions(String),

    // generic business logic error
    Walrus(String),

    // Unexpected result from SQL function
    SQLFunction(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            Self::PostgresConnectionError(x) => x,
            Self::PgRecvLogicalError(x) => x,
            Self::Subscriptions(x) => x,
            Self::Walrus(x) => x,
            Self::SQLFunction(x) => x,
        };

        write!(f, "{}", msg)
    }
}

#[derive(Debug)]
pub enum FilterError {
    // Filter too difficult to handle locally. Delgate it to SQL
    DelegateToSQL(String),
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            Self::DelegateToSQL(x) => x,
        };

        write!(f, "{}", msg)
    }
}
