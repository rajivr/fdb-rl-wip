use std::fmt::Debug;

/// Low endpoint of a range.
#[derive(Debug, Clone, PartialEq)]
pub enum LowEndpoint<T>
where
    T: Debug + PartialEq,
{
    /// Start of the range.
    Start,
    /// Includes endpoint value.
    RangeInclusive(T),
    /// Excludes endpoint value.
    RangeExclusive(T),
}

impl<T> LowEndpoint<T>
where
    T: Debug + PartialEq,
{
    /// Maps an `LowEndpoint<T>` to `LowEndpoint<U>` by applying a
    /// function to a contained value.
    pub(crate) fn map<F, U>(self, f: F) -> LowEndpoint<U>
    where
        F: FnOnce(T) -> U,
        U: Debug + PartialEq,
    {
        match self {
            LowEndpoint::Start => LowEndpoint::Start,
            LowEndpoint::RangeInclusive(t) => LowEndpoint::RangeInclusive(f(t)),
            LowEndpoint::RangeExclusive(t) => LowEndpoint::RangeExclusive(f(t)),
        }
    }
}

/// High endpoint of a range.
#[derive(Debug, Clone, PartialEq)]
pub enum HighEndpoint<T>
where
    T: Debug + PartialEq,
{
    /// Includes endpoint value.
    RangeInclusive(T),
    /// Excludes endpoint value.
    RangeExclusive(T),
    /// End of a range.
    End,
}

impl<T> HighEndpoint<T>
where
    T: Debug + PartialEq,
{
    /// Maps an `HighEndpoint<T>` to `HighEndpoint<U>` by applying a
    /// function to a contained value.
    pub(crate) fn map<F, U>(self, f: F) -> HighEndpoint<U>
    where
        F: FnOnce(T) -> U,
        U: Debug + PartialEq,
    {
        match self {
            HighEndpoint::RangeInclusive(t) => HighEndpoint::RangeInclusive(f(t)),
            HighEndpoint::RangeExclusive(t) => HighEndpoint::RangeExclusive(f(t)),
            HighEndpoint::End => HighEndpoint::End,
        }
    }
}

#[cfg(test)]
mod tests {
    // No tests here as we are just defining types.
}
