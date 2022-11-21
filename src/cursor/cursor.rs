use futures::Future;

use crate::cursor::{CursorError, CursorResult};

/// Prevent users from implementing private trait.
mod private {
    use crate::cursor::{CursorSkip, KeyValueCursor};

    pub trait Sealed {}

    impl Sealed for KeyValueCursor {}

    impl<C> Sealed for CursorSkip<C> {}
}

/// An asynchronous iterator that supports [`Continuation`].
///
/// A continuation is an opaque token that represents the position of
/// the cursor. A continuation can be at the *begin marker* which is
/// the position before any items are returned. It can be somewhere in
/// the middle or it can be at the *end marker* position. End marker
/// would be the position after all the items are returned.
///
/// When a [`Cursor::next`] stops producing values and assuming there
/// was no [`FdbError`], then the reason for not producing the values
/// is reported using [`NoNextReason`]. This is returned as part of
/// [`CursorResult`].
///
/// No next reasons are fundamentally distinguished between those that
/// are due to the data itself (in-band) and those that are due to the
/// environment / context (out-of-band). For example, running out of
/// data or having returned the maximum number of values requested are
/// in-band, while reaching a limit on the number of key-value pairs
/// scanned by the transaction or the time that a transaction has been
/// open are out-of-band.
///
/// [`Continuation`]: crate::cursor::Continuation
/// [`FdbError`]: fdb::error::FdbError
/// [`NoNextReason`]: crate::cursor::NoNextReason
pub trait Cursor<T>: private::Sealed {
    /// Next asynchronously return value from this cursor.
    type Next<'a>: Future<Output = CursorResult<T>> + 'a
    where
        Self: 'a,
        T: 'a;

    /// Asynchronously return the next result from this cursor.
    // For clarity
    #[allow(clippy::needless_lifetimes)]
    fn next<'a>(&'a mut self) -> Self::Next<'a>;

    /// Get a new cursor that skips the given number of cursor values.
    fn skip(self, skip: usize) -> CursorSkip<Self>
    where
        Self: Sized,
    {
        CursorSkip::new(self, skip, None)
    }
}

/// Cursor for the [`skip`] method.
///
/// [`skip`]: Cursor::skip
#[derive(Debug)]
pub struct CursorSkip<C> {
    cursor: C,
    remaining: usize,
    error: Option<CursorError>,
}

impl<C> CursorSkip<C> {
    fn new(cursor: C, remaining: usize, error: Option<CursorError>) -> CursorSkip<C> {
        CursorSkip {
            cursor,
            remaining,
            error,
        }
    }
}

impl<T, C> Cursor<T> for CursorSkip<C>
where
    C: Cursor<T>,
{
    type Next<'a> = impl Future<Output = CursorResult<T>> + 'a
    where Self: 'a, T: 'a;

    fn next(&mut self) -> Self::Next<'_> {
        async move {
            // If the underlying cursor has previously returned an
            // error, return the last seen error.
            if let Some(x) = &self.error {
                return Err(x.clone());
            }

            if self.remaining == 0 {
                let res = self.cursor.next().await;
                if let Err(e) = res {
                    self.error = Some(e.clone());
                    return Err(e);
                }
            }

            // remaining != 0 and we have not seen any error. In that
            // case, run the loop till we encounter an error or
            // remaining becomes `0`.

            let iter = self.remaining;
            for n in 0..iter {
                let res = self.cursor.next().await;
                if let Err(e) = res {
                    self.error = Some(e.clone());
                    return Err(e);
                }
                self.remaining -= 1;
            }

            let res = self.cursor.next().await;
            if let Err(e) = res {
                self.error = Some(e.clone());
                return Err(e);
            }

            res
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO write unit tests for CursorSkip, etc.,
}
