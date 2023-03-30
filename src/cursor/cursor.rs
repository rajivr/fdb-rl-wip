use std::future::Future;
use std::marker::PhantomData;

use crate::cursor::{CursorError, CursorResult, CursorSuccess};

/// Prevent users from implementing private trait.
mod private {
    use crate::cursor::{Cursor, KeyValueCursor};
    use crate::raw_record::RawRecordCursor;

    use super::{CursorFilter, CursorMap};

    pub trait Sealed {}

    impl<T, C, F> Sealed for CursorMap<T, C, F> where C: Cursor<T> {}

    impl<T, C, F> Sealed for CursorFilter<T, C, F> where C: Cursor<T> {}

    impl Sealed for KeyValueCursor {}

    impl Sealed for RawRecordCursor {}
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
//
// Unlike Java RecordLayer, we do not have monadic abstractions (i.e.,
// methods such as `flat_map`, `flatten` etc.,). This is because in
// the `next` method, `CursorResult` returns a continuation. When
// cursors are composed, we have to reason about how the continuations
// will get composed, and if the composition of the continuations is
// correct. A related issue is that when we need throughput, we need
// to use pipelining, which can interact with continuations and
// parallel cursors in subtle ways. Once we have a good handle on
// these issues, we can explore how to add methods for cursor
// composition.
pub trait Cursor<T>: private::Sealed {
    /// Asynchronously return the next result from this cursor.
    async fn next(&mut self) -> CursorResult<T>;

    /// Drain the cursor pushing all emitted values into a collection.
    async fn collect(mut self) -> (Vec<T>, CursorError)
    where
        Self: Sized,
    {
        let mut v = Vec::new();

        let iter = &mut self;

        loop {
            match iter.next().await {
                Ok(t) => v.push(t.into_value()),
                Err(err) => return (v, err),
            }
        }
    }

    /// Filters the values produced by this cursor according to the
    /// provided predicate.
    ///
    /// # Note
    ///
    /// Unlike a general iterator, the computation that you can do
    /// inside the async closure is limited. Specifically the closure
    /// `FnMut(&T) -> impl Future<Output = bool>` does not provide a
    /// mechanism to return an error that might possibly occur within
    /// the closure.
    ///
    /// If you need this feature, you will need to build the loop
    /// yourself and handle the issue of the how and which
    /// continuation to return that would be useful to the caller.
    ///
    /// Also, since [`Cursor`] is a sealed class, this method is
    /// primarily meant for types defined in this crate.
    //
    // *Note:* The reason for not providing the capability for
    //         returning an error from within the closure is because
    //         it is not clear if the API user would want the
    //         continuation at the point of the error or the
    //         continuation *before* the error occurred.
    //
    //         Providing the latter, would require maintaining
    //         additional state when creating a value of
    //         `CursorFilter` type. We will also need a generic way of
    //         extracting a continuation from a `Cursor`.
    //
    //         This means we will need to introduce another API such
    //         as `fn get_continuation(&mut self) ->
    //         CursorResultContinuation` on the `Cursor` trait. Till
    //         the semantics of the `Cursor` is properly understood we
    //         want to keep the `Cursor` API as minimal as possible.
    //
    //         We can always roll the feature we need in the `next`
    //         method implementation and in the builder type for the
    //         `Cursor`. It won't be generic, but will get the job
    //         done.
    async fn filter<F, Fut>(self, f: F) -> CursorFilter<T, Self, F>
    where
        Self: Sized,
        F: FnMut(&T) -> Fut,
        Fut: Future<Output = bool>,
    {
        CursorFilter {
            cursor: self,
            f,
            phantom: PhantomData,
        }
    }

    /// Map this cursor's items to a different type, returning a new
    /// cursor of the resulting type.
    ///
    /// # Note
    ///
    /// Unlike a general iterator, the computation that you can do
    /// inside the async closure is limited. Specifically the closure
    /// `FnMut(T) -> impl Future<Output = U>` does not provide a
    /// mechanism to return an error that might possibly occur within
    /// the closure.
    ///
    /// If you need this feature, you will need to build the loop
    /// yourself and handle the issue of the how and which
    /// continuation to return that would be useful to the caller.
    ///
    /// Also, since [`Cursor`] is a sealed class, this method is
    /// primarily meant for types defined in this crate.
    //
    // *Note:* See the comment mentioned in `filter`.
    async fn map<U, F, Fut>(self, f: F) -> CursorMap<T, Self, F>
    where
        Self: Sized,
        F: FnMut(T) -> Fut,
        Fut: Future<Output = U>,
    {
        CursorMap {
            cursor: self,
            f,
            phantom: PhantomData,
        }
    }
}

/// Cursor returned by [`Cursor::filter`] method.
#[derive(Debug)]
pub struct CursorFilter<T, C, F>
where
    C: Cursor<T>,
{
    cursor: C,
    f: F,
    phantom: PhantomData<T>,
}

impl<T, C, F, Fut> Cursor<T> for CursorFilter<T, C, F>
where
    C: Cursor<T>,
    F: FnMut(&T) -> Fut,
    Fut: Future<Output = bool>,
{
    async fn next(&mut self) -> CursorResult<T> {
        loop {
            let item = self.cursor.next().await;

            match item {
                Ok(cursor_success) => {
                    let (value, continuation) = cursor_success.into_parts();

                    if ((self.f)(&value)).await {
                        return Ok(CursorSuccess::new(value, continuation));
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Cursor returned by [`Cursor::map`] method.
#[derive(Debug)]
pub struct CursorMap<T, C, F>
where
    C: Cursor<T>,
{
    cursor: C,
    f: F,
    phantom: PhantomData<T>,
}

impl<U, T, C, F, Fut> Cursor<U> for CursorMap<T, C, F>
where
    C: Cursor<T>,
    F: FnMut(T) -> Fut,
    Fut: Future<Output = U>,
{
    async fn next(&mut self) -> CursorResult<U> {
        let item = self.cursor.next().await;

        match item {
            Ok(cursor_success) => Ok({
                let (value, continuation) = cursor_success.into_parts();
                CursorSuccess::new(((self.f)(value)).await, continuation)
            }),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    // No tests here as we are just defining traits.
}
