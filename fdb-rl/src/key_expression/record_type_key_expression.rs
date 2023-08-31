//! TODO
//
// Takes `KeyExpression` and `WellFormedMessageDescriptor` and
// verifies that `KeyExpression` can correctly act on
// `WellFormedMessageDescriptor`. If that is possible, returns a value
// of type `RecordTypeKeyExpression.
//
// Once you have a value of type `RecordTypeKeyExpression`, you can
// get the `TupleSchema` that it would return. Additionally, you can
// give the value of type `RecordTypeKeyExpression` a value of `T
// where T: Message + Default` (a prost struct), and you will get a
// value of `Tuple` back.
