use displaydoc::Display;

/// The kinds of errors that can occur when calculating a linear regression.
#[derive(Copy, Clone, Display, Debug, PartialEq)]
pub enum Error {
    /// The slope is too steep to represent, approaching infinity.
    TooSteep,
    /// Failed to calculate mean.
    ///
    /// This means the input was empty or had too many elements.
    Mean,
    /// Lengths of the inputs are different.
    InputLenDif,
    /// Can't compute linear regression of zero elements
    NoElements,
}

