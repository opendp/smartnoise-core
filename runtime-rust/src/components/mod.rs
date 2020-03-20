//! Component trait implementations
//!
//! Each component represents an abstract computation.
//! Example components are Materialize for loading a dataframe, Index for retrieving specific columns from the dataframe, Mean for aggregating data, LaplaceMechanism for privatizing data, etc.
//!
//! There are a set of possible behaviours each component may implement. Each behavior corresponds to a trait.
//! The only trait in the runtime is the Evaluable trait.
//!
//! Implementations of the Evaluable trait are distributed among the module files.

use whitenoise_validator::errors::*;
use crate::base::NodeArguments;
use whitenoise_validator::base::Value;

use whitenoise_validator::proto;
use ndarray::{ArrayD, Array, Zip};

pub mod bin;
pub mod cast;
pub mod clamp;
pub mod count;
pub mod covariance;
pub mod filter;
pub mod impute;
pub mod index;
pub mod kth_raw_sample_moment;
pub mod maximum;
pub mod materialize;
pub mod mean;
pub mod minimum;
pub mod quantile;
pub mod mechanisms;
pub mod resize;
//pub mod row_max;
//pub mod row_min;
pub mod sum;
pub mod transforms;
pub mod variance;

/// Evaluable component trait
///
/// Evaluable structs represent an abstract computation.
pub trait Evaluable {
    /// The concrete implementation of the abstract computation that the struct represents.
    ///
    /// # Arguments
    /// * `arguments` - a hashmap, where the `String` keys are the names of arguments, and the `Value` values are the data inputs
    ///
    /// # Returns
    /// The concrete value corresponding to the abstract computation that the struct represents
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value>;
}

impl Evaluable for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn evaluate(
        &self, arguments: &NodeArguments
    ) -> Result<Value> {

        macro_rules! evaluate {
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.evaluate(arguments)
                                .chain_err(|| format!("node specification: {:?}:", self))
                       }
                    )*
                }
            }
        }

        evaluate!(
            // INSERT COMPONENT LIST
            Bin, Cast, Clamp, Count, Covariance, Filter, Impute, Index, KthRawSampleMoment, Maximum,
            Materialize, Mean, Minimum, Quantile, LaplaceMechanism, GaussianMechanism,
            SimpleGeometricMechanism, Resize, Sum, Variance,

            Add, Subtract, Divide, Multiply, Power, Log, Modulo, LogicalAnd, LogicalOr, Negate,
            Equal, LessThan, GreaterThan, Negative
        );

        Err(format!("Component type not implemented: {:?}", self).into())

    }
}


/// Broadcast left and right to match each other, and map an operator over the pairs
///
/// # Arguments
/// * `left` - left vector to map over
/// * `right` - right vector to map over
/// * `operator` - function to apply to each pair
///
/// # Return
/// An array of mapped data
///
/// # Example
/// ```
/// use whitenoise_validator::errors::*;
/// use ndarray::{Array1, arr1, ArrayD};
/// use whitenoise_runtime::components::broadcast_map;
/// let left: ArrayD<f64> = arr1!([1., -2., 3., 5.]).into_dyn();
/// let right: ArrayD<f64> = arr1!([2.]).into_dyn();
/// let mapped: Result<ArrayD<f64>> = broadcast_map(&left, &right, &|l, r| l.max(r.clone()));
/// println!("{:?}", mapped); // [2., 2., 3., 5.]
/// ```
pub fn broadcast_map<T, U>(
    left: &ArrayD<T>,
    right: &ArrayD<T>,
    operator: &dyn Fn(&T, &T) -> U ) -> Result<ArrayD<U>> where T: std::clone::Clone, U: Default {

    match (left.ndim(), right.ndim()) {
        (l, r) if l == 0 && r == 0 =>
            Ok(Array::from_shape_vec(vec![],
                                     vec![operator(left.first().unwrap(), right.first().unwrap())]).unwrap()),
        (l, r) if l == 1 && r == 1 => {
            if left.len() != right.len() {
                return Err("the size of the left and right vectors do not match".into())
            }

            let mut default: ArrayD<U> = Array::default(left.shape());
            Zip::from(&mut default)
                .and(left)
                .and(right).apply(|acc, l, r| *acc = operator(&l, &r));
            Ok(default)
        },
        (l, r) if l == 1 && r == 0 => {
            let mut default: ArrayD<U> = Array::default(left.shape());
            Zip::from(&mut default).and(left).apply(|acc, l| *acc = operator(&l, &right.first().unwrap()));
            Ok(default)
        },
        (l, r) if l == 0 && r == 1 => {
            let mut default: ArrayD<U> = Array::default(left.shape());
            Zip::from(&mut default).and(right).apply(|acc, r| *acc = operator(&left.first().unwrap(), &r));
            Ok(default)
        },
        _ => Err("unsupported shapes for left and right vector in broadcast_map".into())
    }
}
