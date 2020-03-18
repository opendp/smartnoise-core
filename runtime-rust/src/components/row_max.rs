use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use crate::components::Evaluable;
use whitenoise_validator::base::{Value, get_argument, ArrayND};
use std::convert::TryFrom;
use ndarray::{ArrayD, Array, Zip};
use whitenoise_validator::proto;

impl Evaluable for proto::RowMax {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(broadcast_map(
                        &x, &y, &|l: &f64, r: &f64| l.max(*r))?.into()),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(broadcast_map(
                        &x, &y, &|l: &i64, r: &i64| *std::cmp::max(l, r))?.into()),
                _ => Err("Max: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Max: Both arguments must be arrays.".into())
        }
    }
}



pub fn broadcast_map<T, U>(
    left: &ArrayD<T>,
    right: &ArrayD<T>,
    operator: &dyn Fn(&T, &T) -> U ) -> Result<ArrayD<U>> where T: std::clone::Clone, U: Default {
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
    /// use whitenoise_runtime::utilities::transformations::broadcast_map;
    /// let left: ArrayD<f64> = arr1!([1., -2., 3., 5.]).into_dyn();
    /// let right: ArrayD<f64> = arr1!([2.]).into_dyn();
    /// let mapped: Result<ArrayD<f64>> = broadcast_map(&left, &right, &|l, r| l.max(r.clone()));
    /// println!("{:?}", mapped); // [2., 2., 3., 5.]
    /// ```

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
