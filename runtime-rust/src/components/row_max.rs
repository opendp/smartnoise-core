use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use crate::components::Evaluable;
use yarrow_validator::base::{Value};
use std::convert::TryFrom;
use ndarray::ArrayD;
use yarrow_validator::proto;

impl Evaluable for proto::RowMax {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::broadcast_map(
                        &x, &y, &|l: &f64, r: &f64| l.max(*r))?))),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::I64(utilities::transformations::broadcast_map(
                        &x, &y, &|l: &i64, r: &i64| *std::cmp::max(l, r))?))),
                _ => Err("Max: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Max: Both arguments must be arrays.".into())
        }
    }
}



pub fn broadcast_map<T>(
    left: &ArrayD<T>,
    right: &ArrayD<T>,
    operator: &dyn Fn(&T, &T) -> T ) -> Result<ArrayD<T>> where T: std::clone::Clone, T: Default, T: Copy {
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
    /// use yarrow_validator::errors::*;
    /// use ndarray::{Array1, arr1, ArrayD};
    /// use yarrow_runtime::utilities::transformations::broadcast_map;
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

            let mut zeros: ArrayD<T> = Array::default(left.shape());
            Zip::from(&mut zeros)
                .and(left)
                .and(right).apply(|acc, &l, &r| *acc = operator(&l, &r));
            Ok(zeros)
        },
        (l, r) if l == 1 && r == 0 => {
            let mut zeros: ArrayD<T> = Array::default(left.shape());
            Zip::from(&mut zeros).and(left).apply(|acc, &l| *acc = operator(&l, &right.first().unwrap()));
            Ok(zeros)
        },
        (l, r) if l == 0 && r == 1 => {
            let mut zeros: ArrayD<T> = Array::default(left.shape());
            Zip::from(&mut zeros).and(right).apply(|acc, &r| *acc = operator(&left.first().unwrap(), &r));
            Ok(zeros)
        },
        _ => Err("unsupported shapes for left and right vector in broadcast_map".into())
    }
}
