use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, ReleaseNode};
use whitenoise_validator::utilities::get_argument;
use crate::components::Evaluable;
use ndarray::ArrayD;
use whitenoise_validator::proto;
use std::collections::BTreeMap;
use indexmap::map::IndexMap;


impl Evaluable for proto::Reshape {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let layout = match self.layout.to_lowercase().as_str() {
            "row" => Layout::Row,
            "column" => Layout::Column,
            _ => return Err("layout: unrecognized format. Must be either row or column".into())
        };

        match get_argument(&arguments, "data")?.array()? {
            Array::Bool(data) => {
                let mut reshaped = reshape(&data, &self.symmetric, &layout, &self.shape)?;
                match reshaped.len().clone() {
                    0 => Err("at least one record is required to reshape".into()),
                    1 => Ok(reshaped.remove(0).into()),
                    _ => Ok(reshaped.into_iter().enumerate()
                        .map(|(idx, data)| (idx as i64, data.into()))
                        .collect::<IndexMap<i64, Value>>().into())
                }
            }
            Array::I64(data) => {
                let mut reshaped = reshape(&data, &self.symmetric, &layout, &self.shape)?;
                match reshaped.len().clone() {
                    0 => Err("at least one record is required to reshape".into()),
                    1 => Ok(reshaped.remove(0).into()),
                    _ => Ok(reshaped.into_iter().enumerate()
                        .map(|(idx, data)| (idx as i64, data.into()))
                        .collect::<IndexMap<i64, Value>>().into())
                }
            }
            Array::F64(data) => {
                let mut reshaped = reshape(&data, &self.symmetric, &layout, &self.shape)?;
                match reshaped.len().clone() {
                    0 => Err("at least one record is required to reshape".into()),
                    1 => Ok(reshaped.remove(0).into()),
                    _ => Ok(reshaped.into_iter().enumerate()
                        .map(|(idx, data)| (idx as i64, data.into()))
                        .collect::<IndexMap<i64, Value>>().into())
                }
            }
            Array::Str(data) => {
                let mut reshaped = reshape(&data, &self.symmetric, &layout, &self.shape)?;
                match reshaped.len().clone() {
                    0 => Err("at least one record is required to reshape".into()),
                    1 => Ok(reshaped.remove(0).into()),
                    _ => Ok(reshaped.into_iter().enumerate()
                        .map(|(idx, data)| (idx as i64, data.into()))
                        .collect::<IndexMap<i64, Value>>().into())
                }
            }
        }.map(ReleaseNode::new)
    }
}

#[derive(PartialEq)]
pub enum Layout {
    Row,
    Column,
}

/// Gets number of rows of data.
///
/// # Arguments
/// * `data` - Data for which you want a count.
/// * `symmetric` - indicates if elements of data come from an upper triangular matrix, not a dense matrix
/// * `layout` - indicates data resides in row major or column major order
/// * `shape` - shape of output matrix
///
/// # Return
/// Reshaped matrices, one matrix per row.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use whitenoise_runtime::components::reshape::{reshape, Layout};
/// let data = arr2(&[ [false, false, true] ]).into_dyn();
/// let n = reshape(&data, &true, &Layout::Row, &vec![2, 2]).unwrap();
/// assert!(n[0] == arr2(&[ [false, false], [false, true] ]).into_dyn());
/// ```
pub fn reshape<T: Clone>(data: &ArrayD<T>, symmetric: &bool, layout: &Layout, shape: &Vec<u32>) -> Result<Vec<ArrayD<T>>> {
    data.genrows().into_iter()
        .map(|row| {
            if *symmetric {
                let row = row.to_vec();
                let num_rows = match shape.len() {
                    1 => shape[0],
                    2 => {
                        if shape[0] != shape[1] {
                            return Err("the width and height must match to form a triangular matrix".into());
                        }
                        shape[0]
                    }
                    _ => return Err("shape must be 0 or 1-dimensional to form a triangular matrix".into())
                };

                if row.len() != (num_rows * (num_rows + 1) / 2) as usize {
                    return Err("invalid number of elements in row for reshaping into symmetric matrix".into());
                }

                let full = match layout {
                    Layout::Row =>
                        (0..num_rows).map(|i|
                            (0..num_rows).map(|j| if i <= j {
                                // upper triangular (full matrix index, less the arithmetic progression)
                                row[(i * num_rows + j - (i + 1) * i / 2) as usize].clone()
                            } else {
                                // lower triangular (symmetric with upper triangle)
                                row[(j * num_rows + i - (j + 1) * j / 2) as usize].clone()
                            }).collect()).flat_map(|x: Vec<T>| x).collect::<Vec<T>>(),
                    Layout::Column => return Err("not implemented".into())
                };

                Ok(ndarray::Array::from_shape_vec((num_rows as usize, num_rows as usize), full)?.into_dyn())
            } else {
                if &Layout::Column == layout {
                    return Err("reshaping for dense columnar memory layouts is not supported".into());
                }
                let shape = shape.iter().map(|v| v.clone() as usize).collect::<Vec<usize>>();
                match ndarray::ArrayD::from_shape_vec(shape, row.to_vec()) {
                    Ok(arr) => Ok(arr),
                    Err(_) => Err("reshape has incorrect size".into())
                }
            }
        }).collect::<Result<Vec<ArrayD<T>>>>()
}

#[cfg(test)]
mod test_reshape {
    use ndarray::arr2;
    use crate::components::reshape::{reshape, Layout};

    #[test]
    fn test_reshape_symmetric_2x2() {
        let data = arr2(&[[false, false, true]]).into_dyn();
        let n = reshape(&data, &true, &Layout::Row, &vec![2, 2]).unwrap();
        assert!(n[0] == arr2(&[[false, false], [false, true]]).into_dyn());
    }

    #[test]
    fn test_reshape_symmetric_4x4() {
        let data = arr2(&[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]).into_dyn();
        let n = reshape(&data, &true, &Layout::Row, &vec![4, 4]).unwrap();
        assert!(n[0] == arr2(&[[0, 1, 2, 3], [1, 4, 5, 6], [2, 5, 7, 8], [3, 6, 8, 9]]).into_dyn());
    }
}