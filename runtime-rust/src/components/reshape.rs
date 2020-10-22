use smartnoise_validator::errors::*;

use crate::NodeArguments;
use smartnoise_validator::base::{Value, Array, ReleaseNode, IndexKey};
use smartnoise_validator::utilities::take_argument;
use crate::components::Evaluable;
use ndarray::ArrayD;
use smartnoise_validator::{proto, Integer};
use indexmap::map::IndexMap;


impl Evaluable for proto::Reshape {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let layout = match self.layout.to_lowercase().as_str() {
            "row" => Layout::Row,
            "column" => Layout::Column,
            _ => return Err("layout: unrecognized format. Must be either row or column".into())
        };

        match take_argument(&mut arguments, "data")?.array()? {
            Array::Bool(data) => {
                let mut reshaped = reshape(&data, self.symmetric, &layout, &self.shape)?;
                match reshaped.len() {
                    0 => Err("at least one record is required to reshape".into()),
                    1 => Ok(reshaped.remove(0).into()),
                    _ => Ok(Value::Partitions(reshaped.into_iter().enumerate()
                        .map(|(idx, data)|
                            (IndexKey::from(idx as Integer), data.into()))
                        .collect::<IndexMap<IndexKey, Value>>()))
                }
            }
            Array::Int(data) => {
                let mut reshaped = reshape(&data, self.symmetric, &layout, &self.shape)?;
                match reshaped.len() {
                    0 => Err("at least one record is required to reshape".into()),
                    1 => Ok(reshaped.remove(0).into()),
                    _ => Ok(Value::Partitions(reshaped.into_iter().enumerate()
                        .map(|(idx, data)|
                            (IndexKey::from(idx as Integer), data.into()))
                        .collect::<IndexMap<IndexKey, Value>>()))
                }
            }
            Array::Float(data) => {
                let mut reshaped = reshape(&data, self.symmetric, &layout, &self.shape)?;
                match reshaped.len() {
                    0 => Err("at least one record is required to reshape".into()),
                    1 => Ok(reshaped.remove(0).into()),
                    _ => Ok(Value::Partitions(reshaped.into_iter().enumerate()
                        .map(|(idx, data)|
                            (IndexKey::from(idx as Integer), data.into()))
                        .collect::<IndexMap<IndexKey, Value>>()))
                }
            }
            Array::Str(data) => {
                let mut reshaped = reshape(&data, self.symmetric, &layout, &self.shape)?;
                match reshaped.len() {
                    0 => Err("at least one record is required to reshape".into()),
                    1 => Ok(reshaped.remove(0).into()),
                    _ => Ok(Value::Partitions(reshaped.into_iter().enumerate()
                        .map(|(idx, data)|
                            (IndexKey::from(idx as Integer), data.into()))
                        .collect::<IndexMap<IndexKey, Value>>()))
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

/// Reshape an upper triangular matrix or dense matrix represented in one row, to a square matrix.
/// One matrix is returned per row.
///
/// # Arguments
/// * `data` - Data for which you want a count.
/// * `symmetric` - indicates if elements of data come from an upper triangular matrix, not a dense matrix
/// * `layout` - indicates data resides in row major or column major order
/// * `shape` - shape of output matrix
///
/// # Return
/// A vector of reshaped matrices, one matrix per row.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use smartnoise_runtime::components::reshape::{reshape, Layout};
/// let data = arr2(&[ [false, false, true] ]).into_dyn();
/// let n = reshape(&data, true, &Layout::Row, &vec![2, 2]).unwrap();
/// assert_eq!(n[0], arr2(&[ [false, false], [false, true] ]).into_dyn());
/// ```
pub fn reshape<T: Clone>(data: &ArrayD<T>, symmetric: bool, layout: &Layout, shape: &[u32]) -> Result<Vec<ArrayD<T>>> {
    data.genrows().into_iter()
        .map(|row| {
            if symmetric {
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
                            }).collect::<Vec<T>>()).flatten().collect::<Vec<T>>(),
                    Layout::Column => return Err("not implemented".into())
                };

                Ok(ndarray::Array::from_shape_vec((num_rows as usize, num_rows as usize), full)?.into_dyn())
            } else {
                if &Layout::Column == layout {
                    return Err("reshaping for dense columnar memory layouts is not supported".into());
                }
                let shape = shape.iter().map(|v| *v as usize).collect::<Vec<usize>>();
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
        let n = reshape(&data, true, &Layout::Row, &vec![2, 2]).unwrap();
        assert!(n[0] == arr2(&[[false, false], [false, true]]).into_dyn());
    }

    #[test]
    fn test_reshape_symmetric_4x4() {
        let data = arr2(&[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]).into_dyn();
        let n = reshape(&data, true, &Layout::Row, &vec![4, 4]).unwrap();
        assert!(n[0] == arr2(&[[0, 1, 2, 3], [1, 4, 5, 6], [2, 5, 7, 8], [3, 6, 8, 9]]).into_dyn());
    }
}