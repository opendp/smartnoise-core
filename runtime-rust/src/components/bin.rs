use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array, Axis};
use crate::utilities::transformations::convert_to_matrix;
use yarrow_validator::proto;

impl Evaluable for proto::Bin {
    fn evaluate(&self: &proto::Bin, arguments: &NodeArguments) -> Result<Value> {
        let inclusive_left: ArrayD<bool> = get_argument(&arguments, "inclusive_left")?.get_arraynd()?.get_bool()?;

        let data = get_argument(&arguments, "data")?.get_arraynd()?;
        let edges = get_argument(&arguments, "edges")?.get_arraynd()?;

        match (data, edges) {
            (ArrayND::F64(data), ArrayND::F64(edges)) => Ok(Value::ArrayND(ArrayND::Str(utilities::transformations::bin(&data, &edges, &inclusive_left)?))),
            (ArrayND::I64(data), ArrayND::I64(edges)) => Ok(Value::ArrayND(ArrayND::Str(utilities::transformations::bin(&data, &edges, &inclusive_left)?))),
            _ => return Err("data and edges must both be f64 or i64".into())
        }
    }
}


pub fn bin<T>(data: &ArrayD<T>, edges: &ArrayD<T>, inclusive_left: &ArrayD<bool>)
              -> Result<ArrayD<String>> where T: Clone, T: PartialOrd, T: std::fmt::Display {
    /// Accepts vector of data and assigns each element to a bin
    /// NOTE: bin transformation has C-stability of 1
    ///
    /// # Arguments
    /// * `data` - Array of numeric data to be binned
    /// * `edges` - Array of bin edges, an array of n+1 edges will yield n bins
    /// * `inclusive_left` - Boolean for whether or not bins (representing numeric intervals) are closed on the left.
    ///                      If false, then bins are closed on the right.
    ///                      The leftmost and rightmost bins are automatically closed on the left/right (respectively),
    ///                      regardless of the value of `inclusive_left`.
    ///
    /// # Return
    /// ArrayD of bin assignments
    ///
    /// # Example
    /// ```
    /// // set up data
    /// use ndarray::{ArrayD, arr1, Array1};
    /// use yarrow_runtime::utilities::transformations::bin;
    /// let data: ArrayD<f64> = arr1(&[1., 2., 3., 4., 5., 12., 19., 24., 90., 98.]).into_dyn();
    /// let edges: ArrayD<f64> = arr1(&[0., 10., 20., 30., 40., 50., 60., 70., 80., 90., 100.]).into_dyn();
    /// let inclusive_left: ArrayD<bool> = arr1(&[false]).into_dyn();
    ///
    /// // bin data
    /// let binned_data: ArrayD<String> = bin(&data, &edges, &inclusive_left)?;
    /// println!("{:?}", binned_data);
    /// ```


    // initialize new data -- this is what we ultimately return from the function
    let original_dim: u8 = data.ndim() as u8;
    let new_data: ArrayD<T> = convert_to_matrix(data);
    let mut new_bin_array: ArrayD<String> = Array::default(new_data.shape());

    let n_cols: i64 = data.len_of(Axis(0)) as i64;

    for k in 0..n_cols {
        // create vector versions of data and edges
        let data_vec: Vec<T> = data.slice(s![k as usize, ..]).clone().into_dimensionality::<Ix1>().unwrap().to_vec();
        let mut sorted_edges: Vec<T> = edges.slice(s![k as usize, ..]).clone().into_dimensionality::<Ix1>().unwrap().to_vec();

        //  ensure edges are sorted in ascending order
        sorted_edges.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // initialize output vector
        let mut bin_vec: Vec<String> = Vec::with_capacity(data_vec.len());

        // for each data element, look for correct bin and append name to bin_vec
        for i in 0..data_vec.len() {
            // append empty string if data are outside of bin ranges
            if data_vec[i] < sorted_edges[0] || data_vec[i] > sorted_edges[sorted_edges.len()-1] {
                bin_vec.push("".to_string());
            } else {
                // for each bin
                for j in 0..(sorted_edges.len()-1) {
                    if  // element is less than the right bin edge
                    data_vec[i] < sorted_edges[j+1] ||
                        // element is equal to the right bin edge and we are building our histogram to be 'right-edge inclusive'
                        (data_vec[i] == sorted_edges[j+1] && inclusive_left[k as usize] == false) ||
                        // element is equal to the right bin edge and we are checking our rightmost bin
                        (data_vec[i] == sorted_edges[j+1] && j == (sorted_edges.len()-2)) {
                        if j == 0 && inclusive_left[k as usize] == false {
                            // leftmost bin must be left inclusive even if overall strategy is to be right inclusive
                            bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
                        } else if j == (sorted_edges.len()-2) && inclusive_left[k as usize] == true {
                            // rightmost bin must be right inclusive even if overall strategy is to be left inclusive
                            bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
                        } else if inclusive_left[k as usize] == true {
                            bin_vec.push(format!("[{}, {})", sorted_edges[j], sorted_edges[j+1]));
                        } else {
                            bin_vec.push(format!("({}, {}]", sorted_edges[j], sorted_edges[j+1]));
                        }
                        break;
                    }
                }
            }
        }
        // convert bin vector to Array and return
        let bin_array: ArrayD<String> = arr1(&bin_vec).into_dyn();
        new_bin_array.slice_mut(s![k as usize, ..]).assign(&bin_array);
    }
    return Ok(convert_from_matrix(&new_bin_array, &original_dim));
}



//pub struct Bin<T> {
//    edges: Vec<T>,
//    inclusive_left: bool,
//    null: T,
//    side: BinSide
//}
//pub enum BinSide {
//    Left, Right, Center
//}
//
//fn bin<T: std::cmp::PartialOrd>(
//    v: &mut T, args: &Bin<T>
//) -> Result<()> {
//    // assuming well-formed-ness of edges
//
//    // checks for nullity
//    if args.edges.len() == 0 || v < args.edges[0] || v > args.edges[args.edges.len() - 1] {
//        *v = args.null.clone();
//        return Ok(())
//    }
//
//    // assign to edge
//    for idx in args.edges.len() - 1 {
//        if match args.inclusive_left {
//            true => args.edges[idx] <= v && v < args.edges[idx + 1],
//            false => args.edges[idx] < v && v <= args.edges[idx + 1]
//        } {
//            *v = match args.side {
//                BinSide::Left => args.edges[idx],
//                BinSide::Right => args.edges[idx + 1],
//                BinSide::Center => (args.edges[idx] / 2) + (args.edges[idx + 1] / 2)
//            };
//            return Ok(())
//        }
//    }
//
//    return Err("arguments to binning are not well-formed".into())
//}


/// Accepts bin edges and bin definition rule and returns an array of bin names
///
/// # Arguments
/// * `edges` - Array of bin edges, an array of n+1 edges will yield n bins
/// * `inclusive_left` - Boolean for whether or not bins (representing numeric intervals) are closed on the left.
///                      If false, then bins are closed on the right.
///                      The leftmost and rightmost bins are automatically closed on the left/right (respectively),
///                      regardless of the value of `inclusive_left`.
///
/// Return
/// Array of bin names.
///
/// Example
/// ```
/// use yarrow_runtime::utilities::aggregations::get_bin_names;
/// use ndarray::prelude::*;
/// let edges: ArrayD<f64> = arr1(&[0., 10., 20.]).into_dyn();
///
/// let inclusive_left: bool = true;
/// let bin_names: ArrayD<String> = get_bin_names(&edges, &inclusive_left);
/// assert_eq!(arr1(&["[0, 10)", "[10, 20]"]).into_dyn(), bin_names);
///
/// let inclusive_left: bool = false;
/// let bin_names: ArrayD<String> = get_bin_names(&edges, &inclusive_left);
/// assert_eq!(arr1(&["[0, 10]", "(10, 20]"]).into_dyn(), bin_names);
/// ```
pub fn get_bin_names(edges: &ArrayD<f64>, inclusive_left: &bool) -> ArrayD<String> {

    let mut bin_name_vec: Vec<String> = Vec::with_capacity(edges.len()-1);
    let mut left_edge = String::new();
    let mut right_edge = String::new();
    let mut bin_name = String::new();
    for i in 0..(edges.len()-1) {
        left_edge = edges[i].to_string();
        right_edge = edges[i+1].to_string();
        if i == 0 && inclusive_left == &false {
            bin_name = format!("[{}, {}]", left_edge, right_edge);
        } else if i == (edges.len()-2) && inclusive_left == &true {
            bin_name = format!("[{}, {}]", left_edge, right_edge);
        } else if inclusive_left == &true {
            bin_name = format!("[{}, {})", left_edge, right_edge);
        } else {
            bin_name = format!("({}, {}]", left_edge, right_edge);
        }
        bin_name_vec.push(bin_name);
    }
    return arr1(&bin_name_vec).into_dyn();
}
