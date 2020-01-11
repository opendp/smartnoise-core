use std::string::String;
use std::vec::Vec;
use ndarray::prelude::*;

pub fn bin(data: ArrayD<f64>, edges: ArrayD<f64>, inclusive_left: bool) -> ArrayD<String> {
    // create vector versions of data and edges
    let data_vec: Vec<f64> = data.into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut sorted_edges: Vec<f64> = edges.into_dimensionality::<Ix1>().unwrap().to_vec();

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
                    (data_vec[i] == sorted_edges[j+1] && inclusive_left == false) ||
                    // element is equal to the right bin edge and we are checking our rightmost bin
                    (data_vec[i] == sorted_edges[j+1] && j == (sorted_edges.len()-2)) {
                        if j == 0 && inclusive_left == false {
                            // leftmost bin must be left inclusive even if overall strategy is to be right inclusive
                            bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
                        } else if j == (sorted_edges.len()-2) && inclusive_left == true {
                            // rightmost bin must be right inclusive even if overall strategy is to be left inclusive
                            bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
                        } else if inclusive_left == true {
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
    let bin_array: Array1<String> = Array1::from(bin_vec);
    return bin_array.into_dyn();
}