use ndarray::prelude::*;

use crate::utilities::transformations;

pub fn count< T:PartialEq >(data: &ArrayD<T>, group_by: &Option<ArrayD<T>>) -> ArrayD<f64> {
    // Accepts data and an optional array of values to be counted, and returns counts of each value.
    // If no values are provided, the function returns the overall count of the entire data.
    //
    // # Arguments
    // * `data` - Array of data for which you want counts. Data type can be any that supports the `PartialEq` trait.
    // * `group_by` (Optional) - Array of values for which you want counts. Data type should be the same as `data`.
    //
    // # Return
    // Array of counts
    //
    // # Example
    // ```
    // //////////////////
    // // numeric data //
    // //////////////////
    // let data: ArrayD<f64> = arr1(&[1., 1., 2., 3., 4., 4., 4.]).into_dyn();
    // let group_by: ArrayD<f64> = arr1(&[1., 2., 4.]).into_dyn();
    //
    // // count specific values
    // let count_1: ArrayD<f64> = count(&data, &Some(group_by));
    // println!("{:?}", count_1);
    // // get overall size of data
    // let count_2: ArrayD<f64> = count(&data, &None::<ArrayD<f64>>);
    // println!("{:?}", count_2);
    //
    // //////////////////
    // // boolean data //
    // //////////////////
    // let data_bool = arr1(&[true, true, false, false, true]).into_dyn();
    // let bool_vals = arr1(&[true, false]).into_dyn();
    // let bool_count: ArrayD<f64> = count(&data_bool, &Some(bool_vals));
    // println!("{:?}", bool_count);
    // ```

    if Option::is_some(&group_by) {
        let mut count_vec: Vec<f64> = Vec::with_capacity(group_by.as_ref().unwrap().len());
        for i in 0..group_by.as_ref().unwrap().len() {
            count_vec.push(data.iter().filter(|&elem| *elem == group_by.as_ref().unwrap()[i]).count() as f64);        }
        return arr1(&count_vec).into_dyn();
    } else {
        return arr1(&[data.len() as f64]).into_dyn();
    }
}

pub fn get_bin_names(edges: &ArrayD<f64>, inclusive_left: &bool) -> ArrayD<String> {
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
    /// let edges: ArrayD<f64> = arr1(&[0., 10., 20., 30., 40., 50., 60., 70., 80., 90., 100.]).into_dyn();
    /// let inclusive_left: bool = true;
    /// let bin_names: ArrayD<String> = get_bin_names(&edges, &inclusive_left);
    /// println!("{}", bin_names);
    /// ```

    let mut bin_name_vec: Vec<String> = Vec::with_capacity(edges.len()-1);
    let mut left_edge = String::new();
    let mut right_edge = String::new();
    let mut bin_name = String::new();
    for i in 0..(edges.len()-1) {
        left_edge = edges[i].to_string();
        right_edge = edges[i+1].to_string();
        if (i == 0 && inclusive_left == &false) {
            bin_name = format!("[{}, {}]", left_edge, right_edge);
        } else if (i == (edges.len()-2) && inclusive_left == &true) {
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

// pub fn histogram(data: &ArrayD<f64>, edges: &ArrayD<f64>, inclusive_left: &bool) -> HashMap::<String, f64> {
//     /// Accepts data, bin edges, and a bin definition rule and returns a HashMap of
//     /// bin names and counts
//     ///
//     /// # Arguments
//     /// * `data` - Array of numeric data to be binned
//     /// * `edges` - Array of bin edges, an array of n+1 edges will yield n bins
//     /// * `inclusive_left` - Boolean for whether or not bins (representing numeric intervals) are closed on the left.
//     ///                      If false, then bins are closed on the right.
//     ///                      The leftmost and rightmost bins are automatically closed on the left/right (respectively),
//     ///                      regardless of the value of `inclusive_left`.
//     ///
//     /// # Return
//     /// Hashmap of bin names and counts
//     ///
//     /// # Example
//     /// ```
//     /// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
//     /// let edges: ArrayD<f64> = arr1(&[0., 10., 20., 30., 40., 50., 60., 70., 80., 90., 100.]).into_dyn();
//     /// let inclusive_left: bool = true;
//     /// let hist = histogram(&data, &edges, &inclusive_left);
//     /// println!("{:?}", hist);
//     /// ```

//     // map data to bins
//     let binned_data: ArrayD<String> = transformations::bin(data, edges, inclusive_left);

//     // construct bin names
//     let mut bin_names: ArrayD<String> = get_bin_names(edges, inclusive_left);
//     let mut bin_names_copy: ArrayD<String> = bin_names.clone();

//     // get counts for each bin
//     let mut bin_counts: ArrayD<f64> = count(&binned_data, &Some(bin_names));

//     // construct hashmap of bin_name: count pairs
//     let mut hist_hashmap: HashMap::<String, f64> = HashMap::new();
//     for pair in bin_names_copy.iter().zip(bin_counts.iter_mut()) {
//         let (name, count) = pair;
//         hist_hashmap.insert(name.to_string(), *count);
//     }
//     return hist_hashmap;
// }

pub fn median(data: &ArrayD<f64>) -> ArrayD<f64> {
    /// Accepts data and returns median
    ///
    /// # Arguments
    /// * `data` - Array of data for which you would like the median
    ///
    /// # Return
    /// median of your data
    ///
    /// # Example
    /// ```
    /// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
    /// let median: f64 = median(&data);
    /// println!("{}", median);
    /// ```

    // create vector version of data, get length, and sort it
    let mut data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let n = data_vec.len();
    data_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // if even number of elements, return mean of the two middlemost elements
    // if odd number of elements, return middlemost element
    if n % 2 == 0 {
        return arr1(&[(data_vec[n/2 - 1] + data_vec[n/2]) / 2.0]).into_dyn();
    } else {
        return arr1(&[data_vec[n/2]]).into_dyn();
    }
}

pub fn sum(data: &ArrayD<f64>) -> ArrayD<f64> {
    /// Accepts data and returns sum
    ///
    /// # Arguments
    /// * `data` - Array of data for which you would like the median
    ///
    /// # Return
    /// sum of the data
    ///
    /// # Examples
    /// ```
    /// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
    /// let sum: f64 = sum(&data);
    /// println!("{}", sum);
    /// ```
    let data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let data_sum: f64 = data_vec.iter().map(|x| x).sum();
    return arr1(&[data_sum]).into_dyn();
}