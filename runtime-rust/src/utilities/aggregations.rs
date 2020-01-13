use ndarray::prelude::*;

pub fn count< T:PartialEq >(data: ArrayD<T>, group_by: Option<ArrayD<T>>) -> ArrayD<f64> {
    /// Accepts data and an optional array of values to be counted, and returns counts of each value.
    /// If no values are provided, the function returns the overall count of the entire data.
    ///
    /// # Arguments
    /// * `data` - Array of data for which you want counts. Data type can be any that supports the `PartialEq` trait.
    /// * `group_by` (Optional) - Array of values for which you want counts. Data type should be the same as `data`.
    ///
    /// # Return
    /// Array of counts
    ///
    /// # Example
    /// ```
    /// //////////////////
    /// // numeric data //
    /// //////////////////
    /// let data: ArrayD<f64> = arr1(&[1., 1., 2., 3., 4., 4., 4.]).into_dyn();
    /// let group_by: ArrayD<f64> = arr1(&[1., 2., 4.]).into_dyn();
    ///
    /// // count specific values
    /// let count_1: ArrayD<f64> = count(&data, &Some(group_by));
    /// println!("{:?}", count_1);
    /// // get overall size of data
    /// let count_2: ArrayD<f64> = count(&data, &None::<ArrayD<f64>>);
    /// println!("{:?}", count_2);
    ///
    /// //////////////////
    /// // boolean data //
    /// //////////////////
    /// let data_bool = arr1(&[true, true, false, false, true]).into_dyn();
    /// let bool_vals = arr1(&[true, false]).into_dyn();
    /// let bool_count: ArrayD<f64> = count(&data_bool, &Some(bool_vals));
    /// println!("{:?}", bool_count);
    /// ```

    if Option::is_some(&group_by) {
        let mut count_vec: Vec<f64> = Vec::with_capacity(group_by.as_ref().unwrap().len());
        for i in 0..group_by.as_ref().unwrap().len() {
            count_vec.push(data.iter().filter(|&elem| *elem == group_by.as_ref().unwrap()[i]).count() as f64);        }
        return arr1(&count_vec).into_dyn();
    } else {
        return arr1(&[data.len() as f64]).into_dyn();
    }
}