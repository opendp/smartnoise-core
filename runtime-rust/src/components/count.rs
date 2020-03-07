use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, Vector2DJagged, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};
use yarrow_validator::proto;
use crate::utilities::utilities::get_num_columns;


impl Evaluable for proto::Count {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?.get_arraynd()?;

        match arguments.get("categories") {
            Some(categories) => match categories {
//                Value::Vector2DJagged(categories) => match (data, categories) {
//                    (ArrayND::Bool(data), Vector2DJagged::Bool(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(count_grouped(&data, &categories)?))),
//                    (ArrayND::F64(data), Vector2DJagged::F64(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(count_grouped(&data, &categories)?))),
//                    (ArrayND::I64(data), Vector2DJagged::I64(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(count_grouped(&data, &categories)?))),
//                    (ArrayND::Str(data), Vector2DJagged::Str(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(count_grouped(&data, &categories)?))),
//                    _ => return Err("data and by must be ArrayND and categories must be Vector2dJagged".into())
//                }
                _ => Err("by must be ArrayND and categories must be Vector2DJagged".into())
            }
            None => match get_argument(&arguments, "data")?.get_arraynd()? {
                ArrayND::Bool(data) =>
                    Ok(Value::ArrayND(ArrayND::I64(count(&data)?))),
                ArrayND::F64(data) =>
                    Ok(Value::ArrayND(ArrayND::I64(count(&data)?))),
                ArrayND::I64(data) =>
                    Ok(Value::ArrayND(ArrayND::I64(count(&data)?))),
                ArrayND::Str(data) =>
                    Ok(Value::ArrayND(ArrayND::I64(count(&data)?)))
            }
        }
    }
}


pub fn count<T: Clone>(data: &ArrayD<T>) -> Result<ArrayD<i64>> {

    // iterate over the generalized columns. Of course, all columns will share the same length
    let counts = data.gencolumns().into_iter()
        .map(|column| column.len() as i64).collect::<Vec<i64>>();

    let array = match data.ndim() {
        1 => Array::from_shape_vec(vec![], counts),
        2 => Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], counts),
        _ => return Err("invalid data shape for Count".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Count result into an array".into())
    }
}

///// Gets count of data elements for each category
/////
///// Example
///// ```
///// use ndarray::{ArrayD, arr2};
///// use yarrow_runtime::utilities::transformations::count;
///// use yarrow_validator::errors::*;
///// let data: ArrayD<i64> = arr2(&[ [1,1,1,1,1,2,2,2,2,3,3,3,4,4,5],
/////                                 [1,2,2,3,3,3,4,4,4,4,5,5,5,5,5] ]).into_dyn();
///// let categories: Vec<Option<Vec<i64>>> = vec![Some(vec![1,3,5]), Some(vec![2,4])];
///// let t: Result<Vec<Option<Vec<i64>>>> = count(&data, &categories);
///// println!("{:?}", t);
///// ```
//pub fn count_by<T>(data: &ArrayD<T>, categories: &Vec<Option<Vec<T>>>) -> Result<Vec<Option<Vec<i64>>>> where T: Clone, T: PartialEq {
//
//    let data_2d: ArrayD<T> = convert_to_matrix(data);
//    let mut counts: Vec<Option<Vec<i64>>> = Vec::with_capacity(categories.len());
//
//    let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;
//
//    for i in 0..n_cols {
//        let data_vec: Vec<T> = data_2d.slice(s![i as usize, ..]).clone().into_dyn().clone().
//            into_dimensionality::<Ix1>().unwrap().to_vec();
//        let category_vec: Vec<T> = categories[i as usize].clone().unwrap();
//        let mut counts_vec: Vec<i64> = vec![0; category_vec.len()];
//
//        for j in 0..data_vec.len() {
//            for k in 0..category_vec.len() {
//                if data_vec[j as usize] == category_vec[k as usize] {
//                    counts_vec[k] += 1;
//                }
//            }
//        }
//
//        counts.push(Some(counts_vec));
//    }
//
//    return Ok(counts);
//}


// pub fn count< T:PartialEq >(data: &ArrayD<T>, group_by: &Option<ArrayD<T>>) -> ArrayD<f64> {
//     // Accepts data and an optional array of values to be counted, and returns counts of each value.
//     // If no values are provided, the function returns the overall count of the entire data.
//     //
//     // # Arguments
//     // * `data` - Array of data for which you want counts. Data type can be any that supports the `PartialEq` trait.
//     // * `group_by` (Optional) - Array of values for which you want counts. Data type should be the same as `data`.
//     //
//     // # Return
//     // Array of counts
//     //
//     // # Example
//     // ```
//     // //////////////////
//     // // numeric data //
//     // //////////////////
//     // let data: ArrayD<f64> = arr1(&[1., 1., 2., 3., 4., 4., 4.]).into_dyn();
//     // let group_by: ArrayD<f64> = arr1(&[1., 2., 4.]).into_dyn();
//     //
//     // // count specific values
//     // let count_1: ArrayD<f64> = count(&data, &Some(group_by));
//     // println!("{:?}", count_1);
//     // // get overall size of data
//     // let count_2: ArrayD<f64> = count(&data, &None::<ArrayD<f64>>);
//     // println!("{:?}", count_2);
//     //
//     // //////////////////
//     // // boolean data //
//     // //////////////////
//     // let data_bool = arr1(&[true, true, false, false, true]).into_dyn();
//     // let bool_vals = arr1(&[true, false]).into_dyn();
//     // let bool_count: ArrayD<f64> = count(&data_bool, &Some(bool_vals));
//     // println!("{:?}", bool_count);
//     // ```

//     if Option::is_some(&group_by) {
//         let mut count_vec: Vec<f64> = Vec::with_capacity(group_by.as_ref().unwrap().len());
//         for i in 0..group_by.as_ref().unwrap().len() {
//             count_vec.push(data.iter().filter(|&elem| *elem == group_by.as_ref().unwrap()[i]).count() as f64);        }
//         return arr1(&count_vec).into_dyn();
//     } else {
//         return arr1(&[data.len() as f64]).into_dyn();
//     }
// }

