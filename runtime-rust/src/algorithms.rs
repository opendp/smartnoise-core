use ndarray::prelude::*;
use ndarray_stats::SummaryStatisticsExt;
use ndarray::Zip;

use crate::utilities;

pub fn dp_mean_laplace(
    epsilon: f64, num_records: f64,
    data: ArrayD<f64>,
    minimum: f64, maximum: f64) -> f64 {

    let sensitivity: f64 = (maximum - minimum) / num_records;

    let mean: f64 = data
        .mapv(|v| num::clamp(v, minimum, maximum))
        .mean().unwrap();

    let noise: f64 = utilities::sample_laplace(0., sensitivity / epsilon);

    mean + noise
}

pub fn dp_variance_laplace(
    epsilon: f64, num_records: f64,
    data: ArrayD<f64>,
    minimum: f64, maximum: f64) -> f64 {

    let sensitivity: f64 = (num_records - 1.0) / num_records.powi(2) * (maximum - minimum).powi(2);

    let variance: f64 = data
        .mapv(|v| num::clamp(v, minimum, maximum))
        .central_moment(2).unwrap();

    let noise: f64 = utilities::sample_laplace(0., sensitivity / epsilon);

    variance + noise
}

pub fn dp_moment_raw_laplace(
    epsilon: f64, num_records: f64,
    data: ArrayD<f64>,
    minimum: f64, maximum: f64,
    order: u32) -> f64 {

    let sensitivity: f64 = (maximum - minimum).powi(order as i32) / num_records;

    let moment: f64 = data
        .mapv(|v| num::clamp(v, minimum, maximum).powi(order as i32))
        .mean().unwrap();

    let noise: f64 = utilities::sample_laplace(0., sensitivity / epsilon);

    moment + noise
}

pub fn dp_covariance(
    epsilon: f64, num_records: f64,
    data_x: ArrayD<f64>, data_y: ArrayD<f64>,
    minimum_x: f64, minimum_y: f64,
    maximum_x: f64, maximum_y: f64) -> f64 {

    let sensitivity: f64 = 2. * (num_records - 1.)
        / num_records * (maximum_x - minimum_x) * (maximum_y - minimum_y);

    let data_x = data_x.mapv(|v| num::clamp(v, minimum_x, maximum_x)).into_dimensionality::<Ix1>().unwrap();
    let data_y = data_y.mapv(|v| num::clamp(v, minimum_y, maximum_y)).into_dimensionality::<Ix1>().unwrap();

    let mean_x = data_x.mean().unwrap();
    let mean_y = data_y.mean().unwrap();

    let mut products = Array1::<f64>::zeros((data_x.len()));
    Zip::from(&mut products).and(&data_x).and(&data_y)
        .apply(|total, &x, &y| *total += (x - mean_x) * (y - mean_y));

    let covariance = products.mean().unwrap();
    let noise: f64 = utilities::sample_laplace(0., sensitivity / epsilon);

    covariance + noise
}
