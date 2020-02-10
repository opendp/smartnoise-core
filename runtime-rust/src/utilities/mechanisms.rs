use ndarray::prelude::*;

use crate::utilities::noise;

pub fn laplace_mechanism(epsilon: &ArrayD<f64>, sensitivity: &ArrayD<f64>) -> ArrayD<f64> {
    let mut epsilon_vec: Vec<f64> = epsilon.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut sensitivity_vec: Vec<f64> = sensitivity.clone().into_dimensionality::<Ix1>().unwrap().to_vec();

    assert!(epsilon_vec.len() == sensitivity_vec.len());

    let mut noise_vec: Vec<f64> = Vec::with_capacity(epsilon_vec.len());
    let mut scale: f64;
    for i in 0..epsilon_vec.len() {
        scale = sensitivity_vec[i] / epsilon_vec[i];
        noise_vec.push( noise::sample_laplace(0., scale) )
    }
    return arr1(&noise_vec).into_dyn();
}

pub fn gaussian_mechanism(epsilon: &ArrayD<f64>, delta: &ArrayD<f64>, sensitivity: &ArrayD<f64>) -> ArrayD<f64> {
    let mut epsilon_vec: Vec<f64> = epsilon.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut delta_vec: Vec<f64> = delta.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut sensitivity_vec: Vec<f64> = sensitivity.clone().into_dimensionality::<Ix1>().unwrap().to_vec();

    assert!(epsilon_vec.len() == delta_vec.len() &&
            epsilon_vec.len() == sensitivity_vec.len());

    let mut noise_vec: Vec<f64> = Vec::with_capacity(epsilon_vec.len());
    let mut scale: f64;

    for i in 0..epsilon_vec.len() {
        scale = sensitivity_vec[i] * (2. * (1.25 / delta_vec[i]).ln()).sqrt() / epsilon_vec[i];
        noise_vec.push( noise::sample_gaussian(0., scale) )
    }
    return arr1(&noise_vec).into_dyn();
}

pub fn simple_geometric_mechanism(epsilon: &ArrayD<f64>, sensitivity: &ArrayD<f64>, count_min: &ArrayD<i64>, count_max: &ArrayD<i64>, enforce_constant_time: &ArrayD<bool>) -> ArrayD<i64> {
    let mut epsilon_vec: Vec<f64> = epsilon.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut sensitivity_vec: Vec<f64> = sensitivity.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut count_min_vec: Vec<i64> = count_min.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut count_max_vec: Vec<i64> = count_max.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut enforce_constant_time_vec: Vec<bool> = enforce_constant_time.clone().into_dimensionality::<Ix1>().unwrap().to_vec();

    assert!(epsilon_vec.len() == sensitivity_vec.len() &&
            epsilon_vec.len() == count_min_vec.len() &&
            epsilon_vec.len() == count_max_vec.len() &&
            epsilon_vec.len() == enforce_constant_time_vec.len());

    let mut noise_vec: Vec<i64> = Vec::with_capacity(epsilon_vec.len());
    let mut scale: f64;

    for i in 0..epsilon_vec.len() {
        scale = sensitivity_vec[i] / epsilon_vec[i];
        noise_vec.push( noise::sample_simple_geometric_mechanism(&scale, &count_min[i], &count_max[i], &enforce_constant_time[i]) )
    }

    return arr1(&noise_vec).into_dyn();
}