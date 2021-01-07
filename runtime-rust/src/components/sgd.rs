use std::ops::{DivAssign, SubAssign};

use ndarray::prelude::*;
use rand::seq::SliceRandom;

use smartnoise_validator::{Float, Integer, proto};
use smartnoise_validator::base::ReleaseNode;
use smartnoise_validator::errors::*;
use smartnoise_validator::utilities::take_argument;

use crate::components::Evaluable;
use crate::NodeArguments;
use crate::utilities::noise::sample_gaussian;

// Add public_data, private_data_1, private_data_2
impl Evaluable for proto::Dpsgd {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let proto::Dpsgd {
            learning_rate, noise_scale, gradient_norm_bound,
            max_iters, sample_size, ..
        } = self.clone();

        let max_iters = max_iters as usize;

        // unpack
        // add public_data
        let public_data: Option<_> = if let Ok(data) = take_argument(&mut arguments, "public_data") {
            Some(data.array()?.float()?.into_dimensionality()?)
        } else { None };
        // println!("public data {:?}", public_data);

        // add (federated) private data
        let data = take_argument(&mut arguments, "data")?.array()?.float()?.into_dimensionality()?;

        let data_2: Option<_> = if let Ok(data) = take_argument(&mut arguments, "data_2") {
            Some(data.array()?.float()?.into_dimensionality()?)
        } else { None };

        // add starting value
        let mut theta = take_argument(&mut arguments, "theta")?.array()?.float()?.into_dimensionality()?;

        let history_length = max_iters * if public_data.is_none() { 1 } else { 2 } + 1;

        let mut theta_history: Array2<Float> = Array2::zeros((theta.shape()[0], history_length as usize));
        let mut counter = 0;

        theta_history.slice_mut(s![.., counter]).assign(&theta);
        counter += 1;

        // optimize
        // note sgd takes a theta history, uses the last value, appends any new steps and returns lengthened history
        // if public data exists, converge theta using public_data

        let private_options = PrivateSGDOptions {
            noise_scale,
            gradient_norm_bound,
            enforce_constant_time
        };

        if let Some(public_data) = public_data {
            let temp_theta = sgd(
                public_data, theta, learning_rate,
                max_iters as Integer, sample_size as usize,
                SGDOptions::Public, private_options.clone())?.into_dyn();
            theta_history.slice_mut(s![.., 1..1 + max_iters]).assign(&temp_theta);
            counter += max_iters;
            theta = theta_history.index_axis(Axis(1), counter - 1).to_owned();
            println!("final public theta! {:?}", theta);
        }

        // if second data source exists, federate by iterating across datasets
        if let Some(data_2) = data_2 {
            while counter < history_length - 1 {
                let temp_hist_theta1 = sgd(
                    data.clone(), theta.clone(), learning_rate, 1, sample_size as usize, SGDOptions::Private, private_options.clone())?;
                theta = temp_hist_theta1.index_axis(Axis(1), 0).to_owned();
                theta_history.slice_mut(s![.., counter]).assign(&theta);
                counter += 1;

                let temp_hist_theta2 = sgd(
                    data_2.clone(), theta.clone(), learning_rate, 1, sample_size as usize, SGDOptions::Private,private_options.clone())?;
                theta = temp_hist_theta2.index_axis(Axis(1), 0).to_owned();
                theta_history.slice_mut(s![.., counter]).assign(&theta);
                counter += 1;
            }
        } else {

            // else run long sgd chain on one dataset
            let temp_history_theta = sgd(
                data, theta, learning_rate, max_iters as Integer, sample_size as usize, SGDOptions::Private, private_options)?.into_dyn();
            theta_history.slice_mut(s![.., counter..]).assign(&temp_history_theta);
            // counter += max_iters;
        }

        Ok(ReleaseNode {
            value: theta_history.into(),
            // TODO: if the upper bound is not passed in, then I need a privacy usage back from sgd
            privacy_usages: None,
            public: true,
        })
    }
}

#[derive(Clone)]
enum SGDOptions {
    Public,
    Private
}

#[derive(Clone)]
struct PrivateSGDOptions {
    noise_scale: Float,
    gradient_norm_bound: Float,
    enforce_constant_time: bool,
}

/// Calculates an approximate gradient using (f(x | theta + delta) - f(x |theta)) / delta
///
/// # Arguments
/// * `theta` - network weights to perturb
/// * `data` - data to evaluate forward pass of function on (x)
/// * `y` - expected values of the function
/// * `delta` - amount to perturb theta
///
/// # Return
/// Approximation to gradient
fn calculate_gradient(
    mut theta: Array1<Float>, data: &Array2<Float>, y: &Array1<Float>, delta: Float,
) -> Result<Array2<Float>> {
    let initial_nll = evaluate_nll(&theta, data, y);
    println!("initial_nll: {}", initial_nll);
    // each element contains the partials for each user, for one parameter
    let perturbed_nlls = (0..theta.len_of(Axis(0)))
        .into_iter()
        .map(|i_param| {
            theta[i_param] += delta;
            let perturbation_value = evaluate_nll(&theta, data, y).insert_axis(Axis(0));
            theta[i_param] -= delta;
            perturbation_value
        })
        .collect::<Vec<_>>();
    println!("perturbed_nll: {:?}", perturbed_nlls);

    // stack the perturbations into one array
    let mut output = ndarray::stack(Axis(0), &perturbed_nlls.iter()
        .map(|v| v.view()).collect::<Vec<_>>())?;
    output.sub_assign(&initial_nll);
    Ok(output.mapv(|v| v / delta))
}

/// Calculates the negative log-likelihood of a logistic regression model
///
/// # Arguments
/// * `theta` - network weights to perturb
/// * `data` - data to evaluate forward pass of function on (x)
/// * `y` - expected values of the function
///
/// # Return
/// Negative log-likelihood
fn evaluate_nll(theta: &Array1<Float>, data: &Array2<Float>, y: &Array1<Float>) -> Array1<Float> {
    let mut x = data.dot(theta);
    println!("x before mapv_inplace: {}", x);
    x.mapv_inplace(|v| 1.0 / (1.0 + (-v).exp()));
    // println!("x after mapv_inplace: {}", x);
    -(x.mapv(Float::ln) * y + (1.0 - y) * (1.0 - x).mapv(Float::ln))
}

/// Optimize the parameters of a logistic regression network using privacy-preserving gradient descent
///
/// # Arguments
/// * `data` - dataset where the first column is the target variable
/// * `theta` - network weights to perturb
/// * `learning_rate` - scale the gradients at each step
/// * `noise_scale` - scale the noise at each step
/// * `gradient_norm_bound` - maximum gradient norm
/// * `max_iters` - number of steps to run
/// * `sample_size` - number of records to sample at each iteration
/// * `enforce_constant_time` - enforce the elapsed time to sample noise is constant
///
/// # Return
/// Approximation to gradient
fn sgd(
    mut data: Array2<Float>, mut theta: Array1<Float>,
    learning_rate: Float,
    max_iters: Integer,
    sample_size: usize,
    options: SGDOptions,
    private_options: PrivateSGDOptions
) -> Result<Array2<Float>> {
    let delta = 0.0001;

    if data.len_of(Axis(1)) != theta.len_of(Axis(0)) {
        return Err(Error::from("data and theta are non-conformable"))
    }

    let num_rows = data.len_of(Axis(0));
    let num_cols = data.len_of(Axis(1));
    let mut rng = rand::thread_rng();
    let mut indices: Vec<usize> = (0..num_rows).collect();
    indices.shuffle(&mut rng);

    // retrieve the target column as the first column of data
    let y = data.slice(s![.., 0]).to_owned();
    // repurpose first column as intercept
    data.slice_mut(s![.., 0]).fill(1.);

    // each column is an iteration
    // do not put theta into first element of theta_history
    let mut theta_history: Array2<Float> = Array2::zeros((theta.shape()[0], max_iters as usize));

    for i in 0..max_iters as usize {
        let indices_sample = indices.choose_multiple(&mut rng, sample_size).cloned().collect::<Vec<_>>();
        let data_sample = data.select(Axis(0), &indices_sample);
        let y_sample = y.select(Axis(0), &indices_sample);

        // one column for each sampled index
        let mut gradients: Array2<Float> = calculate_gradient(theta.clone(), &data_sample, &y_sample, delta)?;
        // clip - scale down by l2 norm and don't scale small elements
        let PrivateSGDOptions { noise_scale, gradient_norm_bound, enforce_constant_time } = private_options.clone();
        gradients.div_assign(&Array1::from(gradients.gencolumns().into_iter()
            .map(|grad_i| (grad_i.dot(&grad_i).sqrt() / gradient_norm_bound).max(1.))
            .collect::<Vec<Float>>()).insert_axis(Axis(0)));

        println!("gradients {}", gradients);
        theta.sub_assign(&match &options {
            SGDOptions::Public => gradients.sum_axis(Axis(1)) * (learning_rate / sample_size as Float),
            SGDOptions::Private => {
                // let PrivateSGDOptions { noise_scale, gradient_norm_bound, enforce_constant_time } = v.clone();
                // // clip - scale down by l2 norm and don't scale small elements
                // gradients.div_assign(&Array1::from(gradients.gencolumns().into_iter()
                //     .map(|grad_i| (grad_i.dot(&grad_i).sqrt() / gradient_norm_bound).max(1.))
                //     .collect::<Vec<Float>>()).insert_axis(Axis(0)));

                // noise
                let sigma = (noise_scale * gradient_norm_bound).powi(2);
                let noise = Array1::from((0..num_cols)
                    .map(|_| sample_gaussian(0.0, sigma, enforce_constant_time))
                    .collect::<Result<Vec<_>>>()?);

                // update
                (gradients.sum_axis(Axis(1)) + noise) * (learning_rate / sample_size as Float)
            }
        });

        if theta.iter().cloned().any(f64::is_nan) {
            return Err("Undefined theta parameter value".into());
        }

        theta_history.slice_mut(s![.., i]).assign(&theta);
    }

    Ok(theta_history)
}


#[cfg(test)]
mod test_sgd {
    use ndarray::Array;
    use ndarray::Array2;
    use ndarray_rand::rand_distr::Uniform;
    use ndarray_rand::RandomExt;

    use smartnoise_validator::Float;

    use crate::components::sgd::{sgd, SGDOptions, PrivateSGDOptions};
    use crate::utilities::noise::sample_binomial;
    use crate::components::Evaluable;

// use ndarray::{arr2, Array};
    // use smartnoise_validator::Float;

    #[test]
    fn generate_random_array() {
        let _a = Array::random((2, 5), Uniform::new(0., 10.));
        // println!("{:8.4}", a);
        // Example Output:
        // [[  8.6900,   6.9824,   3.8922,   6.5861,   2.4890],
        //  [  0.0914,   5.5186,   5.8135,   5.2361,   3.1879]]
    }

    #[test]
    fn test_dp_sgd() {
        // Build large test dataset, with n rows, x~uniform; y~binomial(pi); pi = 1/(1+exp(-1 - 1x))
        let n = 1000;
        let m = 2;
        let mut data = Array::random((n, m), Uniform::new(0.0, 1.0)); // arr2:random((1000,2), Uniform::new(0.0, 1.0));
        for i in 0..n {
            let transform = 1.0 / (1.0 + ((1.0 - 3.0 * data[[i, 1]]) as Float).exp());
            data[[i, 0]] = sample_binomial(1, transform, false).unwrap() as Float;
        }
        let mut theta = Array::random((m, ), Uniform::new(0.0, 1.0));
        theta[[0]] = 0.0;
        theta[[1]] = 0.0;
        let learning_rate = 1.0;
        let noise_scale = 0.1;
        let gradient_norm_bound = 1.0;//0.15;
        let max_iters = 1000;
        let enforce_constant_time = false;
        let sample_size = 100 as usize;
        let private_options = SGDOptions::Private(PrivateSGDOptions {
            noise_scale,
            gradient_norm_bound,
            enforce_constant_time
        });
        let thetas: Array2<Float> = sgd(data, theta, learning_rate, max_iters, sample_size, private_options).unwrap();
        println!("thetas: {:?}", thetas);

        // assert_eq!(thetas.len()[0], max_iters as usize);
    }
}
