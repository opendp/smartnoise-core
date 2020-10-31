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

impl Evaluable for proto::Dpsgd {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let proto::Dpsgd {
            learning_rate, noise_scale, gradient_norm_bound,
            max_iters, sample_size, ..
        } = self.clone();

        // unpack
        let data = take_argument(&mut arguments, "data")?.array()?.float()?.into_dimensionality()?;
        let theta = take_argument(&mut arguments, "theta")?.array()?.float()?.into_dimensionality()?;

        // optimize
        let theta_history = sgd(
            data, theta, learning_rate, noise_scale, gradient_norm_bound, max_iters.into(), sample_size as usize, enforce_constant_time)?.into_dyn();

        Ok(ReleaseNode {
            value: theta_history.into(),
            // TODO: if the upper bound is not passed in, then I need a privacy usage back from sgd
            privacy_usages: None,
            public: true,
        })
    }
}

fn calculate_gradient(
    mut theta: Array1<Float>, data: &Array2<Float>, y: &Array1<Float>, delta: Float,
) -> Result<Array2<Float>> {
    let initial_value = evaluate_nll(&theta, data, y);
    // each element contains the partials for each user, for one parameter
    let perturbation_rows = (0..theta.len_of(Axis(0)))
        .into_iter()
        .map(|i_param| {
            theta[i_param] += delta;
            let perturbation_value = evaluate_nll(&theta, data, y).insert_axis(Axis(0));
            theta[i_param] -= delta;
            perturbation_value
        })
        .collect::<Vec<_>>();
    // stack the perturbations into one array
    let mut gradient = ndarray::stack(Axis(0), &perturbation_rows.iter()
        .map(|v| v.view()).collect::<Vec<_>>())?;

    gradient.sub_assign(&initial_value);
    Ok(gradient.mapv(|v| v / delta))
}

fn evaluate_nll(theta: &Array1<Float>, data: &Array2<Float>, y: &Array1<Float>) -> Array1<Float> {
    let mut x = data.dot(theta);
    x.mapv_inplace(|v| num::clamp(1.0 / (1.0 + (-v).exp()), 0.001, 0.999));
    -(x.mapv(Float::ln) * y + (1.0 - y) * (1.0 - x).mapv(Float::ln))
}


fn sgd(
    mut data: Array2<Float>, mut theta: Array1<Float>,
    learning_rate: Float, noise_scale: Float,
    gradient_norm_bound: Float, max_iters: Integer,
    sample_size: usize,
    enforce_constant_time: bool,
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
    let mut theta_history: Array2<Float> = Array2::zeros((theta.shape()[0], max_iters as usize));

    for i in 0..max_iters as usize {
        let indices_sample = indices.choose_multiple(&mut rng, sample_size).cloned().collect::<Vec<_>>();
        let data_sample = data.select(Axis(0), &indices_sample);
        let y_sample = y.select(Axis(0), &indices_sample);

        // one column for each sampled index
        let mut gradients: Array2<Float> = calculate_gradient(theta.clone(), &data_sample, &y_sample, delta)?;

        // clip - scale down by l2 norm and don't scale small elements
        gradients.div_assign(&Array1::from(gradients.gencolumns().into_iter()
            .map(|grad_i| (grad_i.dot(&grad_i).sqrt() / gradient_norm_bound).max(1.))
            .collect::<Vec<Float>>()).insert_axis(Axis(1)));

        // noise
        let sigma = (noise_scale * gradient_norm_bound).powi(2);
        let noise = Array1::from((0..num_cols)
            .map(|_| sample_gaussian(0.0, sigma, enforce_constant_time))
            .collect::<Result<Vec<_>>>()?);

        // update
        theta.sub_assign(&((gradients.sum_axis(Axis(1)) + noise) * (learning_rate / sample_size as Float)));

        theta_history.slice_mut(s![.., i]).assign(&theta);
    }

    Ok(theta_history)
}


#[cfg(test)]
mod test_sgd {
    use ndarray::Array2;
    use ndarray::Array;
    use ndarray_rand::rand_distr::Uniform;
    use ndarray_rand::RandomExt;

    use smartnoise_validator::Float;

    use crate::components::sgd::sgd;
    use crate::utilities::noise::sample_binomial;

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
        let mut data = Array::random((n, m), Uniform::new(0., 0.01)); // arr2:random((1000,2), Uniform::new(0.0, 1.0));
        for i in 0..n {
            let transform = 1.0 / (1.0 + ((1.0 - 3.0 * data[[i, 1]]) as Float).exp());
            data[[i, 0]] = sample_binomial(1, transform, false).unwrap() as Float;
        }
        let mut theta = Array::random((m, ), Uniform::new(0.0, 1.0));
        theta[[0]] = -0.5;
        theta[[1]] = 2.0;
        let learning_rate = 1.0;
        let noise_scale = 0.1;
        let gradient_norm_bound = 1.0;//0.15;
        let max_iters = 1000;
        let enforce_constant_time = false;
        let sample_size = 100 as usize;
        let thetas: Array2<Float> = sgd(data, theta, learning_rate, noise_scale,
                                        gradient_norm_bound, max_iters, sample_size, enforce_constant_time).unwrap();
        println!("thetas: {:?}", thetas);

        // assert_eq!(thetas.len()[0], max_iters as usize);
    }
}
