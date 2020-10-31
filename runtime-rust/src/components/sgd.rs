use ndarray::ArrayD;
use ndarray::prelude::*;
use rand::seq::{SliceRandom, IteratorRandom};

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
            learning_rate, noise_scale, group_size, gradient_norm_bound,
            max_iters, clipping_value, sample_size
        } = self.clone();

        let theta = take_argument(&mut arguments, "theta")?.array()?.float()?;
        let theta = Array::from_shape_vec(
            theta.shape(),
            sgd(
                &take_argument(&mut arguments, "data")?.array()?.float()?,
                &theta,
                learning_rate, noise_scale, group_size.into(), gradient_norm_bound,
                max_iters.into(), clipping_value, sample_size as usize, enforce_constant_time)?
                .remove(max_iters as usize - 1),
        )?.into_dyn();


        Ok(ReleaseNode {
            value: theta.into(),
            // TODO: if the upper bound is not passed in, then I need a privacy usage back from sgd
            privacy_usages: None,
            public: true,
        })
    }
}

fn calculate_gradient(theta: &ArrayD<Float>, x: &ArrayD<Float>) -> Vec<Vec<Float>> {
    // TODO: Delta should be parameterized based on how we are scaling the data
    let delta = 0.0001;
    let mut gradients: Vec<Vec<Float>> = Vec::new();
    let initial_value = evaluate_function(theta, x);
    let lim = theta.len_of(Axis(1));
    for i in 0..lim {
        let mut theta_temp = theta.clone();
        theta_temp[[0,i]] += delta.clone();
        let function_value = evaluate_function(&theta_temp, x);
        let mut tmp = Vec::new();
        for j in 0..function_value.len() {
            tmp.push((initial_value.clone()[j] - function_value[j]) / delta.clone());
        }
        gradients.push(tmp);
    }
    gradients
}

fn evaluate_function(theta: &ArrayD<Float>, x: &ArrayD<Float>) -> Vec<Float> {
    let col = x.clone();
    let mut x_copy: ArrayD<Float> = x.clone();
    for i in 0..x.len_of(Axis(0)) {
        x_copy[[i,0]] = 1.0;
    }
    let mut llik = Vec::new();

    // // println!("theta shape: {:?} x_copy shape: {:?}", theta.shape(), x_copy.shape());
    let x_unwrapped = x_copy.into_dimensionality::<ndarray::Ix2>().unwrap();
    let theta_unwrapped = theta.clone().into_dimensionality::<ndarray::Ix2>().unwrap();
    for i in 0..col.len_of(Axis(0)) {
        let y = col[[i,0]];
        //let product = theta_unwrapped.dot(&x_unwrapped.t());
        //let dot_sum = product.scalar_sum();
        
        let mut dot_sum = 0.0;
        for j in 0..col.len_of(Axis(1)) {
            dot_sum += x_copy[[i,j]] * theta[[0,i]]   
        }
        
        // println!("dot sum: {:?}", dot_sum);
        let mut pi = 1.0 / (1.0 + (-1.0 * dot_sum).exp());
        // println!("tmp_exp: {:?}", tmp_exp);
        if pi > 0.999 {
            pi = 0.999;
        } else if pi < 0.001 {
            pi = 0.001;
        }
        // println!("col: {:?} pi: {:?}", col, pi);
        // TODO: This is to prevent passing 0 into the ln() argument....
        //let mut log_argument = (1.0 - y)*(1.0 - pi).ln();
        // println!("log_argument: {:?}", log_argument);
        //if log_argument == 0.0 {
        //    log_argument = 1.0;
        //}
        let llik_tmp = y * pi.ln() + (1.0 - y)*(1.0 - pi).ln();
        // println!("llik_tmp: {:?}", llik_tmp);
        llik.push(-llik_tmp);
    }
    // println!("pi: {:?} llik: {:?}", pi, llik);

    llik
}

fn sgd(
    data: &ArrayD<Float>, theta: &ArrayD<Float>,
    learning_rate: Float, noise_scale: Float, group_size: Integer,
    gradient_norm_bound: Float, max_iters: Integer, clipping_value: Float,
    sample_size: usize,
    enforce_constant_time: bool,
) -> Result<Vec<Vec<Float>>> {
    // TODO: Check theta size matches data
    let data_size = theta.shape()[1];
    let n = data.shape()[0];
    let mut theta_mutable = theta.clone();
    let mut thetas: Vec<Vec<Float>> = Vec::new();
    thetas.push(theta_mutable.clone().into_raw_vec());

    for _ in 0..max_iters {
        // Random sample of observations, without replacement, of fixed size sample_size 
        // New sample each iteration
        // TODO: This is used in loop for clipping?
        let clipping_loop_max: Vec<Float> = Vec::new();

        let mut rng = rand::thread_rng();
        let mut vec_sample: Vec<usize> = (0..n).choose_multiple(&mut rng, sample_size);
        vec_sample.shuffle(&mut rng);
        let data_temp = data.select(Axis(0), &vec_sample);
        // println!("data_temp: {:?}", data_temp);
        // let data_temp = data.select(Axis(0), &vec_sample.into_iter().map(|x| x as usize).collect::<Vec<_>>());

        // Compute gradient
        let gradients: Vec<Vec<Float>> = calculate_gradient(&theta_mutable,
                                                            &data_temp).to_vec();

        // Clip gradient
        let mut clipped_gradients = gradients.clone();
        for j in 0..theta_mutable.len_of(Axis(0)) {
            let gradient_sums: Vec<Float> = clipped_gradients.clone().iter().map(|x| x.iter().map(|y| y.powi(2)).sum()).collect();
            let gradients_magnitude: Float = gradient_sums.iter().map(|x| x.powi(2)).sum();
            if gradients_magnitude > clipping_value {
                clipped_gradients[j] = gradients[j].iter().map(|&x| x * clipping_value.clone() / gradients_magnitude).collect();
            } else {
                clipped_gradients[j] = gradients[j].clone();
            }
        }
        // Add noise
        let mut noisy_gradients = Vec::new();
        let mut gradient_sum: Vec<Float> = Vec::new();
        for i in 0..data_size {
            let mut sum = 0.0;
            for j in 0..sample_size {
                sum += clipped_gradients[i][j];
            }
            sum += sample_gaussian(0.0, noise_scale.powi(2) * gradient_norm_bound.powi(2), enforce_constant_time)?;

            let noisy_grad = (1.0 / sample_size.clone() as Float) * sum;
            noisy_gradients.push(noisy_grad);
        }
        // Descent
        for i in 0..theta_mutable.len() {
            theta_mutable[[0,i]] -= learning_rate.clone() * noisy_gradients[i];
        }
        thetas.push(theta_mutable.clone().into_raw_vec());
    }
    Ok(thetas)
}


#[cfg(test)]
mod test_sgd {
    use ndarray::arr2;
    use ndarray::Array;
    use ndarray_rand::rand_distr::Uniform;
    use ndarray_rand::RandomExt;

    use smartnoise_validator::Float;

    use crate::components::sgd::sgd;
    use num::ToPrimitive;
    use crate::utilities::noise::sample_binomial;

// use ndarray::{arr2, Array};
    // use smartnoise_validator::Float;

    #[test]
    fn generate_random_array() {
        let a = Array::random((2, 5), Uniform::new(0., 10.));
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
             let transform = 1.0 /(1.0 + ((1.0 - 3.0 * data[[i, 1]]) as Float).exp());
            data[[i,0]] = sample_binomial(1, transform, false).unwrap() as Float;
        }
        let theta = Array::random((1, m), Uniform::new(0.0, 0.01));
        let learning_rate = 0.1;
        let noise_scale = 0.1;
        let group_size = 2;
        let gradient_norm_bound = 0.15;
        let max_iters = 100;
        let enforce_constant_time = false;
        let clipping_value = 1.0;
        let sample_size = 100 as usize;
        let thetas: Vec<Vec<Float>> = sgd(&data.into_dyn(), &theta.into_dyn(), learning_rate, noise_scale, group_size,
                                               gradient_norm_bound, max_iters, clipping_value, sample_size, enforce_constant_time).unwrap();
        println!("thetas:");
        for i in 0..thetas.len() {
            println!("{:?}", thetas[i]);
        }

        // assert_eq!(thetas.len()[0], max_iters as usize);

    }
}
