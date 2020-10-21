use whitenoise_validator::{Float, Integer};
use crate::utilities::noise::{sample_uniform, sample_gaussian_mpfr};
use ndarray::{ArrayD, Array};
use std::cmp;
use rand::seq::SliceRandom;

fn calculate_gradient(theta: &ArrayD<Float>, x: &ArrayD<Float>) -> Vec<Vec<Float>> {
    // TODO: Delta should be parameterized based on how we are scaling the data
    let mut delta = 0.0001;
    let mut gradients: Vec<Vec<Float>> = Vec::new();
    let initial_value = evaluate_function(theta, x);
    for i in 0..theta.len() {
        let mut theta_temp = theta.clone();
        theta_temp[i] += delta;
        let function_value = evaluate_function(&theta_temp, x);
        for j in 0..function_value.len()-1 {
            gradients[j][i] = (initial_value.clone()[j] - function_value[j]) / delta;
        }
    }
    gradients
}

fn evaluate_function(theta: &ArrayD<Float>, x: &ArrayD<Float>) -> Vec<Float> {
    let col: Vec<Float> = x.iter().map(|x| x[0]).collect();
    let mut x_copy = x.clone();
    for i in 0..x_copy.len() {
        x_copy[i][0] = 1.0;
    }
    let mut new_thing = col.clone();
    let mut llik = new_thing.clone();

    for i in 0..col.len() {
        let mut dot_sum = 0.0;
        for j in 0..theta.len() {
            dot_sum += theta[j]*x_copy[j];
        }
        new_thing[i] = 1 / (1.0 + (-1.0 * dot_sum).exp());
        llik[i] = col[i] * new_thing[i].ln() + (1.0 - col[i])*(1.0 - new_thing[i]).ln();
    }
    llik
}

fn sgd(data: &ArrayD<Float>, data_size: usize, mut theta: &ArrayD<Float>,
       learning_rate: Float, noise_scale: Float, group_size: Integer,
       gradient_norm_bound: Float, max_iters: Integer, clipping_value: Float,
       sample_size: usize) -> Vec<Vec<Float>> {

    // TODO: Check theta size matches data
    let mut theta_mutable = theta.clone();
    let mut thetas: Vec<Vec<Float>> = Vec::new();

    for t in 0..max_iters {
        // Random Sample with P = L/N
        let mut L: Vec<Float> = Vec::new();

        let range = (0..data_size-1).map(Integer::from).collect::<Vec<Integer>>();
        let mut rng = rand::thread_rng();
        let mut vec_sample: Vec<Integer> = range.choose_multiple(&mut rng, sample_size).cloned().collect();
        vec_sample.shuffle(&mut rng);
        let mut data_temp = Vec::new();
        for random_index in vec_sample {
            data_temp.push(data.into_dyn()[random_index]);
        }

        // Compute gradient
        let gradients: Vec<Vec<Float>> = calculate_gradient(&theta_mutable,
                                                            &Array::from(data_temp).into_dyn()).to_vec();

        // Clip gradient
        let mut clipped_gradients = gradients.clone();
        for j in 0..L.len()-1 {
            let gradients_magnitude = clipped_gradients.clone().iter().map(|x| x.iter().map(|y| y.powi(2))).sum().collect();
            if gradients_magnitude > clipping_value {
                clipped_gradients[j] = gradients[j].iter().map(|&x| x * clipping_value/gradients_magnitude).collect();
            } else {
                clipped_gradients[j] = gradients[j].clone();
            }
        }
        // Add noise
        let mut noisy_gradients = Vec::new();
        let mut multidim_gauss_noise = Vec::new();
        for _ in 0..data_size-1 {
            let noise = sample_gaussian_mpfr(0.0, noise_scale.powi(2) * gradient_norm_bound.powi(2));
            multidim_gauss_noise.push(noise);
        }
        let gradient_sum: Vec<Float> = clipped_gradients.iter().sum().collect();
        let mut sum = 0.0;
        for i in 0..gradient_sum.len()-1 {
            sum += gradient_sum[i] + sample_gaussian_mpfr(0.0, noise_scale.powi(2) * gradient_norm_bound.powi(2)).to_f64();
        }
        let mut noisy_grad = (1.0 / group_size as Float) * sum;
        noisy_gradients.push(noisy_grad);

        // Descent
        for i in 0..theta_mutable.len()-1 {
            theta_mutable[i] = theta_mutable[i] - learning_rate * noisy_grad;
        }
        thetas.push(theta_mutable.into_raw_vec());
    }
    thetas
}


#[cfg(test)]
mod test_sgd {
    use ndarray::arr2;
    use crate::components::sgd::sgd;
    use whitenoise_validator::Float;

    #[test]
    fn test_dp_sgd() {
        let data = arr2(&[[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
            [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]]).into_dyn();
        let data_size = 10 as usize;
        let theta = arr2(&[[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]]).into_dyn();
        let learning_rate = 1.0;
        let noise_scale = 1.0;
        let group_size = 2;
        let gradient_norm_bound = 0.5;
        let max_iters = 100;
        let clipping_value = 2.0;
        let sample_size = 5 as usize;
        let theta_final: Vec<Vec<Float>> = sgd(&data, data_size, &theta, learning_rate, noise_scale, group_size,
                    gradient_norm_bound, max_iters, clipping_value, sample_size);
        assert_eq!(theta_final.len(), max_iters as usize);
    }
}