use whitenoise_validator::{Float, Integer};
use crate::utilities::noise::{sample_gaussian_mpfr};
use ndarray::{ArrayD, Array};
use rand::seq::SliceRandom;
use num_traits::float::FloatCore;

fn calculate_gradient(theta: &ArrayD<Float>, x: &ArrayD<Float>) -> Vec<Vec<Float>> {
    // TODO: Delta should be parameterized based on how we are scaling the data
    let mut delta = 0.0001;
    let mut gradients: Vec<Vec<Float>> = Vec::new();
    let initial_value = evaluate_function(theta, x);
    for i in 0..theta.len() {
        let mut theta_temp = theta.clone();
        theta_temp[i] += delta.clone();
        let function_value = evaluate_function(&theta_temp, x);
        for j in 0..function_value.len()-1 {
            gradients[j][i] = (initial_value.clone()[j] - function_value[j]) / delta.clone();
        }
    }
    gradients
}

fn evaluate_function(theta: &ArrayD<Float>, x: &ArrayD<Float>) -> Vec<Float> {
    let col: Vec<Float> = x.clone().into_raw_vec();
    let mut x_copy: ArrayD<Float> = x.clone();
    for i in 0..x.len() {
        x_copy[0][0] = 1;
    }
    let mut pi = col.clone();
    let mut llik = pi.clone();

    for i in 0..col.len() {
        let mut dot_sum = 0.0;
        for j in 0..theta.len() {
            dot_sum += theta[j]*x_copy[j];
        }
        pi[i] = 1.0 / (1.0 + (-1.0 * dot_sum).exp());
        llik[i] = col[i] * pi[i].ln() + (1.0 - col[i])*(1.0 - pi[i]).ln();
    }
    llik
}

fn sgd(data: &ArrayD<Float>, data_size: usize, mut theta: &ArrayD<Float>,
       learning_rate: Float, noise_scale: Float, group_size: Integer,
       gradient_norm_bound: Float, max_iters: Integer, clipping_value: Float,
       sample_size: usize) -> Vec<Vec<Float>> {

    // TODO: Check theta size matches data
    if theta.len() != data_size {
        println!("Starting parameters length does not match dataset dimensions!")
    }    
                 
    let mut theta_mutable = theta.clone();
    let mut thetas: Vec<Vec<Float>> = Vec::new();

    for t in 0..max_iters {
        // Random sample of observations, without replacement, of fixed size sample_size 
        // New sample each iteration
        let mut L: Vec<Float> = Vec::new();

        let range = (0..data_size.clone() - 1).map(|x| x as Integer).collect::<Vec<Integer>>();
        let mut rng = rand::thread_rng();
        let mut vec_sample: Vec<Integer> = range.choose_multiple(&mut rng, sample_size.clone()).cloned().collect();
        vec_sample.shuffle(&mut rng);
        let mut data_temp = Vec::new();
        for random_index in vec_sample {
            let new_entry = data[random_index as usize];
            data_temp.push(new_entry);
        }

        // Compute gradient
        let gradients: Vec<Vec<Float>> = calculate_gradient(&theta_mutable,
                                                            &Array::from(data_temp).into_dyn()).to_vec();

        // Clip gradient
        let mut clipped_gradients = gradients.clone();
        for j in 0..L.len()-1 {
            let gradients_sums = clipped_gradients.clone().iter().map(|x| x.iter().map(|y| y.powi(2))).collect();
            let gradients_magnitude: Float = gradients_sums.sum();
            if gradients_magnitude > clipping_value {
                clipped_gradients[j] = gradients[j].iter().map(|&x| x * clipping_value.clone() / gradients_magnitude).collect();
            } else {
                clipped_gradients[j] = gradients[j].clone();
            }
        }
        // Add noise
        let mut noisy_gradients = Vec::new();
        let mut multidim_gauss_noise = Vec::new();
        for _ in 0..data_size.clone() - 1 {
            let noise = sample_gaussian_mpfr(0.0, noise_scale.powi(2) * gradient_norm_bound.powi(2));
            multidim_gauss_noise.push(noise);
        }
        let mut gradient_sum: Vec<Float> = Vec::new();
        for i in 0..clipped_gradients.len()-1 {
            gradient_sum[i] = clipped_gradients[i].iter().sum();
        }
        let mut sum = 0.0;
        for i in 0..gradient_sum.len()-1 {
            sum += gradient_sum[i] + sample_gaussian_mpfr(0.0, noise_scale.powi(2) * gradient_norm_bound.powi(2)).to_f64();
        }
        let mut noisy_grad = (1.0 / group_size.clone() as Float) * sum;
        noisy_gradients.push(noisy_grad);

        // Descent
        for i in 0..theta_mutable.len()-1 {
            theta_mutable[i] = theta_mutable[i] - learning_rate.clone() * noisy_grad;
        }
        thetas.push(theta_mutable.into_raw_vec());
    }
    thetas
}


#[cfg(test)]
mod test_sgd {
    use crate::components::sgd::sgd;
    use ndarray_rand::RandomExt;
    use ndarray_rand::rand_distr::Uniform;
    use whitenoise_validator::Float;
    use ndarray::arr2;
    use ndarray::{Array};

    // use ndarray::{arr2, Array};
    // use whitenoise_validator::Float;

    #[test]
    fn main() {
        let a = Array::random((2, 5), Uniform::new(0., 10.));
        println!("{:8.4}", a);
        // Example Output:
        // [[  8.6900,   6.9824,   3.8922,   6.5861,   2.4890],
        //  [  0.0914,   5.5186,   5.8135,   5.2361,   3.1879]]
    }
    
    #[test]
    fn test_dp_sgd() {
        // Build large test dataset, with n rows, x~uniform; y~binomial(pi); pi = 1/(1+exp(-1 - 1x))
        let n = 1000;
        let mut data = arr2(&[[0.0, 1.0]]).into_dyn(); // arr2:random((1000,2), Uniform::new(0.0, 1.0));
        for i in 0..n-1 {
            data[i][0] = 1.0 /(1.0 + ((-1.0 - data[i][1]) as Float).exp())
        }
        let data_size = 2 as usize;
        let theta = arr2(&[[0.0, 0.0]]).into_dyn();
        let learning_rate = 0.1;
        let noise_scale = 1.0;
        let group_size = 2;
        let gradient_norm_bound = 0.15;
        let max_iters = 100;
        let clipping_value = 1.0;
        let sample_size = 5 as usize;
        let theta_final: Vec<Vec<Float>> = sgd(&data, data_size, &theta, learning_rate, noise_scale, group_size,
                    gradient_norm_bound, max_iters, clipping_value, sample_size);
        assert_eq!(theta_final.len(), max_iters.clone() as usize);
    }
}
