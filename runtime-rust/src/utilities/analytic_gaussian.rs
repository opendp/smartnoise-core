use whitenoise_validator::errors::*;

use ndarray::prelude::*;

use rug::{float::Constant, Float, ops::Pow};
use statrs::function::erf;

use crate::utilities::noise;
use crate::utilities;

pub fn predicate_stop_DT(epsilon: &f64, s: &f64, delta: &f64, delta_thr: &f64) -> bool {
    let pred: bool;
    if (delta > delta_thr) {
        pred = caseA(&epsilon, &s) >= *delta;
    } else {
        pred = caseB(&epsilon, &s) <= *delta;
    }
    return(pred);
}

pub fn function_s_to_delta(epsilon: &f64, s: &f64, delta: &f64, delta_thr: &f64) -> f64 {
    let ans: f64;
    if (delta > delta_thr) {
        ans = caseA(&epsilon, &s);
    } else {
        ans = caseB(&epsilon, &s);
    }
    return(ans);
}

pub fn predicate_left_BS(epsilon: &f64, s: &f64, delta: &f64, delta_thr: &f64) -> bool {
    let pred: bool;
    if (delta > delta_thr) {
        pred = function_s_to_delta(&epsilon, &s, &delta, &delta_thr) > *delta;
    } else {
        pred = function_s_to_delta(&epsilon, &s, &delta, &delta_thr) < *delta;
    }
    return(pred);
}

pub fn function_s_to_alpha(epsilon: &f64, s: &f64, delta: &f64, delta_thr: &f64) -> f64 {
    let ans: f64;
    if (delta > delta_thr) {
        ans = (1. + s/2.).sqrt() - (s/2.).sqrt();
    } else {
        ans = (1. + s/2.).sqrt() + (s/2.).sqrt();
    }
    return(ans);
}

pub fn predicate_stop_BS(epsilon: &f64, s: &f64, delta: &f64, delta_thr: &f64, tol: &f64) -> bool {
    let pred: bool = (function_s_to_delta(&epsilon, &s, &delta, &delta_thr) - *delta) <= *tol;
    return(pred);
}

pub fn Phi(t: &f64) -> f64 {
    return( 0.5*(1. + erf::erf(t/2.0_f64.sqrt())) );
}

pub fn caseA(epsilon: &f64, s: &f64) -> f64 {
    return( Phi( &(epsilon*s).sqrt() ) - epsilon.exp() * Phi( &(epsilon*(s+2.)).sqrt() ) );
}

pub fn caseB(epsilon: &f64, s: &f64) -> f64 {
    return( Phi( &(-(epsilon*s).sqrt() )) - epsilon.exp() * Phi( &(epsilon*(s+2.)).sqrt() ) );
}

pub fn doubling_trick(s_inf: &f64, s_sup: &f64, epsilon: &f64, delta: &f64, delta_thr: &f64) -> (f64, f64) {
    let mut s_inf_mut = *s_inf;
    let mut s_sup_mut = *s_sup;
    while ( predicate_stop_DT(epsilon, &s_sup_mut, delta, delta_thr) == false ) {
        s_inf_mut = s_sup_mut;
        s_sup_mut = 2.0 * s_inf_mut;
    }
    return(s_inf_mut, s_sup_mut);
}

pub fn binary_search(s_inf: &f64, s_sup: &f64, epsilon: &f64, delta: &f64, delta_thr: &f64, tol: &f64) -> f64 {
    let mut s_inf_mut: f64 = *s_inf;
    let mut s_sup_mut: f64 = *s_sup;
    let mut s_mid: f64 = s_inf_mut + (s_sup_mut - s_inf_mut)/2.;
    while ( predicate_stop_BS(epsilon, &s_mid, delta, delta_thr, tol) == false ) {
        if ( predicate_left_BS(epsilon, &s_mid, delta, delta_thr) == true ) {
            s_sup_mut = s_mid;
        } else {
            s_inf_mut = s_mid;
        }
    }
    return( s_inf_mut + (s_sup_mut - s_inf_mut)/2. );
}

pub fn get_analytic_gaussian_sigma(epsilon: &f64, delta: &f64, sensitivity: &f64, tol: &f64) -> f64 {
    let delta_thr = caseA(epsilon, &0.);
    let mut alpha: f64 = 0.;
    if (delta == &delta_thr) {
        alpha = 1.;
    } else {
        let (s_inf, s_sup) = doubling_trick(&0., &1., epsilon, delta, sensitivity);
        let s_final = binary_search(&s_inf, &s_sup, epsilon, delta, &delta_thr, tol);
        let alpha = function_s_to_alpha(epsilon, &s_final, delta, &delta_thr);
    }
    return( alpha * *sensitivity / (2. * *epsilon).sqrt() );
}