use statrs::function::erf;

pub fn phi(t: f64) -> f64 {
    0.5 * (1. + erf::erf(t / 2.0_f64.sqrt()))
}

pub fn case_a(epsilon: f64, s: f64) -> f64 {
    phi((epsilon * s).sqrt()) - epsilon.exp() * phi(-(epsilon * (s + 2.)).sqrt())
}

pub fn case_b(epsilon: f64, s: f64) -> f64 {
    phi(-(epsilon * s).sqrt()) - epsilon.exp() * phi(-(epsilon * (s + 2.)).sqrt())
}

pub fn doubling_trick(
    mut s_inf: f64, mut s_sup: f64, epsilon: f64, delta: f64, delta_thr: f64,
) -> (f64, f64) {
    let predicate = |s: f64| if delta > delta_thr {
        case_a(epsilon, s) < delta
    } else {
        case_b(epsilon, s) > delta
    };

    while predicate(s_sup) {
        s_inf = s_sup;
        s_sup = 2.0 * s_inf;
    }
    (s_inf, s_sup)
}

pub fn binary_search(
    mut s_inf: f64, mut s_sup: f64, epsilon: f64, delta: f64, delta_thr: f64, tol: f64,
) -> f64 {
    let mut s_mid: f64 = s_inf + (s_sup - s_inf) / 2.;

    let s_to_delta = |s: f64| if delta > delta_thr {
        case_a(epsilon, s)
    } else {
        case_b(epsilon, s)
    };

    loop {
        let delta_prime = s_to_delta(s_mid);

        let diff = delta_prime - delta;
        if (diff.abs() <= tol) && (diff <= 0.) { break }

        let is_left = if delta > delta_thr {
            delta_prime > delta
        } else {
            delta_prime < delta
        };

        if is_left {
            s_sup = s_mid;
        } else {
            s_inf = s_mid;
        }
        s_mid = s_inf + (s_sup - s_inf) / 2.;
    }
    s_mid
}

pub fn get_analytic_gaussian_sigma(epsilon: f64, delta: f64, sensitivity: f64) -> f64 {
    let delta_thr = case_a(epsilon, 0.);

    let alpha = if delta == delta_thr {
        1.
    } else {
        let (s_inf, s_sup) = doubling_trick(0., 1., epsilon, delta, delta_thr);
        let tol: f64 = 10_f64.powf(-12.);
        let s_final = binary_search(s_inf, s_sup, epsilon, delta, delta_thr, tol);
        (1. + s_final / 2.).sqrt() - (s_final / 2.).sqrt()
    };

    alpha * sensitivity / (2. * epsilon).sqrt()
}