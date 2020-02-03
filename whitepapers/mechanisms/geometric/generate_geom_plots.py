import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import math
import gmpy2

def find_max_mult_difference(alpha, n):
    # NOTE: n is the max_count
    L_n_reciprocal = gmpy2.mpfr((alpha**(2*n+2) + alpha**n) / (alpha**(2*n+1) + alpha**(n+1)), 128)
    U_0_reciprocal = gmpy2.mpfr((1 + alpha**(n+2)) / (alpha + alpha**(n+1)), 128)
    L_1 = gmpy2.mpfr((alpha**3 + alpha**(n+1)) / (alpha**4 + alpha**n), 128)
    U_n1 = gmpy2.mpfr((alpha**(2*n-1) + alpha**(n+1)) / (alpha**(2*n-2) + alpha**(n+2)), 128)
    return( float(max(L_n_reciprocal, U_0_reciprocal, L_1, U_n1)) )

def find_specific_max_mult_difference(true_val, epsilon, alpha, max_count):
    # NOTE: min_count as 0 is implicit
    class_array = [i for i in range(31)]
    score_array = [30 - abs(true_val - i) for i in range(31)]
    true_val_est = exponential_mechanism(class_array, score_array, epsilon, sensitivity = 1)

    L = gmpy2.mpfr( (alpha**(2*true_val_est + 1) + alpha**(max_count + 1)) /
                    (alpha**(2*true_val_est + 2) + alpha**(max_count)), 128)
    U = gmpy2.mpfr( (alpha**(2*true_val_est + 1) + alpha**(max_count + 1)) /
                    (alpha**(2*true_val_est) + alpha**(max_count+2)), 128)
    return( float(max(1/L, L, 1/U, U)) )

def exponential_mechanism(classes, scores, epsilon, sensitivity):
    ''''''
    exp_scores = np.exp([epsilon * score / (2*sensitivity) for score in scores])
    exp_probs = exp_scores / np.sum(exp_scores)
    output = np.random.choice(classes, size = 1, p = exp_probs)[0]
    return(int(output))

def rejection_sampling_geom(alpha_prime, true_val, count_min, count_max, M, i):
    print("{}".format(i))
    while True:
        # TODO: check if this is correct threshold
        threshold = (1-alpha_prime)/(1+alpha_prime)
        unif = np.random.uniform(low = 0, high = 1, size = 1)
        if unif < threshold:
            return true_val
        sign = np.random.choice(a = [-1,1], size = 1)[0]
        draw = np.random.geometric(p = 1-alpha_prime, size = 1)[0]
        geom = sign*draw
        ret = true_val + geom
        if ret >= count_min and ret <= count_max:
            return ret

if __name__ == '__main__':
    # generate draws from two-sided geometric (no possibility of returning 0)
    epsilon = 0.2
    alpha = math.e ** -epsilon
    n = 100_000

    '''rejection sampling approach'''
    M_a = find_specific_max_mult_difference(20, epsilon, alpha, 30)
    M_b = find_specific_max_mult_difference(21, epsilon, alpha, 30)
    M_c = find_specific_max_mult_difference(19, epsilon, alpha, 30)

    epsilon_prime = epsilon - math.log(M)
    alpha_prime = math.e ** -epsilon_prime
    data_a_release = [rejection_sampling_geom(alpha_prime, 20, 0, 30, M_a, i) for i in range(n)]
    data_b_release = [rejection_sampling_geom(alpha_prime, 21, 0, 30, M_b, i) for i in range(n)]
    data_c_release = [rejection_sampling_geom(alpha_prime, 19, 0, 30, M_c, i) for i in range(n)]

    truncated_geom_df = pd.DataFrame(columns = ['mechanism_return', 'count', 'phi_D'])
    for i in range(31):
        data_a_count = data_a_release.count(i)
        data_b_count = data_b_release.count(i)
        data_c_count = data_c_release.count(i)
        truncated_geom_df.loc[i] = [i, data_c_count, '19']
        truncated_geom_df.loc[i+31] = [i, data_a_count, '20']
        truncated_geom_df.loc[i+62] = [i, data_b_count, '21']
    truncated_geom_df['prob'] = truncated_geom_df['count'] / n

    # plot distributions induced by truncated geometric mechanism
    plt.clf()
    truncated_plot = sns.barplot(x = 'mechanism_return', y = 'prob', hue = 'phi_D', data = truncated_geom_df)
    truncated_plot.set_xticklabels(truncated_plot.get_xticklabels(), fontsize='small', rotation = -45)
    plt.title('Geometric mechanism over neighboring data sets (rejection sampling)')
    plt.savefig('rejection_sampling_geometric_mech_dist.png')
    plt.clf()

    # '''normal truncated version'''
    # geom = np.random.geometric(p = 1-alpha, size = n)
    # sign = np.random.choice(a = [-1,1], size = n)
    # two_sided_geom_noise = [g*s for g,s in zip(geom,sign)]

    # # generate uniform (0,1), then threshold to randomly set geom noise to 0
    # threshold = (1-alpha)/(1+alpha)
    # unif = np.random.uniform(low = 0, high = 1, size = n)
    # geom_mech = [0 if u < threshold else n for n, u in zip(two_sided_geom_noise, unif)]

    # # create distributions from geometric mechanism on neighboring data sets
    # count_min = 0
    # count_max = 30
    # data_a_release = [max(count_min, min(20 + elem, count_max)) for elem in geom_mech]
    # data_b_release = [max(count_min, min(21 + elem, count_max)) for elem in geom_mech]
    # data_c_release = [max(count_min, min(19 + elem, count_max)) for elem in geom_mech]

    # # build data frame for easier barplotting
    # truncated_geom_df = pd.DataFrame(columns = ['mechanism_return', 'count', 'phi_D'])
    # for i in range(31):
    #     data_a_count = data_a_release.count(i)
    #     data_b_count = data_b_release.count(i)
    #     data_c_count = data_c_release.count(i)
    #     truncated_geom_df.loc[i] = [i, data_c_count, '19']
    #     truncated_geom_df.loc[i+31] = [i, data_a_count, '20']
    #     truncated_geom_df.loc[i+62] = [i, data_b_count, '21']
    # truncated_geom_df['prob'] = truncated_geom_df['count'] / n

    # # plot distributions induced by truncated geometric mechanism
    # plt.clf()
    # truncated_plot = sns.barplot(x = 'mechanism_return', y = 'prob', hue = 'phi_D', data = truncated_geom_df)
    # truncated_plot.set_xticklabels(truncated_plot.get_xticklabels(), fontsize='small', rotation = -45)
    # plt.title('Geometric mechanism over neighboring data sets')
    # plt.savefig('truncated_geometric_mech_dist.png')
    # plt.clf()