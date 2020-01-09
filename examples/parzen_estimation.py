# Adapted from https://sebastianraschka.com/Articles/2014_multiprocessing.html#kernel-density-estimation-as-benchmarking-function # noqa E501
from fiber import Pool
import numpy as np


def parzen_estimation(x_samples, point_x, h):
    k_n = 0
    for row in x_samples:
        x_i = (point_x - row[:, np.newaxis]) / (h)
        for row in x_i:
            if np.abs(row) > (1 / 2):
                break
        else:  # "completion-else"*
            k_n += 1
    return (h, (k_n / len(x_samples)) / (h**point_x.shape[1]))


def serial(samples, x, widths):
    return [parzen_estimation(samples, x, w) for w in widths]


def multiprocess(processes, samples, x, widths):
    pool = Pool(processes=processes)
    results = [pool.apply_async(parzen_estimation, args=(samples, x, w))
               for w in widths]
    results = [p.get() for p in results]
    results.sort()  # to sort the results by input window width
    return results


def main():
    np.random.seed(123)

    # Generate random 2D-patterns
    mu_vec = np.array([0, 0])
    cov_mat = np.array([[1, 0], [0, 1]])
    x_2Dgauss = np.random.multivariate_normal(mu_vec, cov_mat, 10000)

    widths = np.arange(0.1, 10.3, 0.1)
    point_x = np.array([[0], [0]])
    results = []

    results = multiprocess(4, x_2Dgauss, point_x, widths)

    for r in results:
        print('h = %s, p(x) = %s' % (r[0], r[1]))
    pass


if __name__ == '__main__':
    main()
