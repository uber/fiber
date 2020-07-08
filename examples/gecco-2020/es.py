import fiber
import functools
import numpy as np

solution = np.array([5.0, -5.0, 1.5])


def F(theta):
    return -np.sum(np.square(theta - solution))


def worker(dim, sigma, theta):
    epsilon = np.random.rand(dim)
    return F(theta + sigma * epsilon), epsilon


def es(theta0, worker, workers=40, sigma=0.1, alpha=0.05, iterations=200):
    dim = theta0.shape[0]
    theta = theta0
    pool = fiber.Pool(workers)
    func = functools.partial(worker, dim, sigma)

    for t in range(iterations):
        returns = pool.map(func, [theta] * workers)
        rewards = [ret[0] for ret in returns]
        epsilons = [ret[1] for ret in returns]
        # normalize rewards
        normalized_rewards = (rewards - np.mean(rewards)) / np.std(rewards)
        theta = theta + alpha * 1.0 / (workers * sigma) * sum(
            [reward * epsilon for reward, epsilon in zip(normalized_rewards, epsilons)]
        )
        if t % 10 == 0:
            print(theta)
    return theta


def main():
    theta0 = np.random.rand(3)
    result = es(theta0, worker)
    print("Result", result)


if __name__ == "__main__":
    main()
