# Adapted from https://spark.apache.org/examples.html
from fiber import Pool
import random


NUM_SAMPLES = int(1e6)


def is_inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1


def main():
    pool = Pool(processes=4)
    pi = 4.0 * sum(pool.map(is_inside, range(0, NUM_SAMPLES))) / NUM_SAMPLES
    print("Pi is roughly {}".format(pi))


if __name__ == '__main__':
    main()
