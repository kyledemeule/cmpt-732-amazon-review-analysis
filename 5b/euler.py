from pyspark import SparkConf, SparkContext
import sys, random

def generate_counts(iterations):
    rand = random.Random()

    total_count = 0
    for i in xrange(0, iterations):
        temp_count, temp_sum = 0, 0
        while temp_sum < 1.0:
            temp_sum += rand.random()
            temp_count += 1
        total_count += temp_count

    return (total_count, iterations)

def combine(a, b):
    return (a[0]+b[0], a[1]+b[1])

def main():
    iterations = int(sys.argv[1])

    conf = SparkConf().setAppName('Euler Estimator')
    sc = SparkContext(conf=conf)

    euler_count, euler_iterations = sc.parallelize([iterations] * 100).map(generate_counts).reduce(combine)
    euler_estimate = float(euler_count) / float(euler_iterations)

    print("\n")
    print(u"Euler Count: %i" % (euler_count))
    print(u"Euler Iterations: %i" % (euler_iterations))
    print(u"Euler Estimate: %f" % (euler_estimate))
    print("\n")

if __name__ == "__main__":
    main()