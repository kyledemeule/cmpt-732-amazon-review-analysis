from pyspark import SparkConf, SparkContext
import sys, random

ITER_PARTITION = 100

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
    iterations = int(sys.argv[1]) / ITER_PARTITION
    output = sys.argv[2]

    conf = SparkConf().setAppName('Euler Estimator')
    sc = SparkContext(conf=conf)

    euler_count, euler_iterations = sc.parallelize([iterations] * ITER_PARTITION).map(generate_counts).reduce(combine)
    euler_estimate = float(euler_count) / float(euler_iterations)

    sc.parallelize([(euler_count, euler_iterations, euler_estimate)]).coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    main()