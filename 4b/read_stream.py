from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys, datetime, math

# returns a tuple (x, y)
def parse_line(line):
    x, y = map(float, line.split())
    return (x, y, x**2, y**2, x*y, 1.0)

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

def process_rdd(sc, rdd, output):
    values = rdd.map(parse_line).cache()
    x_sum, y_sum, x2_sum, y2_sum, xy_sum, n = values.reduce(add_tuples)
    
    x_mean = x_sum / n
    y_mean = y_sum / n
    xy_mean = xy_sum / n
    x2_mean = x2_sum / n
    y2_mean = y2_sum / n

    #correlation_coefficiant = (xy_mean - x_mean * y_mean) / math.sqrt((x2_mean - x_mean**2) * (y2_mean - y_mean**2))
    correlation_coefficiant = (n * xy_sum - x_sum * y_sum) / (math.sqrt(n * x2_sum - (x_sum**2)) * math.sqrt(n * y2_sum - (y_sum**2)))
    # calculate two standard deviations for the price of one
    x_stdev = values.map(lambda t: t[0]).stdev()
    y_stdev = values.map(lambda t: t[1]).stdev()
    #x_stdev, y_stdev = map(math.sqrt, values.map(lambda (x, y, x2, y2, xy, n): ((x - x_mean)**2, (y - y_mean)**2)).reduce(add_tuples))

    m = correlation_coefficiant * (y_stdev / x_stdev)
    b = y_mean - m * x_mean

    result = sc.parallelize([(m, b)], numSlices=1)
    result.saveAsTextFile(output + '/' + datetime.datetime.now().isoformat().replace(':', '-'))

def main():
    host = sys.argv[1]
    port = int(sys.argv[2])
    output = sys.argv[3]
    batch_length = 5 if len(sys.argv) < 5 else int(sys.argv[4])

    conf = SparkConf().setAppName('Read Stream')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_length)

    ssc.socketTextStream(host, port).foreachRDD(lambda rdd: process_rdd(sc, rdd, output))

    ssc.start()
    ssc.awaitTermination(timeout=300)

if __name__ == "__main__":
    main()