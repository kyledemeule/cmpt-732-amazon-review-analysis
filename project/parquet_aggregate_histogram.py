from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator

buckets = [0, 0.24, 0.74, 1.24, 1.74, 2.24, 2.74, 3.24, 3.74, 4.24, 4.74, 5.01]
num_buckets = len(buckets)

def find_bucket(a):
    for index in xrange(0, num_buckets):
        if a < buckets[index]:
            return buckets[index - 1]

def main():
    inputs = sys.argv[1]
    output = sys.argv[2]
     
    conf = SparkConf().setAppName('Amazon Histogram Parquet')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(inputs)
    result = df.groupBy("asin").avg("overall").map(lambda a: (find_bucket(a[1]), 1)).reduceByKey(operator.add)
    result.coalesce(1).sortByKey().saveAsTextFile(output)

if __name__ == "__main__":
    main()