from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator, time, datetime

import matplotlib
import numpy
import matplotlib.mlab
import matplotlib.pyplot as pyplot


def main():
    inputs = sys.argv[1]
    output = sys.argv[2]
     
    conf = SparkConf().setAppName('Amazon Histogram Parquet')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(inputs)
    result = df.groupBy("asin").avg("overall").select("asin", "avg(overall)").map(lambda a: u"%s, %f" % (a[0], a[1]))
    result.repartition(1).saveAsTextFile(output)

    #pyplot.hist(result.collect())
    #pyplot.savefig('histogram.png')


if __name__ == "__main__":
    main()