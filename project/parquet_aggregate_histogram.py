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

    sqlContext.read.parquet(inputs).registerTempTable('amazon_reviews')

    result = sqlContext.sql("""
        SELECT asin, AVG(overall) AS overall, count(*) AS ccount 
        FROM amazon_reviews 
        GROUP BY asin""")

    result.rdd.map(lambda a: u"%s, %f, %i" % (a.asin, a.overall, a.ccount)).repartition(1).saveAsTextFile(output)


if __name__ == "__main__":
    main()