from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator, json

def main():
    inputs = sys.argv[1]
     
    conf = SparkConf().setAppName('Amazon Histogram Parquet')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(inputs)
    df.groupBy("overall").count().show()

if __name__ == "__main__":
    main()