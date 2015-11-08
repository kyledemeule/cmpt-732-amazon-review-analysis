from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator, time, datetime

import plotly.plotly as py
from plotly.graph_objs import *
import pandas as pd

def main():
    inputs = sys.argv[1]
     
    conf = SparkConf().setAppName('Amazon Histogram Parquet')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(inputs)
    result = df.groupBy("asin").avg("overall")

    data = Data([Histogram(x=result.toPandas()['avg(overall)'])])
    filename= "spark/histogram-" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M:%S')
    py.iplot(data, filename=filename)

if __name__ == "__main__":
    main()