from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator, json

def main():
    inputs = sys.argv[1]
    output = sys.argv[2]

    conf = SparkConf().setAppName('Reddit Averages SQL')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    comments = sqlContext.read.json(inputs)
    averages = comments.select('subreddit', 'score').groupby('subreddit').avg()

    averages.coalesce(1).write.save(output, format='json', mode='overwrite')

if __name__ == "__main__":
    main()