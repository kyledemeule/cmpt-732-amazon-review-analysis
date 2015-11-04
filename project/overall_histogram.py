from pyspark import SparkConf, SparkContext
import sys, operator, json

def main():
    inputs = sys.argv[1]
    output = sys.argv[2]
     
    conf = SparkConf().setAppName('Amazon Histogram')
    sc = SparkContext(conf=conf)
     
    text = sc.textFile(inputs)
     
    jsons = text.map(lambda line: json.loads(line))
    tuples = jsons.map(lambda j: (j["overall"], 1))
    combined = tuples.reduceByKey(operator.add).coalesce(1)
    combined.saveAsTextFile(output)

if __name__ == "__main__":
    main()