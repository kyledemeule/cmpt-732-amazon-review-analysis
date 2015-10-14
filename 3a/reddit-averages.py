from pyspark import SparkConf, SparkContext
import sys, operator, json

def add_pairs(pair1, pair2):
    return (pair1[0] + pair2[0], pair1[1] + pair2[1])
def average_pair(pair):
    if pair[0] == 0:
        return 0
    else:
        return float(pair[1])/float(pair[0])

inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('Reddit Average')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)
 
jsons = text.map(lambda line: json.loads(line))
tuples = jsons.map(lambda j: (j["subreddit"], (1, j["score"])))
combined = tuples.reduceByKey(lambda a, b: add_pairs(a, b)).coalesce(1)
outdata = combined.sortBy(lambda (w,c): w).map(lambda (w,c): json.dumps([w, average_pair(c)]))
outdata.saveAsTextFile(output)
