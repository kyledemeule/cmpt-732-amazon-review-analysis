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
 
comment_data = text.map(lambda line: json.loads(line)).cache()
tuples = comment_data.map(lambda j: (j["subreddit"], (1, j["score"])))
positive_subreddit_means = tuples.reduceByKey(lambda a, b: add_pairs(a, b)).map(lambda (s, p): (s, average_pair(p))).filter(lambda (subreddit, mean): mean > 0)
averages_broadcast = sc.broadcast(dict(positive_subreddit_means.collect()))

comment_by_sub = comment_data.map(lambda c: (c['subreddit'], c))

def get_relative_score(subreddit, json, broadcast_averages):
    subreddit_average = broadcast_averages.value[subreddit]
    relative_score = json['score'] / subreddit_average
    return (relative_score, json['author'])

relative_scores = comment_by_sub.map(lambda (subreddit, json): get_relative_score(subreddit, json, averages_broadcast))
printable_relative_scores = relative_scores.sortBy(lambda (score,author): -score).map(lambda (score,author): u"%s %f" % (author, score))

printable_relative_scores.saveAsTextFile(output)
