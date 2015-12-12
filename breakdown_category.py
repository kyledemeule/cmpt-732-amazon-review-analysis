from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, ast
from balance_rating_normal import calculate_ascore

#000014357X, 5.000000, 2
#{
#    "reviewerID": "...",
#    "asin": "...",
#    "reviewerName": "...",
#    "helpful": [0, 0],
#    "reviewText": "...",
#    "overall": 5.0,
#    "summary": "...",
#    "unixReviewTime": 1353456000,
#    "reviewTime": "...""
#}

# ascore file
# (asin, original_score, adjusted score, original_histo, adjusted_histo, n)
def process_ascore(string):
    t = ast.literal_eval(string)
    asin = t[0]
    original_score = t[1]
    adjusted_score = t[2]
    n = t[5]
    return (asin, (original_score - adjusted_score, n))

def main():
    metadata_file = sys.argv[1]
    ascore_file = sys.argv[2]
    output = sys.argv[3]
    
    conf = SparkConf().setAppName('Amazon Alternate Score')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sqlContext.read.parquet(metadata_file).registerTempTable('amazon_metadata')
    # get RDD of (asin, category)
    products = sqlContext.sql("""
        SELECT asin, categories
        FROM amazon_metadata""").rdd.flatMap(lambda a: [(a.asin, category) for category in a.categories or []])
    # join rdd with ascores
    ascores = sc.textFile(ascore_file).map(process_ascore)
    joined = products.join(ascores).map(lambda (asin, (category, (diff, n))): (category, asin, diff, n, 1))
    # remove products with < 5 reviews
    over5 = joined.filter(lambda (category, asin, diff, rev_num, prod_num): rev_num >= 5).map(lambda (category, asin, diff, rev_num, prod_num): (category, (diff, prod_num)))
    per_category = over5.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda (category, (diff, num)): (category, diff / float(num), num))
    per_category.sortBy(lambda x: x[2]).repartition(1).saveAsTextFile(output + "actual/")

if __name__ == "__main__":
    main()