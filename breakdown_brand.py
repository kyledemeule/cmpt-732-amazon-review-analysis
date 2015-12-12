from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, ast
from balance_rating_normal import calculate_ascore

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
        SELECT asin, brand
        FROM amazon_metadata""").rdd.map(lambda a: (a.asin, a.brand))
    # join rdd with ascores
    ascores = sc.textFile(ascore_file).map(process_ascore)
    joined = products.join(ascores).map(lambda (asin, (brand, (diff, n))): (brand, asin, diff, n, 1))
    # remove products with < 5 reviews
    over5 = joined.filter(lambda (brand, asin, diff, rev_num, prod_num): rev_num >= 5).map(lambda (brand, asin, diff, rev_num, prod_num): (brand, (diff, prod_num)))
    per_brand = over5.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda (brand, (diff, num)): (brand, diff / float(num), num))
    per_brand.sortBy(lambda x: x[2]).repartition(1).saveAsTextFile(output + "actual/")

if __name__ == "__main__":
    main()