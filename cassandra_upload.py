from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator, json, pyspark_cassandra, numpy, ast
from collections import Counter

def process_review(row):
  result = {}
  result["reviewerid"] = row.reviewerID
  result["asin"] = row.asin
  result["reviewerName"] = row.reviewerName
  result["reviewText"] = row.reviewText
  result["overall"] = row.overall
  result["summary"] = row.summary
  result["unixReviewTime"] = row.unixReviewTime
  result["reviewTime"] = row.reviewTime
  result["helpful_pos"] = row.helpful_positive
  result["helpful_total"] = row.helpful_total
  return result

def fillin_review(json, score):
  json["meth2_score"] = score
  return json

def process_reviewer(row):
  result = {}
  result["reviewerid"] = row.reviewerID
  result["reviewerName"] = row.reviewerName
  return result

def fillin_reviewer(json, score, histo):
  json["meth2_score"] = score
  json["overall_histogram"] = dict([(int(key), int(histo[key])) for key in histo])
  return json

def process_product(row):
  result = {}
  result["asin"] = row.asin
  result["title"] = row.title
  result["brand"] = row.brand
  if type(row.categories) is list:
    result["categories"] = row.categories
  else:
    result["categories"] = []
  return result

def fillin_product(json, orig_score, orig_histo, adjusted_score, adjusted_histo):
  json["overall_score"] = orig_score
  json["overall_histogram"] = dict([(int(key), int(orig_histo[key])) for key in orig_histo])
  json["meth2_score"] = adjusted_score
  json["meth2_histogram"] = dict([(int(key), int(adjusted_histo[key])) for key in adjusted_histo])
  return json

def main():
  reviews_parquet = sys.argv[1]
  metadata_parquet = sys.argv[2]
  users_ascores_file = sys.argv[3]
  products_ascores_file = sys.argv[4]

  conf = SparkConf().setAppName('Amazon Cassandra Injector').setMaster("local").set("spark.cassandra.connection.host", "localhost")
  sc = SparkContext(conf=conf)
  sqlContext = SQLContext(sc)

  sqlContext.read.parquet(reviews_parquet).registerTempTable('amazon_reviews')
  reviews = sqlContext.sql("""SELECT * FROM amazon_reviews""").rdd.cache()
  reviews_by_reviewer = reviews.map(process_review).map(lambda j: (j["reviewerid"], j))
  users_ascores = sc.textFile(users_ascores_file).map(ast.literal_eval).map(lambda (r_id, score, histo): (r_id, (score, histo)))
  reviews_joined = reviews_by_reviewer.join(users_ascores).map(lambda (reviewerid, (j, (score, histo))): fillin_review(j, score))
  # join with meth2_users_ascores. join on reviewerid -> ascore is reviewer ascore
  reviews_joined.saveToCassandra("amzdb", "reviews")

  # reviewers need their alternative score
  reviewers = reviews.map(process_reviewer).map(lambda j: (j["reviewerid"], j))
  # join with meth2_user_ascores. Get ascore and overall_histogram
  reviewers_joined = reviewers.join(users_ascores).map(lambda (reviewerid, (j, (score, histo))): fillin_reviewer(j, score, histo))
  reviewers_joined.saveToCassandra("amzdb", "reviewers")

  # products need their overall score/histogram, and adjuted score/histogram
  sqlContext.read.parquet(metadata_parquet).registerTempTable('amazon_metadata')
  products = sqlContext.sql("""SELECT * FROM amazon_metadata""").rdd.map(process_product).map(lambda j: (j["asin"], j))
  # join with meth2_product_ascores
  products_ascores = sc.textFile(products_ascores_file).map(ast.literal_eval).map(lambda (asin, o_s, a_s, o_h, a_h, n): (asin, (o_s, o_h, a_s, a_h)))
  products_joined = products.join(products_ascores).map(lambda (asin, (j, (o_s, o_h, a_s, a_h))): fillin_product(j, o_s, o_h, a_s, a_h))
  products_joined.saveToCassandra("amzdb", "products")

if __name__ == "__main__":
    main()