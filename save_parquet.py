from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, MapType
import sys, json

def review_schema():
    return StructType([
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("reviewerName", StringType(), True),
        StructField("helpful", ArrayType(IntegerType()), True),
        StructField("reviewText", StringType(), True),
        StructField("overall", FloatType(), True),
        StructField("summary", StringType(), True),
        StructField("unixReviewTime", IntegerType(), True),
        StructField("reviewTime", StringType(), True)
    ])

def metadata_schema():
    return StructType([
        StructField("asin", StringType(), True),
        StructField("title", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("salesRank", MapType(StringType(), IntegerType()), True),
        StructField("brand", StringType(), True),
        StructField("categories", ArrayType(StringType()), True)
    ])

def parse_helpful(helpful_string):
    return map(int, helpful_string[1:-1].split(", "))

def main():
    mode = sys.argv[1]
    inputs = sys.argv[2]
    output = sys.argv[3]

    conf = SparkConf().setAppName('Save Parquet')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    if mode == "reviews":
        reviews = sqlContext.read.schema(review_schema()).json(inputs)
        reviews_helpful = reviews.withColumn("helpful_positive", reviews.helpful[0]).withColumn("helpful_total", reviews.helpful[1]).drop("helpful")
        reviews_helpful.write.format('parquet').save(output)
    elif mode == "metadata":
        metadatas = sqlContext.read.schema(metadata_schema()).json(inputs)
        metadatas.write.format('parquet').save(output)
    else:
        print "Unknown mode: %s." % (mode)

if __name__ == "__main__":
    main()