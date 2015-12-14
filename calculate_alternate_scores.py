from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
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

def count_dict(score, ascore=-1):
    counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    increment = int(ascore * 100) if ascore != -1 else 1
    counts[int(score)] += increment
    return counts

# I'd like to use collections.Counter here, but it's addition exlcudes zero values
def combine_counts(count1, count2):
    for key in count1:
        count1[key] += count2[key]
    return count1

def get_average(counts):
    ratings_count = sum(counts.values())
    if ratings_count == 0.0:
        return 0.0
    total = 0.0
    for score in counts:
        total += score * counts[score]
    return total / ratings_count

def main():
    inputs = sys.argv[1]
    output = sys.argv[2]
     
    conf = SparkConf().setAppName('Amazon Alternate Score')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sqlContext.read.parquet(inputs).registerTempTable('amazon_reviews')

    # convert reviews into (user_id, {counts})
    # reduceByKey userid
    user_reviews = sqlContext.sql("""
        SELECT reviewerID, overall
        FROM amazon_reviews""").rdd.map(lambda a: (a.reviewerID, a.overall))
    #(u'A3AF8FFZAZYNE5', 5.0)
    user_review_counts = user_reviews.map(lambda (u_id, score): (u_id, count_dict(score)))
    #(u'A3AF8FFZAZYNE5', {1: 0, 2: 0, 3: 0, 4: 0, 5: 1})
    aggregate_user_review_counts = user_review_counts.reduceByKey(combine_counts)
    #(u'A019669721M1JHRC4WWW3', {1: 0, 2: 0, 3: 0, 4: 4, 5: 1})
    # calculate user balanced rating
    user_ascores = aggregate_user_review_counts.map(lambda (u_id, scores): (u_id, calculate_ascore(scores), scores)).cache()
    # save user balance rating
    user_ascores.repartition(1).saveAsTextFile(output + "ascores/")
    user_ascores = user_ascores.map(lambda (u_id, score, scores): (u_id, score))

    # join reviews to user balance rating
    all_reviews = sqlContext.sql("""
        SELECT reviewerID, asin, overall
        FROM amazon_reviews""").rdd.map(lambda a: (a.reviewerID, (a.asin, a.overall)))
    joined_reviews = all_reviews.leftOuterJoin(user_ascores).map(lambda (u_id, ((item_id, score), ascore)): (item_id, score, ascore))
    # reduceByKey product_id, calculate normal aggregate review score AND weighted review score
    countable_reviews = joined_reviews.map(lambda (id, score, ascore): (id, (count_dict(score), count_dict(score, ascore=ascore))))
    #(u'B0012TDNAM', {1: 0, 2: 0, 3: 0, 4: 0, 5: 1}, {1: 0, 2: 0, 3: 0, 4: 0, 5: 6})
    reviews_with_counts = countable_reviews.reduceByKey(lambda a, b: (combine_counts(a[0], b[0]), combine_counts(a[1], b[1])))
    # (u'B007R6AEBK', ({1: 0, 2: 0, 3: 0, 4: 0, 5: 1}, {1: 0, 2: 0, 3: 0, 4: 0, 5: 17}))
    product_averages = reviews_with_counts.map(lambda (id, (normal, weighted)): (id, get_average(normal), get_average(weighted), normal, weighted, sum(normal.values())))
    
    # save product with normal and weighted review scores
    product_averages.repartition(1).saveAsTextFile(output + "averages/")

if __name__ == "__main__":
    main()