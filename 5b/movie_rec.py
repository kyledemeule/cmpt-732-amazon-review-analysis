from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import sys, random

def datfile(tdir, file):
    return tdir + file + ".dat"

def extract_rating(line):
    user_id, movie_id, rating, timestamp = line.split("::")
    return (int(user_id), int(movie_id), float(rating))

def extract_movies(line):
    movie_id, movie_name, genre = line.split("::")
    return int(movie_id)
    
def main():
    tdir = sys.argv[1]
    users_votes = sys.argv[2]
    output = sys.argv[3]

    conf = SparkConf().setAppName('Move Recommender')
    sc = SparkContext(conf=conf)

    unique_user_id = 66666666
    test_data = [
        (unique_user_id, int("0328832"), 10.0), #matrix
        (unique_user_id, int("0076759"), 10.0), #starwars
        (unique_user_id, int("0078748"), 10.0), #alien
        (unique_user_id, int("0196229"), 1.0), #zoolander
        (unique_user_id, int("0116483"), 1.0), #happygilmore
        (unique_user_id, int("0396269"), 1.0) #wedding crashers
    ]

    ratings = sc.textFile(datfile(tdir, "ratings")).map(extract_rating).union(sc.parallelize(test_data))
    model = ALS.train(ratings, 10, 10)

    unlabeled_movies = sc.textFile(datfile(tdir, "movies")).map(extract_movies).map(lambda m_id: (unique_user_id, m_id))

    predictions = model.predictAll(unlabeled_movies).map(lambda r: (r[1], r[2])).sortBy(lambda r: r[1], ascending=False)
    predictions.repartition(1).saveAsTextFile(output)

if __name__ == "__main__":
    main()