from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import levenshtein
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import sys, os

RECOMMEND_NUM = 10

def datfile(tdir, file):
    return tdir + file + ".dat"

def extract_rating(line):
    user_id, movie_id, rating, timestamp = line.split("::")
    return (int(user_id), int(movie_id), float(rating))

def extract_movies(line):
    movie_id, movie_name, genre = line.split("::")
    return (int(movie_id), movie_name, genre)

def extract_user_rating(line):
    first_space_index = line.find(" ")
    rating = int(line[:first_space_index])
    movie_name = line[first_space_index+1:]
    return (movie_name, rating)

def format_result(seen_movies, top_rec):
    outlines = []
    outlines.append("Hi! Here are my top {} recommendations for you:".format(RECOMMEND_NUM))
    printed = 0
    for (movie_id, movie_name) in top_rec:
        if movie_id not in seen_movies:
            outlines.append(("%s" % (movie_name)).encode("UTF-8"))
            printed += 1
        if printed >= RECOMMEND_NUM:
            break
    outlines.append("https://www.youtube.com/watch?v=t6mTqlhTJTU&t=8")
    return outlines
    
def main():
    tdir = sys.argv[1]
    users_ratings = sys.argv[2]
    output = sys.argv[3]
    rank = int(sys.argv[4])
    iterations = int(sys.argv[5])

    conf = SparkConf().setAppName('Move Recommender')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    movies = sc.textFile(datfile(tdir, "movies")).map(extract_movies).cache()

    user_written_movies = sc.textFile(users_ratings).map(extract_user_rating)

    # cartesian the user provided movie names with all movies
    written_actual_cartesian = user_written_movies.cartesian(movies).map(lambda t: sum(t, ())) # sum flattens
    written_movie_pairs = sqlContext.createDataFrame(written_actual_cartesian, ['written', 'rating', 'm_id', 'm_name', 'm_genre'])
    # calculate levensthein distance for each pair, and the minimum distance
    leve_distance = written_movie_pairs.withColumn("distance", levenshtein('written', 'm_name'))
    min_per_written = leve_distance.groupBy('written').min('distance')
    # based on the min distance find the actual movies
    join_conditions = [leve_distance.written == min_per_written.written, leve_distance.distance == min_per_written['min(distance)']]
    # pick some arbitrary ID for our user, and create there ratings for the movies
    unique_user_id = 66666666
    user_ratings = leve_distance.join(min_per_written, join_conditions).rdd.map(lambda (w, rating, m_id, n, g, d, w2, m_d): (unique_user_id, m_id, rating))

    # because of the intense calculation above, and the small expected size of user_ratings, it is better to convert it to a python object here
    user_ratings = user_ratings.collect()

    # adding the user ratings to the corpus, so the model can understand how this person thinks
    ratings = sc.textFile(datfile(tdir, "ratings")).map(extract_rating).union(sc.parallelize(user_ratings))

    # Explicit vs Implicit model, which is it really? Implicit works much better
    #model = ALS.train(ratings, rank, iterations)
    model = ALS.trainImplicit(ratings, rank, iterations, alpha=1.0)

    # get the ratings for every movie for our user
    unlabeled_movies = movies.map(lambda (m_id, name, genre): (unique_user_id, m_id))
    predictions = model.predictAll(unlabeled_movies).map(lambda (u_id, m_id, proj): (m_id, proj))
    # sort those predictions so the best recommendatons are at the top
    named_predictions = predictions.join(movies).sortBy(lambda (m_id, (proj, name)): proj, ascending=False)

    test_data_movies = set(map(lambda (x, y, z): y, user_ratings))
    # I could do a filter on the RDD, but the number of provided movies is likely small and it's easier to just fetch
    # the top n + n-test movies and filter in memory (if they show up)
    top_rec = named_predictions.map(lambda (m_id, (proj, name)): (m_id, name)).take(RECOMMEND_NUM + len(test_data_movies))

    sc.parallelize(format_result(test_data_movies, top_rec), numSlices=1).saveAsTextFile(output)

if __name__ == "__main__":
    main()