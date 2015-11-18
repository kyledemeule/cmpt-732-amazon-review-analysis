from pyspark import SparkConf, SparkContext
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

def print_result(out_dir, seen_movies, top_rec):
    if not os.path.exists(os.path.dirname(out_dir)):
        os.makedirs(os.path.dirname(out_dir))
    with open(out_dir + "result.txt", 'w') as output_file:
        output_file.write("Hi! Here are my top {} recommendations for you:\n".format(RECOMMEND_NUM))
        printed = 0
        for (movie_id, movie_name) in top_rec:
            if movie_id not in seen_movies:
                output_file.write("{}\n".format(movie_name))
                printed += 1
            if printed >= RECOMMEND_NUM:
                break
        output_file.write("{}\n".format("https://www.youtube.com/watch?v=t6mTqlhTJTU&t=8"))
    return True
    
def main():
    tdir = sys.argv[1]
    users_votes = sys.argv[2]
    output = sys.argv[3]

    conf = SparkConf().setAppName('Move Recommender')
    sc = SparkContext(conf=conf)

    unique_user_id = 66666666
    test_data = [
        (unique_user_id, int("0167260"), 10), #return of the king
        (unique_user_id, int("0120737"), 8), #fellowship
        (unique_user_id, int("0109830"), 5), #forrest gump
        (unique_user_id, int("1392190"), 9), #mad max new
        (unique_user_id, int("0079501"), 3) #mad max old
    ]

    test_data2 = [
        (unique_user_id, int("0328832"), 10.0), #matrix
        (unique_user_id, int("0076759"), 10.0), #starwars
        (unique_user_id, int("0078748"), 10.0), #alien
        (unique_user_id, int("0196229"), 1.0), #zoolander
        (unique_user_id, int("0116483"), 1.0), #happygilmore
        (unique_user_id, int("0396269"), 1.0) #wedding crashers
    ]

    ratings = sc.textFile(datfile(tdir, "ratings")).map(extract_rating).union(sc.parallelize(test_data))
    #model = ALS.train(ratings, 1, 10)
    model = ALS.trainImplicit(ratings, 10, 10, alpha=1.0)

    movies = sc.textFile(datfile(tdir, "movies")).map(extract_movies).cache()

    unlabeled_movies = movies.map(lambda (m_id, name, genre): (unique_user_id, m_id))
    predictions = model.predictAll(unlabeled_movies).map(lambda (u_id, m_id, proj): (m_id, proj))

    named_predictions = predictions.join(movies).sortBy(lambda (m_id, (proj, name)): proj, ascending=False)

    test_data_movies = set(map(lambda (x, y, z): y, test_data))
    # I could do a filter on the RDD, but the number of provided movies is likely small and it's easier to just fetch
    # the top n + n-test movies and filter in memory (if they show up)
    top_rec = named_predictions.map(lambda (m_id, (proj, name)): (m_id, name)).take(RECOMMEND_NUM + len(test_data_movies))

    print_result(output, test_data_movies, top_rec)

if __name__ == "__main__":
    main()