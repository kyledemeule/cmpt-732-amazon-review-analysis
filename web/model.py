from cassandra.cluster import Cluster
import re

class Model:
    default_length = 10
    session = None

    @staticmethod
    def get_session():
        if Model.session == None:
            cluster = Cluster()
            Model.session = cluster.connect('amzdb')
        return Model.session

    @staticmethod
    def get_one(query):
        s = Model.get_session()
        res = s.execute(query)
        res = list(res)
        if len(res) == 1:
            return res[0]
        else:
            return None

    @staticmethod
    def get_list(query):
        s = Model.get_session()
        res = s.execute(query)
        return list(res)

    @staticmethod
    def clean_params(*args):
        return map(re.escape, args)

    @staticmethod
    def get_product(asin):
        [asin] = Model.clean_params(asin)
        query = "SELECT * FROM products WHERE asin = '%s' LIMIT 1" % (asin)
        return Model.get_one(query)

    @staticmethod
    def get_reviewer(reviewer_id):
        [reviewer_id] = Model.clean_params(reviewer_id)
        query = "SELECT * FROM reviewers WHERE reviewerid = '%s' LIMIT 1" % (reviewer_id)
        return Model.get_one(query)

    @staticmethod
    def get_review(reviewer_id, asin):
        [reviewer_id, asin] = Model.clean_params(reviewer_id, asin)
        query = "SELECT * FROM reviews WHERE reviewerid = '%s' AND asin = '%s' LIMIT 1" % (reviewer_id, asin)
        return Model.get_one(query)

    @staticmethod
    def get_reviews(reviewer_id, length=default_length):
        length = int(length)
        [reviewer_id] = Model.clean_params(reviewer_id)
        query = "SELECT * FROM reviews WHERE reviewerid = '%s' LIMIT %i" % (reviewer_id, length)
        return Model.get_list(query)

    @staticmethod
    def get_top_reviews(asin, length=default_length):
        length = int(length)
        [asin] = Model.clean_params(asin)
        # already ordered by score
        query = "SELECT * FROM reviews WHERE asin = '%s' LIMIT %i" % (asin, length)
        return Model.get_list(query)