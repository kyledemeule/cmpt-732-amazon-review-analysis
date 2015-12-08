from cassandra.cluster import Cluster

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
    def get_product(asin):
        s = Model.get_session()
        res = s.execute("SELECT * FROM products WHERE asin = '%s' LIMIT 1" % (asin))
        res = list(res)
        if len(res) == 1:
            return res[0]
        else:
            return None

    @staticmethod
    def get_reviewer(reviewer_id):
        return {}

    @staticmethod
    def get_review(review_id):
        return {}

    @staticmethod
    def get_reviews(reviewer, page=0, length=default_length):
        return []

    @staticmethod
    def get_top_reviewers(length=default_length):
        return []

    @staticmethod
    def get_top_products(length=default_length):
        return []

    @staticmethod
    def get_top_reviews(asin, length=default_length):
        return []