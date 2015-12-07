class Model:
    default_length = 10

    @staticmethod
    def get_product(asin):
        return {}

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