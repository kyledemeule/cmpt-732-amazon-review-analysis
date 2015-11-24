import math

# amazon rates things on 5 stars
NUMBER_OF_BUCKETS = 5
REVIEW_MEAN = 10
REVIEW_STDEV = 5

def calculate_ascore(ratings):
    ratings_count = sum(ratings.values())
    return calculate_balance(ratings, ratings_count) * calculate_weight(ratings_count)

def calculate_balance(ratings, ratings_count):
    expected_per_bucket = ratings_count / 5.0
    balance = 0.0
    for bucket in ratings:
        balance += ratings[bucket] if ratings[bucket] < expected_per_bucket else expected_per_bucket
    balance_percent = min(balance / ratings_count, 1.0)
    # the lowest possible value is actually 0.2, so give a result as a percent from 0.2 to 1.0
    return (balance_percent - 0.2) / 0.8

def calculate_weight(ratings_count):
    return 1 / (1 + math.exp((REVIEW_MEAN-ratings_count)/REVIEW_STDEV))
