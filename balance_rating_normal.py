import math

# amazon rates things on 5 stars
NUMBER_OF_BUCKETS = 5
REVIEW_MEAN = 10
REVIEW_STDEV = 5
IDEAL_DIST = {1: 0.07, 2: 0.24, 3: 0.38, 4: 0.24, 5: 0.07}

def calculate_ascore(ratings):
    ratings_count = sum(ratings.values())
    return calculate_balance(ratings, ratings_count) * calculate_weight(ratings_count)

def calculate_balance(ratings, ratings_count):
    balance = 0.0
    for bucket in ratings:
        allowed = IDEAL_DIST[bucket] * ratings_count
        balance += ratings[bucket] if ratings[bucket] < allowed else round(allowed)
    balance_percent = min(balance / ratings_count, 1.0)
    # the lowest possible value is actually 0.07, so give a result as a percent from 0.07 to 1.0
    balance = (balance_percent - 0.07) / 0.93
    return max(balance, 0.0)

def calculate_weight(ratings_count):
    return 1 / (1 + math.exp((REVIEW_MEAN-ratings_count)/REVIEW_STDEV))
