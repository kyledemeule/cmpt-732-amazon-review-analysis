# All things Cassandra

Start cassandra:
```
sudo ${CASSANDRA_HOME}/bin/cassandra
```

Cassandra shell:
```
${CASSANDRA_HOME}/bin/cqlsh
```

## Cassandra structure:
Here's the structure of my Cassandra DB.

```
CREATE KEYSPACE "amzdb" WITH REPLICATION = { 'class' : 'SimpleStrategy' , 'replication_factor' :1 };
USE amzdb;

CREATE TABLE reviews (
    reviewerid varchar,
    asin varchar,
    reviewername varchar,
    helpful_pos int,
    helpful_total int,
    reviewtext text,
    overall float,
    summary text,
    unixreviewtime int,
    reviewtime varchar,
    meth2_score float,
    PRIMARY KEY ((asin), meth2_score)
) WITH CLUSTERING ORDER BY (meth2_score DESC);

CREATE INDEX reviews_reviewerid_index ON reviews (reviewerid);

CREATE TABLE reviewers (
    reviewerid varchar,
    reviewername varchar,
    overall_histogram map<int, int>,
    meth2_score float,
    meth2_histogram map<int, int>,
    PRIMARY KEY (reviewerid)
);

CREATE TABLE products (
    asin varchar,
    title text,
    salesrank map<varchar, int>,
    brand text,
    categories list<varchar>,
    overall_score float,
    overall_histogram map<int, int>,
    meth2_score float,
    meth2_histogram map<int, int>,
    PRIMARY KEY (asin)
);
```

## Spark Jobs

Running spark jobs that interact with Cassandra:
```
${SPARK_HOME}/bin/spark-submit --packages TargetHolding:pyspark-cassandra:0.2.1 cassandra.py data/subset.json
```