# All things Cassandra

Start cassandra:
```
sudo ${CASSANDRA_HOME}/bin/cassandra
```

Cassandra shell:
```
${CASSANDRA_HOME}/bin/cqlsh
```

FYI cassandra run at ```127.0.0.1:9042``` by default.

Create a keyspace (kind of like a database, mostly used to describe replication):
```
CREATE KEYSPACE "sampledb" WITH REPLICATION = { 'class' : 'SimpleStrategy' , 'replication_factor' :3 };
```
Then use it:
```
USE sampledb;
```

Create column family:
```
CREATE TABLE revs (
reviewerID varchar,
asin varchar,
reviewerName varchar,
helpful_pos int,
helpful_total int,
reviewText text,
overall float,
summary text,
unixReviewTime int,
reviewTime varchar,
PRIMARY KEY (reviewerID, asin));
```

Insert data:
```
INSERT INTO revs (reviewerID, asin, reviewerName, helpful_pos, helpful_total, reviewText, overall, summary, unixReviewTime, reviewTime)
VALUES ('A2SUAM1J3GNN3B', '0000013714', 'J. McDonal', 2, 3, 'Its really good', 5.0, 'Good', 1252800000, '09 13, 2009');
```

Retrieve data:
```
SELECT * FROM revs WHERE reviewerID = 'A2SUAM1J3GNN3B' AND asin = '0000013714';
```
It gets pissy though if you don't use an indexed column. More queries:
```
SELECT * FROM revs WHERE reviewerID IN ('A2SUAM1J3GNN3B', 'A2SUAM1J3GNN3A');
```
or you can allow filtering:
```
SELECT * FROM revs WHERE asin='0000013714' allow filtering;
```

You can also batch insert:
```
BEGIN BATCH
INSERT INTO users (userID, password, name) VALUES ('user2', 'ch@ngem3b', 'second user')
UPDATE users SET password = 'ps22dhds' WHERE userID = 'user2'
INSERT INTO users (userID, password) VALUES ('user3', 'ch@ngem3c')
DELETE name FROM users WHERE userID = 'user2'
INSERT INTO users (userID, password, name) VALUES ('user4', 'ch@ngem3c', 'Andrew')
APPLY BATCH;
```

Update some data (provide entire key):
```
UPDATE revs SET reviewerName='John Doe', helpful_total=4 WHERE reviewerId='A2SUAM1J3GNN3B' AND asin='0000013714';
```

or create a seconday index:
```
CREATE INDEX revs_asin_index ON revs (asin);
```

## Actual Cassandra structure:
```
CREATE KEYSPACE "amzdb" WITH REPLICATION = { 'class' : 'SimpleStrategy' , 'replication_factor' :1 };
USE amzdb;

CREATE TABLE reviews (
reviewerID varchar,
asin varchar,
reviewerName varchar,
helpful_pos int,
helpful_total int,
reviewText text,
overall float,
summary text,
unixReviewTime int,
reviewTime varchar,
meth1_ascore float,
meth2_ascore float,
PRIMARY KEY (reviewerID, asin));

CREATE INDEX reviews_asin_index ON reviews (asin);

CREATE TABLE reviewers (
reviewerID varchar,
reviewerName varchar,
overall_histogram map<int, int>,
meth1_score float,
meth1_histogram map<int, int>,
meth2_score float,
meth2_histogram map<int, int>,
PRIMARY KEY (reviewerID));

CREATE TABLE products (
asin varchar,
title text,
salesRank map<varchar, int>,
brand text,
categories list<varchar>,
overall_score float,
overall_histogram map<int, int>,
meth1_score float,
meth1_histogram map<int, int>,
meth2_score float,
meth2_histogram map<int, int>,
PRIMARY KEY (asin));
```