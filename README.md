Note on commit history:
I originally had one github repository for all my assignments and project, I spun out the project folder after starting it for a bit so the commit history has assignment and project commits mixed in. I was pretty good with prefixing the commit messages though so all the relevant project ones should start like "PROJECT ...".

## Project Breakdown

The project directory breakdown is like this:
- All the spark jobs are in the main folder.
- All the file to generate graphs for the reports are in the ```draw/``` folder.
- The web crawler for the most-helpful analysis is in the ```spider/``` folder.
- The web application for interacting with the data is in the ```web/``` folder.
- The ```CASSANDRA.md``` has information on cassandra as well as the structure of the database
- Some utility/help files are in ```utils/```

## Dataset

The dataset is relatively large (~23GB) and can't be included in git. It was attained from http://jmcauley.ucsd.edu/data/amazon/. None of those download links work, you must request access from the author.

## How to Run

Without the dataset I'm not sure how you can run my project (other than the web front end). I can maybe make subsets of the dataset available on S3, but you wont really get useful results on a subset. My project was designed around the report with several steps of analysis. I'll go through each of step of the report and detail what jobs/files were used:

### Parquet
The first step was to convert the dataset to parquet. This actually reduced the filesize compared to the originall compressed version, while making the dataset easy to query against. ```save_parquet.py``` handles this. It also defines the schemas. The metadata json was in a little different format (it used single quotes instead of double, which the parquet importer didn't like, and some other stuff), so I use ```utils/process_metadata.py``` to process it first.

### Score Histograms
Generate a histogram of the overall scores of each review, see ```draw/draw_score_histogram.py``` for chart generation. ```parquet_histogram.py``` to generate the data. Result is small so just prints it to screen.

### Aggregate Score Histogram / Star Bins
Calculate the average score per item, so we can generate a histogram. ```parquet_aggregate_histogram.py``` handles this task. It outputs a file (or many files, if we needed) with each item and it's average review score. ```draw/draw_overall_histogram.py``` can take this file and use matplotlib to draw the histogram. This file will have ~9 million lines, which is fine for modern computers. If this needed to scale we could move the bin-counts into the spark file, greatly reducing the amount of output of the job. 

### Helpfulness Crawler
```amz.json``` are the randomly chosen files. Scraper was written with the Scrapy project.Change to the ```spider``` directory, and run like this:
```
scrapy runspider scrapy_most_helpful.py -o amz.json
```
Result and drawing of the chart are in ```draw/helpfulness.py```.

### Calculating Alternative Scores
```balance_rating_normal.py``` will calculate a users score given their rating counts. ```calculate_alternate_scores.py``` is a spark job that will take all the reviews, calculate the alternate score for each user, and then recalculate all the products average score with those weights. We're dealing with a few million products and a few million users, so I output the results as one file for symplicity, but the rest of the code could be adapted to deal with a colletion of files.

```draw/draw_ascore_histogram.py``` and ```draw/draw_ascore_diff_histogram.py``` correspond to the two graphs evaluating the overall difference when using alternate scores.

To analyze categories and brands use the spark jobs ```breakdown_category.py``` and ```breakdown_brand.py```. ```draw/print_stats.py``` is used to analyze the results, which are put in ```draw/draw_category_breakdown.py``` and ```draw_brand_breakdown.py``` to draw the comparison graphs.

### Running the web app.
The web front end is a flask application that connects to a Cassandra cluster. Code is in ```web/```. Code to interact with Cassandra is in ```web/model.py```. You can run the web app with:
```
python application.py
```

See ```CASSANDRA.md``` for info on cassandra. It just assumes cassandra is running locally with default settings

Putting all the data into Cassandra is done with a spark job. It uses the two parquet files and the two files that are generated from ```calculate_alternate_scores.py``` with alternate score sna histogram info.

```
${SPARK_HOME}/bin/spark-submit --packages TargetHolding:pyspark-cassandra:0.2.1 cassandra_upload.py REVIEWS_PARQUET_FILE METADATA_PARQUET_FILE USERS_ASCORES PRODUCT_ASCORES
```