from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys, operator

def weather_schema():
    return StructType([
        StructField("station", StringType(), True),
        StructField("date", StringType(), True),
        StructField("observation", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("other1", StringType(), True),
        StructField("QFLAG", StringType(), True),
        StructField("other3", StringType(), True),
        StructField("other4", StringType(), True)
    ])

def main():
    inputs = sys.argv[1]
    output = sys.argv[2]

    conf = SparkConf().setAppName('Temperature Range')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    raw_temperature_data = sqlContext.read.format('com.databricks.spark.csv').load(inputs, schema=weather_schema()).where("QFLAG = ''").cache()
    temp_min = raw_temperature_data.where("observation = 'TMIN'").withColumnRenamed('value', 'min_value')
    temp_max = raw_temperature_data.where("observation = 'TMAX'").withColumnRenamed('value', 'max_value')

    joined_stations = temp_min.join(temp_max, ['station', 'date']).withColumn("range", temp_max.max_value - temp_min.min_value)
    ranged_dates = joined_stations.select(joined_stations.station, joined_stations.date, joined_stations.range).cache()
    
    maxes = ranged_dates.groupBy("date").max("range")
    max_stations = ranged_dates.join(maxes, [maxes['max(range)'] == ranged_dates.range, maxes.date == ranged_dates.date]).select(ranged_dates.station, ranged_dates.date, ranged_dates.range).distinct().sort(ranged_dates.date.asc())

    max_stations.rdd.repartition(1).map(lambda (station,date,rrange): u"%s %s %i" % (date, station, rrange)).saveAsTextFile(output)

if __name__ == "__main__":
    main()