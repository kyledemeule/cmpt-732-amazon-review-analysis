from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys, operator, json

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

    conf = SparkConf().setAppName('Temperature Range SQL')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    raw_temperature_data = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(inputs, schema=weather_schema())
    raw_temperature_data.registerTempTable('raw_temperature_data')

    temp_min = sqlContext.sql("""
        SELECT station, date, value AS min_value 
        FROM raw_temperature_data 
        WHERE observation = 'TMIN' AND QFLAG = ''
        """).cache()
    temp_min.registerTempTable('temp_min')

    temp_max = sqlContext.sql("""
        SELECT station, date, value AS max_value 
        FROM raw_temperature_data 
        WHERE observation = 'TMAX' AND QFLAG = ''
        """).cache()
    temp_max.registerTempTable('temp_max')

    with_range = sqlContext.sql("""
        SELECT temp_min.date AS date, temp_min.station AS station, temp_max.max_value - temp_min.min_value AS range
        FROM temp_min
        INNER JOIN temp_max
        ON temp_min.station = temp_max.station AND temp_min.date = temp_max.date
        """)
    with_range.registerTempTable('with_range')

    max_per_day = sqlContext.sql("""
        SELECT date, Max(range) AS range
        FROM with_range
        GROUP BY date
        ORDER BY date ASC
        """)
    max_per_day.registerTempTable('max_per_day')

    all_max = sqlContext.sql("""
        SELECT with_range.date, with_range.station, with_range.range
        FROM with_range
        INNER JOIN max_per_day
        WHERE max_per_day.range = with_range.range AND max_per_day.date = with_range.date
        ORDER BY with_range.date
        """)

    all_max.rdd.repartition(1).map(lambda (date, station, rrange): u"%s %s %i" % (date, station, rrange)).saveAsTextFile(output)

if __name__ == "__main__":
    main()