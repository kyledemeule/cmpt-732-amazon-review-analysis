from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator, re, datetime

def get_nasa_keys(s):
    linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    pieces = linere.split(s)
    if len(pieces) == 6:
        host = pieces[1]
        path = pieces[3]
        dt = datetime.datetime.strptime(pieces[2], '%d/%b/%Y:%H:%M:%S')
        num_bytes = float(pieces[4])
        return host, path, dt, num_bytes
    else:
        return None

def main():
    inputs = sys.argv[1]
    output = sys.argv[2]

    conf = SparkConf().setAppName('NASA Log Load SQL')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    requests = sc.textFile(inputs).map(lambda line: get_nasa_keys(line)).filter(lambda k: k != None)
    requests_dataframe = sqlContext.createDataFrame(requests)
    requests_dataframe.coalesce(1).write.format('parquet').save(output)

if __name__ == "__main__":
    main()