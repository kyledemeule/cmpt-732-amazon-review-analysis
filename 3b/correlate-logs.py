from pyspark import SparkConf, SparkContext
import sys, operator, re, math

linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('correlate logs')
sc = SparkContext(conf=conf)

def get_nasa_key(s):
    pieces = linere.split(s)
    if len(pieces) == 6:
        host = pieces[1]
        num_bytes = pieces[4]
        return host, (float(num_bytes), float(1))
    else:
        return None
def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

text = sc.textFile(inputs)

requests = text.map(lambda line: get_nasa_key(line)).filter(lambda k: k != None)
summed_requests = requests.reduceByKey(add_tuples)

def get_six(num_records, num_bytes):
    n = 1
    x = num_records
    x_2 = num_records ** 2
    y = num_bytes
    y_2 = num_bytes ** 2
    xy = num_records * num_bytes
    return (n, x, x_2, y, y_2, xy)

the_six = summed_requests.map(lambda row: get_six(row[1][1], row[1][0])).reduce(add_tuples)

def outputable(result_tuple):
    n = result_tuple[0]
    x = result_tuple[1]
    x_2 = result_tuple[2]
    y = result_tuple[3]
    y_2 = result_tuple[4]
    xy = result_tuple[5]

    r = ((n * xy) - (x * y)) / (math.sqrt((n * x_2) - (x **2 )) * math.sqrt((n * y_2) - (y ** 2)))
    r_2 = r ** 2
    return [
        ("n", n),
        ("Sx", x),
        ("Sx2", x_2),
        ("Sy", y),
        ("Sy2", y_2),
        ("Sxy", xy),
        ("r", r),
        ("r2", r_2),
    ]
# this is probably unnecessary, but for consistency in printing
result = sc.parallelize([the_six]).flatMap(outputable)#.map(lambda (w,c): u"%s %d" % (w, c))
result.saveAsTextFile(output)
