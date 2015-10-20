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
summed_requests = requests.reduceByKey(add_tuples).cache()
n, x, y = summed_requests.map(lambda (k, v): (float(1), v[1], v[0])).reduce(add_tuples)
x_mean = x / n
y_mean = y / n


def get_pieces(v, x_mean, y_mean):
    x = v[1]
    y = v[0]

    top = (x - x_mean) * (y - y_mean)
    bottom_left = (x - x_mean) ** 2
    bottom_right = (y - y_mean) ** 2

    return (top, bottom_left, bottom_right)


top, bottom_left, bottom_right = summed_requests.map(lambda (k, v): get_pieces(v, x_mean, y_mean)).reduce(add_tuples)

r = top / (math.sqrt(bottom_left) * math.sqrt(bottom_right))
r_2 = r ** 2

res = [
    ("n", n),
    ("Sx", x),
    ("x mean", x_mean),
    ("Sy", y),
    ("y mean", y_mean),
    ("r", r),
    ("r2", r_2),
]

# this is probably unnecessary, but for consistency in printing
sc.parallelize(res).saveAsTextFile(output)