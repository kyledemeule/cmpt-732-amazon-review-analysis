from pyspark import SparkConf, SparkContext
from pyspark.mllib import fpm
import sys, operator
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('itemsets')
sc = SparkContext(conf=conf)

raw_lines = sc.textFile(inputs)
transactions = raw_lines.map(lambda line: map(int, line.strip().split(" ")))

model = fpm.FPGrowth.train(transactions, 0.002)
result = model.freqItemsets()

tuple_results = result.map(lambda freq_itemset: (sorted(freq_itemset.items), freq_itemset.freq))
sorted_result = tuple_results.sortBy(lambda (items, freq): (-freq, items)).take(10000)
sc.parallelize(sorted_result).map(lambda (items,freq): u"%s %i" % (items, freq)).saveAsTextFile(output)

