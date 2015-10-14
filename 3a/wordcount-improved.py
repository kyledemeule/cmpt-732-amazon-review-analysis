from pyspark import SparkConf, SparkContext
import sys, operator, re, string, unicodedata

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)
 
words = text.flatMap(lambda line: wordsep.split(line))
cleaned_words = words.filter(lambda w: w != '').map(lambda w: w.lower()).map(lambda w:unicodedata.normalize("NFD", w))
word_tuples = cleaned_words.map(lambda w: (w, 1))
 
wordcount = word_tuples.reduceByKey(operator.add).coalesce(1)
 
outdata = wordcount.sortBy(lambda (w,c): w).map(lambda (w,c): u"%s %i" % (w, c))
outdata.saveAsTextFile(output)