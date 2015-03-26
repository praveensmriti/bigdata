import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    sc = SparkContext(appName="Praveen_wordcount")
    lines = sc.textFile(sys.argv[1])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    upper_text = lines.map(lambda y: y.upper()).collect()

    output = counts.collect()
    for (word, count) in output:
        print "%s: %i" % (word, count)

    print "Upper text = {0}".format(upper_text)

    sc.stop()

