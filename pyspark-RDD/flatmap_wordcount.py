from pyspark import SparkConf, SparkContext
import re


def regex_matching(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster('local').setAppName('flatMapWordCount')
sc = SparkContext(conf=conf)

lines = sc.textFile("resources/Book")
flat_rdd = lines.flatMap(regex_matching)
word_freq = flat_rdd.countByValue()

print(f"Number of words {flat_rdd.count()}")

for (key, val) in sorted(word_freq.items(), lambda item: item[1]):
    print(f"{key.encode('ascii',errors='ignore')} : {val}")
