from pyspark import SparkConf, SparkContext
import csv

conf = SparkConf().setMaster('local').setAppName("temperature_agg")
sc = SparkContext(conf=conf)


def extractData(line):
    fields = line.split(',')
    stationID = fields[0]
    entrykey = fields[2]
    value = (float(fields[3]) * 0.1)  # store it in celsius
    return (stationID, entrykey, value)


lines = sc.textFile("resources/1800.csv")

entries = lines.map(extractData)
stationtemps = entries.filter(lambda x: x[1] in ['TMIN', 'TMAX']).map(lambda x: (x[0], (x[1], x[2])))  # (stationID, (tempkey, value))

max_temp = stationtemps.values().values().max()
min_temp = stationtemps.values().values().min()


def max_seqOp(accumulator, element):
    return (accumulator if accumulator[1] > element[1] else element)


def max_combOp(accu1, accu2):
    return (accu1 if accu1[1] > accu2[1] else accu2)


def min_seqOp(accumulator, element):
    return (accumulator if accumulator[1] < element[1] else element)


def min_combOp(accu1, accu2):
    return (accu1 if accu1[1] < accu2[1] else accu2)


station_max_temps = stationtemps.aggregateByKey(('', min_temp), max_seqOp, max_combOp).sortByKey()
station_min_temps = stationtemps.aggregateByKey(('', max_temp), min_seqOp, min_combOp).sortByKey()

min_max_temps = station_max_temps.zip(station_min_temps).collect()

with open('output/1800_min_max.csv', 'w') as fd:
    writer = csv.writer(fd)
    writer.writerows(map(lambda x: list(list(x)), min_max_temps))
