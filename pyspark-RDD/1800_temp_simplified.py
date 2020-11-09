from pyspark import SparkConf, SparkContext
import csv


def get_temp_points(item):
    if item[0][1] == 'TMIN':
        return (item[0], min(item[1]))
    else:
        return (item[0], max(item[1]))


conf = SparkConf().setMaster('local').setAppName("temperature_agg")
sc = SparkContext(conf=conf)

lines = sc.textFile("resources/1800.csv").map(lambda x: x.split(','))
data = lines.filter(lambda x: any(ele for ele in x if ele in ['TMIN', 'TMAX']))

temps = data.map(lambda x: ((x[0], x[2]), float(x[3]) * 0.1))
temp_list = temps.groupByKey().mapValues(list)

min_max_temps = temp_list.map(get_temp_points).collect()
