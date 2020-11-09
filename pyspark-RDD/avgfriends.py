# code to run on a fictitious data to find average number of friends for every age

import csv
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("avgFriendsByAge")
sc = SparkContext(conf=conf)


def ageNumFriendsRetrieval(line):
    list_items = line.split(',')
    # returns (age, num of friends) as a tuple
    return (int(list_items[2]), int(list_items[3]))


lines = sc.textFile("resources/fakefriends.csv")
numFriendsRDD = lines.map(ageNumFriendsRetrieval)


keySumVals = numFriendsRDD.mapValues(lambda x: (x, 1))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))  # (age, (sum(all friends), num of entries))
avgNumFriends = keySumVals.mapValues(lambda x: x[0] / x[1]).collect()

fd = open('output/avgFriends.csv', 'w')
with fd:
    writer = csv.writer(fd)
    writer.writerow(['age', 'avg_num_friends'])
    writer.writerows(map(lambda x: list(x), avgNumFriends))
