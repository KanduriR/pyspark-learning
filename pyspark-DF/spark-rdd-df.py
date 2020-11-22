from pyspark.sql import SparkSession
from pyspark.sql import Row
import csv

# create spark session to initiate driver process
spark = SparkSession.builder.master('local').appName('spark-rdd-DF').getOrCreate()


def mapper(line):
    items = line.split(',')
    return Row(id=int(items[0]), name=items[1], age=int(items[2]), numFriends=int(items[3]))


# read non header CSV into Row RDD with columns listed in the row
lines = spark.sparkContext.textFile("../resources/fakefriends.csv")
people = lines.map(mapper)

# converting row RDD into a dataframe and temporary table view
people_df = spark.createDataFrame(people).cache()
people_df.createTempView('people')

teenagers = spark.sql('SELECT * FROM people WHERE age >= 13 AND age <= 19')

with open('output/teen_users.csv', 'w') as fd:
    writer = csv.writer(fd)

    # write header
    writer.writerow(['id', 'name', 'age', 'numFriends'])
    writer.writerows(teenagers.sort("age").collect())

spark.stop()
