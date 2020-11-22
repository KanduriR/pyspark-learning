from pyspark.sql import SparkSession
from pyspark.sql.functions import avg as favg, round
import csv

spark = SparkSession.builder\
    .master('local')\
    .appName('avgFriendsByAge')\
    .getOrCreate()

# no caching since its only aggregation we are doing
friends_df = spark.read.csv('../resources/fakefriends-header.csv', header='true', inferSchema='True', sep=',')

# another option to give schema is via custom schema
# custom_schema = StructType([StructField('UserId',IntegerType()),\
#			      StructField('Name',StringType()),\
#			      StructField('Age',IntegerType()),\
#			      StructField('Numfriends',IntegerType())])
# read with the custom schema
# friends_df = spark.read.csv('../resources/fakefriends-header.csv',header='true', schema=custom_schema, sep=',',header='true')

# print schema
friends_df.printSchema()

# finding the average number of friends per age
avg_num_friends = friends_df.groupby('age').agg(round(favg('friends'), 2).alias('avg_num_friends')).sort('age')

# save the output to a csv file. If they are very big we will have to partition by available size
avg_num_friends.coalesce(1).write.csv(path='output/avg_num_friends', mode='append', header='True')

spark.stop()
