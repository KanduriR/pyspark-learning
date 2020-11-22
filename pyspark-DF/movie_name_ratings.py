from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.sql.functions as fn
import csv


def movie_titles_dict(file):
    movie_dict = {}
    with open(file, 'r') as fd:
        reader = csv.DictReader(fd)
        for row in reader:
            movie_dict[row['movieId']] = row['title']
    return movie_dict


spark = SparkSession.builder.master('local[*]').appName('movieRatings').getOrCreate()
schema = StructType([
                    StructField('userID', StringType()),
                    StructField('movieID', StringType()),
                    StructField('rating', FloatType()),
                    StructField('timestamp', StringType())])

data = spark.read.load(path='../resources/ml-latest-small/ratings.csv', format='csv', schema=schema, header='True')
movie_df = data.select('movieID', 'rating') \
    .groupBy('movieID') \
    .agg(fn.count(data.rating).alias('ratings_count'),
         fn.round(fn.avg(data['rating']), 2).alias('avg_rating'))
# create a broadcasted dictionary
titles_bdcast = spark.sparkContext.broadcast(movie_titles_dict('../resources/ml-latest-small/movies.csv'))


# register a udf to lookup values from broadcasted dictionary
def lookupMovieNames(id):
    return titles_bdcast.value[id]


lookupUDF = fn.udf(lookupMovieNames)
movie_df = movie_df.withColumn('Title', lookupUDF(fn.col('movieID')))
movie_df = movie_df.repartition(1)
movie_df.write.csv('output/movie_title_ratings', header=True)

spark.stop()
