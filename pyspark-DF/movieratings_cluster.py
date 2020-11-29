from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.sql.functions as fn

spark = SparkSession.builder.appName('movieStats').getOrCreate()
spark.SparkContext.setLogLevel("ERROR")

# read user ratings
schema = StructType([
                    StructField('userId', StringType()),
                    StructField('movieId', StringType()),
                    StructField('rating', FloatType()),
                    StructField('timeStamp', StringType())])
user_data = spark.read.load('s3://user-ratings-input/ml-latest-small/ratings.csv', format='csv',
                            schema=schema, header='True')

user_data.printSchema()
user_data.show(5)

# read movie titles
titles = spark.read.csv('s3://user-ratings-input/ml-latest-small/movies.csv', "id INT, title STRING", header=True)
# split the release year from titles and broadcast them
movie_title = titles.withColumn('release_year', fn.regexp_extract('title', r'(\(\d{4}\))', 1)) \
    .withColumn('title', fn.regexp_replace('title', r'(\(\d{4}\))', '')) \
    .select('id', 'title', 'release_year') \
    .withColumnRenamed('id', 'movieId') \
    .sort(fn.asc('movieId'))

movie_title.printSchema()
movie_title.show()

# summarize the user_data as <id>,<ratings count>, <avg rating>, <distinct users>
ratings = user_data.select('userId', 'movieId', 'rating') \
    .groupBy('movieId') \
    .agg(fn.count('userId').alias('ratings_count'),
         fn.round(fn.avg('rating'), 2).alias('avg_rating'),
         fn.countDistinct('userId').alias('distinct_viewers')) \
    .sort(fn.asc('movieId')).cache()

final_df = ratings.join(movie_title, 'movieId', 'left') \
    .select('movieId', 'title', 'release_year', 'avg_rating', 'ratings_count', 'distinct_viewers') \
    .coalesce(1)
print('\n\n Final movie stats are : \n', final_df.show())

final_df.write.csv('s3://movie-stats', header=True)

spark.stop()
