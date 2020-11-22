from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.sql.functions as fn

spark = SparkSession.builder.master('local')\
                            .appName('popularMovie').getOrCreate()

schema = StructType([
                    StructField('UserID', StringType(), True),
                    StructField('MovieID', StringType(), True),
                    StructField('Rating', FloatType(), True),
                    StructField('Timestamp', StringType(), True)
                    ])

movie_df = spark.read.csv('../resources/ml-latest-small/ratings.csv', schema=schema, header='true') \
    .select('MovieID', 'Rating')

movie_df.createOrReplaceTempView('movie_tbl')

popular_df = spark.sql("""
                        SELECT MovieID, count(*) AS rating_count, avg(Rating) AS avg_rating
                        FROM movie_tbl
                        GROUP BY MovieID
                        ORDER BY count(*) DESC """)

popular_df.write.csv(path='output/avg_ratings', mode='append', header='True')

print(f'num of partitions for movie_df {movie_df.rdd.getNumPartitions()}')
print(f'num of partitions for popular_df {popular_df.rdd.getNumPartitions()}')
print(f'configuration for shuffle partitions {spark.conf.get("spark.sql.shuffle.parameters")}')

# aggregate_df = movie_df.groupBy('MovieID').agg(fn.count('*').alias('rating_count'),
#                                                fn.avg(movie_df.Rating.cast('float')).alias('avg_rating'))

# aggregate_df.sort(fn.desc('rating_count')).show(10)

# popular_df.select(fn.countDistinct(popular_df.MovieID).alias('movieDistinct')).show()

spark.stop()
