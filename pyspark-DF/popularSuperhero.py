from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as fn

spark = SparkSession.builder.master('local[*]').appName('popularSuperhero').getOrCreate()


graph_df = spark.read.text('../resources/marvel-heroes/Marvel+Graph')

names_schema = StructType([StructField('id', StringType(), True),
                           StructField('name', StringType(), True)])
graph_names = spark.read.csv('../resources/marvel-heroes/Marvel+Names', sep=' ', schema=names_schema)

graphcounts = graph_df.withColumn('heroId', fn.split(graph_df.value, ' ', 2)[0]) \
    .withColumn('connections', fn.size(fn.split(graph_df.value, ' ')) - 1) \
    .groupBy('heroId') \
    .sum('connections') \
    .withColumnRenamed('sum(connections)', 'connections') \
    .sort(fn.desc('connections'))

#mapper = fn.create_map([graph_names.id, graph_names.name])
#graphcounts = graphcounts.withColumn('Name', mapper[fn.col('heroId')])

# we need name of the popular hero
popularhero = graph_names.filter(graph_names['id'] == graphcounts.first()[0]).first()[1]
print(f'Most popular hero is {popularhero} with {graphcounts.first()[1]} of connections')
# output - Most popular hero is CAPTAIN AMERICA with 1937 of connections

spark.stop()
