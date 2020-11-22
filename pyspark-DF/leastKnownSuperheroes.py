from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
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
    .sort(fn.asc('connections'))

# list out all the super heroes with minimum number of connections
minconnections_count = graphcounts.select('connections').first()[0]
obscure_heroes = graphcounts.filter(graphcounts['connections'] == minconnections_count) \
                            .withColumnRenamed('heroId', 'id') \
                            .join(graph_names, 'id', 'left') \
                            .sort(fn.desc('name'))

print(f'number of obscureheroes are {obscure_heroes.count()} with {minconnections_count} connections.')
obscure_heroes.select('name').show(obscure_heroes.count())

spark.stop()

# output of the code
# number of obscureheroes are 19 with 1 connections.
# +--------------------+
# |                name|
# +--------------------+
# |              ZANTOR|
# |           SHARKSKIN|
# |         SEA LEOPARD|
# |                RUNE|
# |         RED WOLF II|
# |              RANDAK|
# |MARVEL BOY/MARTIN BU|
# |MARVEL BOY II/MARTIN|
# |          LUNATIK II|
# |                KULL|
# |JOHNSON, LYNDON BAIN|
# |      GIURESCU, RADU|
# |GERVASE, LADY ALYSSA|
# |              FENRIS|
# |         DEATHCHARGE|
# |       CLUMSY FOULUP|
# |     CALLAHAN, DANNY|
# |              BLARE/|
# |        BERSERKER II|
# +--------------------+
