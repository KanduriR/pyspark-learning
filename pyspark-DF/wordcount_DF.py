from pyspark.sql import functions as fn
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('wordcountDF')\
                            .master('local')\
                            .getOrCreate()

text_df = spark.read.text('../resources/Book')

# split the lines into words based on the delimiter
words_df = text_df.select(fn.explode(fn.split(text_df.value, pattern='\W+')).alias('words'))
words_df = words_df.where(words_df.words != "").select(fn.lower(words_df.words).alias('words'))

# filter out the common occuring words
commons = ['a', 'an', 'the', 'it', 'in', 'of', 'i', 'by']
filter_words = words_df.filter(~words_df.words.isin(commons))

filter_words.groupBy('words').count().sort('count', ascending=False)\
    .coalesce(1).write.csv('output/word_count', header='true')

spark.stop()
