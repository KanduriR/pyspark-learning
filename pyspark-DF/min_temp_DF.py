from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName('minMaxTemp').master('local').getOrCreate()


custom_schema = StructType([StructField('StationID', StringType()),
                            StructField('Date', IntegerType()),
                            StructField('Measure', StringType()),
                            StructField('Value', FloatType())])

data_df = spark.read.csv('../resources/1800.csv', schema=custom_schema).select('StationID', 'Measure', 'Value')
data_df = data_df.filter(data_df.Measure.isin(['TMIN', 'TMAX']))

min_temps = data_df.filter(data_df.Measure == 'TMIN').groupBy('StationID').min('Value').withColumnRenamed('min(Value)', 'Value')
mins = min_temps.withColumn('Measure', lit('TMIN'))
#max_temps = data_df.groupBy('StationID', 'Measure').max('Value').withColumnRenamed('max(Value)', 'Value')


#max_temps.join(min_temps, 'StationID', 'outer').withColumn('Value', max_temps.Value * 0.1)
#temps = max_temps.collect()

# for t in temps:
#    print(f'ID:{t[0]}\tMeasure:{t[1]}\tValue:{t[2]:.2f}')
mins.show()
spark.stop()
