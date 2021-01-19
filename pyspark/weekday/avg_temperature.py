#!/usr/bin/python
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('bigquery-analytics-avg-temperature') \
  .getOrCreate()

bucket = 'rj-aa-logistics-spark-bucket'
spark.conf.set('temporaryGcsBucket', bucket)

historical = spark.read.format('bigquery') \
  .option('table', 'vehicle_analytics.historical') \
  .load()
historical.createOrReplaceTempView('historical')

avg_temperature = spark.sql(
    'SELECT vehicle_id, date, AVG(temperature) AS avg_temperature FROM historical GROUP BY vehicle_id, date'
)
avg_temperature.show()
avg_temperature.printSchema()

avg_temperature.write.format('bigquery') \
    .option('table', 'vehicle_analytics.avg_temperature') \
    .mode('append') \
    .save()
