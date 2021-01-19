#!/usr/bin/python
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('bigquery-analytics-avg-speed') \
  .getOrCreate()

bucket = 'rj-aa-logistics-spark-bucket'
spark.conf.set('temporaryGcsBucket', bucket)

historical = spark.read.format('bigquery') \
  .option('table', 'vehicle_analytics.historical') \
  .load()
historical.createOrReplaceTempView('historical')

avg_speed = spark.sql(
    'SELECT vehicle_id, date, AVG(speed) AS avg_speed FROM historical GROUP BY vehicle_id, date'
)
avg_speed.show()
avg_speed.printSchema()

avg_speed.write.format('bigquery') \
    .option('table', 'vehicle_analytics.avg_speed') \
    .mode('append') \
    .save()
