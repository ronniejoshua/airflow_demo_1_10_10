#!/usr/bin/python
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('bigquery-analytics-avg-tire-pressure') \
  .getOrCreate()

bucket = 'rj-aa-logistics-spark-bucket'
spark.conf.set('temporaryGcsBucket', bucket)

historical = spark.read.format('bigquery') \
  .option('table', 'vehicle_analytics.historical') \
  .load()
historical.createOrReplaceTempView('historical')

avg_tire_pressure = spark.sql(
    'SELECT vehicle_id, date, AVG(tire_pressure) AS avg_tire_pressure FROM historical GROUP BY vehicle_id, date'
)
avg_tire_pressure.show()
avg_tire_pressure.printSchema()

avg_tire_pressure.write.format('bigquery') \
    .option('table', 'vehicle_analytics.avg_tire_pressure') \
    .mode('append') \
    .save()
