from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.format("jdbc").option("url","jdbc:mysql://<hostiporname>:30980/classicmodels").option("dbtable"
,"customers").option("user","your").option("password", "yourabcpassword").load()
df.write.format("avro").mode("overwrite").save("gs://mysqldataflow/avro/customer/")
df.write.format("json").mode("overwrite").save("gs://mysqldataflow/json/customer/")
