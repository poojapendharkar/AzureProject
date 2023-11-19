# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import col, desc, row_number, first
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType, FloatType

# COMMAND ----------


#Instead of keyvault we are using the id and key directly here
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "",
"fs.azure.account.oauth2.client.secret": '',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/code/oauth2/token"}


dbutils.fs.mount(
source = "abfss://airbnbdata@airbnbprojectpoojap.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/airbnbmountdata",
extra_configs = configs)
  
#now we check if mount was successful

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/airbnbmountdata"

# COMMAND ----------

listings = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/airbnbmountdata/raw-data/bostonlistings.csv")
reviews = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/airbnbmountdata/raw-data/bostonreviews.csv")

# COMMAND ----------

listings.show()

# COMMAND ----------

reviews.show()

# COMMAND ----------

reviews.printSchema()

# COMMAND ----------

listings.printSchema()

# COMMAND ----------

columns_to_drop = ["scrape_id", "last_scraped", "source"]

listings = listings.drop(*columns_to_drop)


# COMMAND ----------

reviews = reviews.withColumnRenamed("id", "review_id")


# COMMAND ----------

# Get reviews data sorted with latest reviews shwon on top grouped by listing id
reviews = reviews.withColumn("date", col("date").cast("timestamp"))

window_spec = Window.partitionBy("listing_id").orderBy(col("date").desc())

sorted_reviews = reviews.withColumn("latest_review_number", row_number().over(window_spec))

sorted_reviews.show()

# COMMAND ----------

# add recent review to listings table

latest_reviews = sorted_reviews.filter(col("latest_review_number") == 1)

latest_reviews.show()


# COMMAND ----------

listings.show()

# COMMAND ----------

# add recent review to the listings table
listings = listings.withColumnRenamed("id", "listing_id")

result_df = listings.join(latest_reviews.select("listing_id", "comments").alias("latest_reviews"),
                           on="listing_id",
                           how="left_outer")


result_df = result_df.drop("listing_id")

result_df.show()

# COMMAND ----------

# Load 

sorted_reviews.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/airbnbmountdata/transformed-data/b-sortedreviews") 
result_df.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/airbnbmountdata/transformed-data/b-listings") 


# COMMAND ----------


