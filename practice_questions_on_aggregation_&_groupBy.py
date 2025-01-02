# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Questions:
# MAGIC
# MAGIC #### 1. *Total Sales for Each Store*
# MAGIC    Write a PySpark query to calculate the total sales (sales column) for each store (store column).
# MAGIC
# MAGIC #### 2. *Average Number of Items Sold per Day for Each Store*
# MAGIC    Write a PySpark query to calculate the average number of items sold (items_sold column) for each store across all dates.
# MAGIC
# MAGIC #### 3. *Number of Records for Each Store*
# MAGIC    Write a PySpark query to count how many records there are for each store.
# MAGIC
# MAGIC #### 4. *Sum of Sales for Store B*
# MAGIC    Write a PySpark query to get the total sales for "Store B" (use the filter or where function).
# MAGIC
# MAGIC #### 5. *Find the Store with the Maximum Sales on a Single Day*
# MAGIC    Write a PySpark query to find the store that had the maximum sales on any single day.
# MAGIC
# MAGIC #### 6. *Average Sales Across All Stores*
# MAGIC    Write a PySpark query to calculate the average sales across all stores.
# MAGIC
# MAGIC #### 7. *Sum of Items Sold for Each Store on October 2, 2024*
# MAGIC    Write a PySpark query to calculate the total number of items sold for each store on October 2, 2024.
# MAGIC
# MAGIC #### 8. *Total Sales and Average Items Sold for Each Store, Grouped by Date*
# MAGIC    Write a PySpark query to group the data by the date column and calculate the total sales and average items sold for each store on each date.
# MAGIC
# MAGIC #### 9. *Total Sales for Each Store on October 1, 2024*
# MAGIC    Write a PySpark query to calculate the total sales for each store specifically on October 1, 2024.
# MAGIC
# MAGIC #### 10. *Stores with More Than 10 Items Sold on Average*
# MAGIC    Write a PySpark query to list stores where the average number of items sold across all dates is greater than 10.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

# Sample data
data = [
    (1, "2024-10-01", 100, 5),
    (2, "2024-10-01", 200, 10),
    (1, "2024-10-02", 150, 7),
    (2, "2024-10-02", 250, 12),
    (3, "2024-10-01", 300, 15),
    (3, "2024-10-02", 350, 17),
    (1, "2024-10-03", 120, 6),
    (2, "2024-10-03", 220, 11),
    (3, "2024-10-03", 330, 16)
]

# Create DataFrame
columns = ["store", "date", "sales", "items_sold"]
df = spark.createDataFrame(data, columns)
df.show()


# COMMAND ----------

##1. Total Sales for Each Store
df1=df.groupBy("store").agg(sum("sales").alias("sum_of_Sales"))
df1.show()

# COMMAND ----------

##2. Average Number of Items Sold per Day for Each Store
df2 = df.groupBy("store")\
        .agg(avg("items_sold").cast("int").alias("avg_no_of_items_sold"))
df2.show()

# COMMAND ----------

##3. Number of Records for Each Store

df3 = df.groupBy("store")\
      .agg(count("*").alias("no_of_records_for_Each_store"))
df3.show()

# COMMAND ----------

## 4. Sum of Sales for Store B

df4 = df.groupBy("store")\
        .agg(sum("sales").alias("sum_of_sales"))\
          .filter(col("store") == 2)
df4.show()

# COMMAND ----------

## 5. Find the Store with the Maximum Sales on a Single Day

df.groupBy("store")\
  .agg(max("sales").alias("maximum_sales_on_a_single_day")).show()

# COMMAND ----------

## 6. Average Sales Across All Stores
df.agg(avg("sales").cast("int").alias("avergae_Sales_across_all_stores")).show()

# COMMAND ----------

## 7. Sum of Items Sold for Each Store on October 2, 2024

df.filter(col("date")== '2024-10-02')\
.groupBy("store")\
  .agg(sum("items_sold")).show()

# COMMAND ----------

## 8. Total Sales and Average Items Sold for Each Store, Grouped by Date

df.groupBy("store","date")\
  .agg(sum("sales").alias("sum_of_sales"),avg("items_sold").alias("Average Items Sold"))\
    .sort(asc("store")).show()
