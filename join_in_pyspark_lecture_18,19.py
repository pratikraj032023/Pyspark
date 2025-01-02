# Databricks notebook source
# MAGIC %md
# MAGIC Questions
# MAGIC
# MAGIC 1. How join works?
# MAGIC 2. Why do we need join ?
# MAGIC 3. What to do after joining two tables ?
# MAGIC 4. What if two tables have same column name ?
# MAGIC 5. Join on two or more columns ?
# MAGIC 6. Types of join?

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema=['customer_id','customer_name','address','date_of_joining']


sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema=['customer_id','product_id','quantity','date_of_purchase']


product_data = [(1, 'fanta',20),
(2, 'dew',22),
(5, 'sprite',40),
(7, 'redbull',100),
(12,'mazza',45),
(22,'coke',27),
(25,'limca',21),
(27,'pepsi',14),
(56,'sting',10)]

product_schema=['id','name','price']

customer_df = spark.createDataFrame(data=customer_data,schema=customer_schema)
sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)
product_df = spark.createDataFrame(data=product_data,schema=product_schema)

customer_df.show()
sales_df.show()
product_df.show()

# COMMAND ----------

df1_inner_joined = customer_df.join(sales_df,customer_df["customer_id"]==sales_df["customer_id"],"inner")
df1_inner_joined.show()

# COMMAND ----------

df1_left_joined = customer_df.join(sales_df,customer_df["customer_id"]==sales_df["customer_id"],"left")
df1_left_joined.show()

# COMMAND ----------

df1_right_joined = customer_df.join(sales_df,customer_df["customer_id"]==sales_df["customer_id"],"right")
df1_right_joined.show()

# COMMAND ----------

df1_left_anti_joined = customer_df.join(sales_df,customer_df["customer_id"]==sales_df["customer_id"],"left_anti")
df1_left_anti_joined.show()

# COMMAND ----------

df1_left_semi_joined = customer_df.join(sales_df,customer_df["customer_id"]==sales_df["customer_id"],"left_semi")
df1_left_semi_joined.show()

# COMMAND ----------


