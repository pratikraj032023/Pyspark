# Databricks notebook source
# MAGIC %md
# MAGIC Today's Topic
# MAGIC 1. Count- always note that count function counts NULL values when count is taken for a group. so if you want to avoid null value count, apply count on single column.
# MAGIC
# MAGIC 2. Min
# MAGIC 3. Max
# MAGIC 4. Average

# COMMAND ----------

import pyspark
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

emp_schema = StructType ([
StructField("id",IntegerType(),True),
StructField("name",StringType(),True),
StructField("age",IntegerType(),True),
StructField("salary",IntegerType(),True),
StructField("country",StringType(),True),
StructField("department",StringType(),True)
])

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df.show()

# COMMAND ----------

emp_df.count()
emp_df.select(col("salary")).show( )

# COMMAND ----------

emp_df.select(sum("salary").alias("sum_of_salary"),min("salary").alias("minimum_salary"),avg("salary").cast("int").alias("avg_salary")).show()
