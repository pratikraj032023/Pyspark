# Databricks notebook source
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

emp_data =[(1,'manish',50000,"IT","India"),
(2,'vikash',60000,"Sales","Us"),
(3,'raushan',70000,"marketing","India"),
(4,'mukesh',80000,"IT","Us"),
(5,'pritam',90000,"Sales","India"),
(6,'nikita',45000,"marketing","Us"),
(7,'ragini',55000,"marketing","India"),
(8,'rakesh',100000,"IT","Us"),
(9,'aditya',65000,"IT","India"),
(10,'rahul',50000,"marketing","Us")]

emp_schema = StructType (
  [
  StructField ("id",IntegerType(),False),
  StructField ("name",StringType(),False),
  StructField ("salary",IntegerType(),False),
  StructField ("department",StringType(),False),
  StructField ("country",StringType(),False)
]
)

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df.show()

# COMMAND ----------

#groupBy department + country

emp_df.groupBy(col("department"),col("country"))\
      .agg(sum("salary").alias("total_salary")).show()

# COMMAND ----------

emp_df.createOrReplaceTempView("table")

# COMMAND ----------

spark.sql("""
          select department, country,sum(salary) as total_Salary
          from table
          group By department,country
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
