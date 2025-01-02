# Databricks notebook source
# MAGIC %md
# MAGIC 1. What is Window Function ?
# MAGIC 2. What is Row_number, Rank and Dense_Rank function?
# MAGIC 3. What is the difference between Row_number, Rank and Dense_Rank function?
# MAGIC 4. Calculate top 2 earner from each department.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

emp_schema= ["id","name","salary","dept","gender"]

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Window , sort data by descending by salary and use Row_number, Rank, Rank_over

# COMMAND ----------

window = Window.partitionBy("dept").orderBy(desc("salary"))
emp_df.withColumn("Total_Salary_row_number",row_number().over(window))\
       .withColumn("Total_Salary_rank",rank().over(window))\
      .withColumn("Total_Salary_dense_rank",dense_rank().over(window)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create a window,sort data by descending by salary and filter the top 2 salary by every department
# MAGIC Use Dense_Rank
# MAGIC __

# COMMAND ----------

window = Window.partitionBy("dept").orderBy(desc("salary"))
emp_df.withColumn("Total_Salary",dense_rank().over(window))\
       .filter(col("Total_Salary")<=2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Create a window,sort data by descending by salary and filter the top 2 salary by every department
# MAGIC Use Row_Number

# COMMAND ----------

window = Window.partitionBy("dept").orderBy(desc("salary"))
emp_df.withColumn("Total_Salary",row_number().over(window))\
      .filter(col("Total_Salary")<=2).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC Create a window,sort data by descending by salary and filter the top 2 salary by every department Use Rank

# COMMAND ----------

window = Window.partitionBy("dept").orderBy(desc("salary"))

emp_df.withColumn("Total_Salary",rank().over(window))\
       .filter(col("Total_Salary")<=2).show()
