# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC We will learn about:
# MAGIC 1. how to find distinct records in a dataframe
# MAGIC 2. how to find and drop duplicates in a dataframe
# MAGIC 3. how to sort the dataframe
# MAGIC

# COMMAND ----------


data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)
     ]

schema= StructType(
  [
  StructField("id",IntegerType(),False),
  StructField("name",StringType(),False),
  StructField("salary",IntegerType(),False),
  StructField("mngr_id",IntegerType(),False),
]
)

manager_df = spark.createDataFrame(data=data,schema=schema)
manager_df.show()

# COMMAND ----------

#1 find distinct records in a dataframe

distinct_manager_df_all_columns =manager_df.distinct().show()
distinct_manager_df_selected_columns= manager_df.select("id","name").distinct().show()


# COMMAND ----------

#2  find and drop duplicates in a dataframe

dropped_duplicates_df_on_all_columns= manager_df.dropDuplicates().show()

dropped_duplicates_df_on_selected_columns= manager_df.dropDuplicates(["id","name","salary"]).show()

# COMMAND ----------

#3 sort the dataframe
sorted_manager_df_ascending= manager_df.sort("salary")
sorted_manager_df_ascending.show()

sorted_manager_df_descending=manager_df.sort(col("salary").desc())
sorted_manager_df_descending.show()


sorted_manager_df_descending_multiple_columns=manager_df.sort(col("salary"),col("id").desc())
sorted_manager_df_descending_multiple_columns.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #4 Leetcode Problem
# MAGIC
# MAGIC Find the names of the customer that are not referred by the customer with id = 2.
# MAGIC
# MAGIC
# MAGIC leet_code_data = [
# MAGIC     (1, 'Will', None),
# MAGIC     (2, 'Jane', None),
# MAGIC     (3, 'Alex', 2),
# MAGIC     (4, 'Bill', None),
# MAGIC     (5, 'Zack', 1),
# MAGIC     (6, 'Mark', 2)
# MAGIC ]
# MAGIC
# MAGIC expected output
# MAGIC +------+
# MAGIC | name |
# MAGIC | Will |
# MAGIC | Jane |
# MAGIC | Bill |
# MAGIC | Zack |
# MAGIC +------+

# COMMAND ----------

leet_code_data = [ (1, 'Will', None), (2, 'Jane', None), (3, 'Alex', 2), (4, 'Bill', None), (5, 'Zack', 1), (6, 'Mark', 2) ]
leet_code_schema = StructType ([
                    StructField("id",IntegerType(),False),
                    StructField("name",StringType(),True),
                    StructField("referee_id",IntegerType(),True)

]
)

leet_code_df= spark.createDataFrame(data=leet_code_data,schema=leet_code_schema)
leet_code_df.show()

# COMMAND ----------

df1=leet_code_df.filter((col("referee_id").isNull()) | (col("referee_id")==1))
df2= df1.select("name")
df2.show()

# COMMAND ----------

leet_code_df.createOrReplaceTempView("tbl")


# COMMAND ----------

spark.sql("""
          select name from tbl where referee_id =1 or referee_id is null
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
