# Databricks notebook source
# MAGIC %md
# MAGIC 1. What is the difference beteween Union and Union All?
# MAGIC 2. What will heppen if I change the number of columns while Unioning the data ?
# MAGIC 3. What if column names are different?
# MAGIC 4. What is UnionByName?

# COMMAND ----------

# MAGIC %md
# MAGIC Answers:
# MAGIC
# MAGIC 1. In Dataframe, there is no difference between Union and UnionAll but in Spark.sql Union gives distinct records after removing the duplicate values & UnionAll gives all the values including any duplicates present in the dataframe.
# MAGIC 2. It will throw error while doing Union/UnionAll as it requires same number of columns.
# MAGIC 3. The resulting dataframe will retain the column names of first dataframe, it will ignore the column names from the second dataframe.
# MAGIC 4. UnionByName is used when same column name is present in a scneario where out of order schema is supplied. Although it is not advised to use it for very high number of columns.

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Union") \
    .getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(18 ,'Sam',65000,   17),
(18 ,'Sam',65000,   17)]

schema = StructType([
        StructField("id",IntegerType(),False),
        StructField("name",StringType(),True),
        StructField("salary",IntegerType(),True),
        StructField("mngr_id",IntegerType(),False)

]
)

manager_df=spark.createDataFrame(data=data,schema=schema)

manager_df.show()




# COMMAND ----------

manager_df.count()

# COMMAND ----------

data1=[
        (18 ,'Sam',65000,   17),
         (19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17)]


schema = StructType([
        StructField("id",IntegerType(),False),
        StructField("name",StringType(),True),
        StructField("salary",IntegerType(),True),
        StructField("mngr_id",IntegerType(),False)

]
)

manager_df1=spark.createDataFrame(data=data1,schema=schema)

manager_df1.show()
manager_df1.count()


# COMMAND ----------

# MAGIC %md
# MAGIC Union of manager_df and manager_df1

# COMMAND ----------

manager_df2=manager_df.union(manager_df1)
manager_df2.show()
manager_df2.count()


# COMMAND ----------

# MAGIC %md
# MAGIC Union All of manager_df and manager_df1

# COMMAND ----------

manager_df3= manager_df.unionAll(manager_df1)
manager_df3.show()
manager_df3.count()

# COMMAND ----------

manager_df.createOrReplaceTempView("manager_df_tbl")
manager_df1.createOrReplaceTempView("manager_df_tbl1")

spark.sql("""select * from manager_df_tbl
          union
          select * from manager_df_tbl1
          """).count()


# COMMAND ----------

manager_df.createOrReplaceTempView("manager_df_tbl")
manager_df1.createOrReplaceTempView("manager_df_tbl1")

spark.sql("""select * from manager_df_tbl
          union all
          select * from manager_df_tbl1
          """).count()


# COMMAND ----------

spark.sql("""select * from manager_df_tbl1
          union
          select * from manager_df_tbl1
          """).show()


# COMMAND ----------


wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]

wrong_column_schema= StructType(
    [
        StructField(("id"),IntegerType(),False),
        StructField(("sal"),IntegerType(),False),
        StructField(("mngr_id"),IntegerType(),False),
        StructField(("Name"),StringType(),True)

    ]
)

wrong_manager_df = spark.createDataFrame(data=wrong_column_data,schema=wrong_column_schema)
wrong_manager_df.show()

# COMMAND ----------

manager_df1.union(wrong_manager_df).show()
manager_df1.unionAll(wrong_manager_df).show()
