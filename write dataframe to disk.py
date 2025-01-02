# Databricks notebook source
from pyspark.sql.types import IntegerType,StringType,StructType,StructType
from pyspark.sql.functions import * 

data = [

(1,"Manish",26,75000,"INDIA","m"),
(2,"Nikita",23,100000,"USA","f"),
(3,"Prita",22,150000,"INDIA","m"),
(4,"Prantosh",17,200000,"JAPAN","m"),
(5,"Vikash",31,300000,"USA","m"),
(6,"Rahul",55,300000,"INDIA","m"),
(7,"Raju",67,540000,"USA","m"),
(8,"Praveen",28,70000,"JAPAN","m"),
(9,"Dev",32,150000,"JAPAN","m"),
(10,"Sherin",16,25000,"RUSSIA","f"),
(11,"Ragu",12,35000,"INDIA","f"),
(12,"Sweta",43,200000,"INDIA","f"),
(13,"Raushan",48,650000,"USA","m"),
(14,"mukesh",36,95000,"RUSSIA","m"),
(15,"Prakash",52,750000,"INDIA","m")

]

schema = StructType ([
    StructField("id",IntegerType(),False),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),False),
    StructField("salary",IntegerType(),False),
    StructField("country",StringType(),True),
    StructField("gender",StringType(),True)
    
])
df = spark.createDataFrame(data=data,schema=schema)
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Write to disk
# MAGIC

# COMMAND ----------


#dbutils.fs.ls("dbfs:/FileStore/tables/write_dataframe_to_disk/")
df.write.format("csv")\
.option("header","true")\
.option("mode","overwrite")\
.option("path","dbfs:/FileStore/tables/write_dataframe_to_disk/first_csv_written.csv")\
.save()



# COMMAND ----------

print(dbutils.fs.head('dbfs:/FileStore/tables/write_dataframe_to_disk/part-00003-tid-8227306036402293313-28f3ceb6-acd6-4a2b-841b-14074f405761-19-1-c000.csv'))

# COMMAND ----------


