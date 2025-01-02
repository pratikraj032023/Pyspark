# Databricks notebook source
# MAGIC %md
# MAGIC Partitioning is done when there is a key coloumn present which can be used to create logical grouping
# MAGIC Bucketing is done when there is no key column present which can be used to create logical grouping
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC creating a dataframe by using StructField, StructType

# COMMAND ----------



from pyspark.sql.types import IntegerType,StringType,StructField,StructField
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
# MAGIC creating a partitioned dataframe on country column

# COMMAND ----------

df.write.format("csv")\
.option("path","dbfs:/FileStore/tables/write_dataframe_to_disk/partitioned_df/")\
  .option("header","true")\
    .option("mode","overwrite")\
      .partitionBy("country")\
      .save()

# COMMAND ----------

# MAGIC %md
# MAGIC displaying the contents in each partition and data

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/write_dataframe_to_disk/partitioned_df/country=INDIA/"))

print(dbutils.fs.count("dbfs:/FileStore/tables/write_dataframe_to_disk/partitioned_df/country=INDIA/part-00007-tid-717624420070275772-0c62532c-ad08-4238-af45-5f2072f4c7d1-55-1.c000.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC creating a bucket on the Id column

# COMMAND ----------

df.write.format("csv")\
    .option("header","true")\
        .option("path","dbfs:/FileStore/tables/write_dataframe_to_disk/bucketing_df/")\
            .option("mode","overwrite")\
                .bucketBy(3,"id")\
                    .saveAsTable("Bucket_by_id_table")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/write_dataframe_to_disk/bucketing_df/"))

print(dbutils.fs.head("dbfs:/FileStore/tables/write_dataframe_to_disk/bucketing_df/part-00005-tid-914703089846257277-032a1135-53c8-4753-851a-d005c6c24a17-133-2_00002.c000.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC Using Repartition and Then bucketing to equal number of files created to the number of buckets specfied
# MAGIC

# COMMAND ----------

df.write.format("csv")\
    .option("header","true")\
        .option("path","dbfs:/FileStore/tables/write_dataframe_to_disk/repartitoned_bucketing_df/")\
            .option("mode","overwrite")\
                .save()




# COMMAND ----------

df.repartition(3)
df.createOrReplaceTempView("Repartitoned_table")
df1 =spark.sql("select * from Repartitoned_table where salary>100000 and gender='f' ")
df1.show()


# COMMAND ----------

from pyspark.sql.functions import col
df2= df1.filter(col("salary")>10000)
df2.show()
