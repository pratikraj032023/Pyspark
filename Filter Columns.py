# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("FilterColumns") \
    .getOrCreate()

data = [
    ("James",None,"M"),
    ("Anna","NY","F"),
    ("Julia",None,None)
  ]

columns = ["name","state","gender"]
df = spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter rows with NULL values in the dataFrame

# COMMAND ----------

from pyspark.sql.functions import col
df.filter("state is NULL").show()
df.filter(col("state").isNull()).show()
df.filter(df.state.isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Columns Transformation in DataFrame
# MAGIC

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
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
df.show()



# COMMAND ----------

df1 = df.select((col("age")+ 9).alias("new_age")).show()

# COMMAND ----------

df2 = df.withColumnRenamed(("age"),("age2")).show()

# COMMAND ----------

df3 = df.withColumn(("pratik_new_country"),lit("Australia")).show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import * 

data = [

("1","Manish",26,75000,"INDIA","m"),
("2","Nikita",23,100000,"USA","f"),
("3","Prita",22,150000,"INDIA","m"),
]

schema = StructType ([
    StructField("id",StringType(),False),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),False),
    StructField("salary",IntegerType(),False),
    StructField("country",StringType(),True),
    StructField("gender",StringType(),True)
    
])
df4 = spark.createDataFrame(data=data,schema=schema)
df4.show()
df4.printSchema()



# COMMAND ----------

df5=df4.withColumn("id",col("id").cast("Integer"))
df5.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Alias
# MAGIC

# COMMAND ----------

df6 =df5.select(col("id").alias("new_id"))
df6.show()
