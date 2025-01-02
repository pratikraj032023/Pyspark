# Databricks notebook source
# MAGIC %md
# MAGIC Objective:
# MAGIC 1. How to use Case When in SparkSQL
# MAGIC 2. How to use When Otherwise in Dataframe API
# MAGIC 3. How to deal with Null value
# MAGIC 4. Use of when/otherwise and case/when with multiple AND,OR

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC create new column adult and mark it as Yes -IF age > 18, No IF age <> 18,and If age is NULL, populate a constant value and assign Yes/No as per above conditions.

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

emp_schema = StructType(
    [
        StructField("id",IntegerType(),True),
        StructField("name",StringType(),True),
        StructField("age",IntegerType(),True),
        StructField("salary",IntegerType(),True),
        StructField("country",StringType(),True),
        StructField("dept",StringType(),True),
    ]
)

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df.show()

# COMMAND ----------

emp_df.withColumn("age",when(col("age").isNull(),lit(19)).otherwise(col("age")))\
       .withColumn("adult",when(col("age")>18, "Yes").otherwise("No")).show()

# COMMAND ----------

df1=emp_df.withColumn("age_wise",
            when((col("age")>0) & (col("age") <18), "minor")
           .when((col("age")>18) & (col("age")<30), "mid")
           .otherwise("major"))
df1.show()

# COMMAND ----------

emp_df.createOrReplaceTempView("emp_df_tbl")

# COMMAND ----------

spark.sql("""
          
          select * from emp_df_tbl
          
          """).show()

# COMMAND ----------

spark.sql("""
select *, case 
when age < 18 then 'minor'
when age is null then 'novalue'
else 'major'
end as adult
from emp_df_tbl
""").show()

# COMMAND ----------

spark.sql("""
          
          select *,case
          when salary < 21000 then 'low_salary'
          when salary between 20000 and 40000 then 'mid_salary'
          when salary is null then '5000'
          else 'high_salary'
          end as income_level
          from emp_df_tbl
          """).show()
