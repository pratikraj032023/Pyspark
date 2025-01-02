# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Input: 
# MAGIC Employees table:
# MAGIC +----+----------+
# MAGIC | id | name     |
# MAGIC +----+----------+
# MAGIC | 1  | Alice    |
# MAGIC | 7  | Bob      |
# MAGIC | 11 | Meir     |
# MAGIC | 90 | Winston  |
# MAGIC | 3  | Jonathan |
# MAGIC +----+----------+
# MAGIC
# MAGIC
# MAGIC EmployeeUNI table:
# MAGIC +----+-----------+
# MAGIC | id | unique_id |
# MAGIC +----+-----------+
# MAGIC | 3  | 1         |
# MAGIC | 11 | 2         |
# MAGIC | 90 | 3         |
# MAGIC +----+-----------+
# MAGIC
# MAGIC Output: 
# MAGIC +-----------+----------+
# MAGIC | unique_id | name     |
# MAGIC +-----------+----------+
# MAGIC | null      | Alice    |
# MAGIC | null      | Bob      |
# MAGIC | 2         | Meir     |
# MAGIC | 3         | Winston  |
# MAGIC | 1         | Jonathan |
# MAGIC +-----------+----------+
# MAGIC Explanation: 
# MAGIC Alice and Bob do not have a unique ID, We will show null instead.
# MAGIC The unique ID of Meir is 2.
# MAGIC The unique ID of Winston is 3.
# MAGIC The unique ID of Jonathan is 1.
# MAGIC

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *



# COMMAND ----------

emp_data = ([(1,"Alice"),(7,"Bob"),(11,"Meir"),(90,"Winston"),(3,"Jonathon")])
emp_schema = ("id","name")
emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df.show()
emp_uni_data = ([(3,1),(11,2),(90,3)])
emp_uni_schema = ("id","uniqueId")
emp_uni_df = spark.createDataFrame(data=emp_uni_data,schema=emp_uni_schema)
emp_uni_df.show()

# COMMAND ----------

emp_df.join(emp_uni_df,emp_df["id"]==emp_uni_df["id"],"left")\
  .drop(emp_df["id"],emp_uni_df["id"])\
    .select(emp_uni_df["uniqueId"],emp_df["name"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL code for the same problem

# COMMAND ----------

emp_df.createOrReplaceTempView("emp_df_tbl")
emp_uni_df.createOrReplaceTempView("emp_uni_df_tbl")


spark.sql("""
          select emp_df_tbl.name, emp_uni_df_tbl.uniqueId from emp_df_tbl
          left join emp_uni_df_tbl on emp_df_tbl.id = emp_uni_df_tbl.id        
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC LeetCode manager with 5 direct reporties 
# MAGIC
# MAGIC Write a solution to find managers with at least five direct reports.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC https://leetcode.com/problems/managers-with-at-least-5-direct-reports/description/?envType=study-plan-v2&envId=top-sql-50
# MAGIC
# MAGIC Input: 
# MAGIC Employee table:
# MAGIC +-----+-------+------------+-----------+
# MAGIC | id  | name  | department | managerId |
# MAGIC +-----+-------+------------+-----------+
# MAGIC | 101 | John  | A          | null      |
# MAGIC | 102 | Dan   | A          | 101       |
# MAGIC | 103 | James | A          | 101       |
# MAGIC | 104 | Amy   | A          | 101       |
# MAGIC | 105 | Anne  | A          | 101       |
# MAGIC | 106 | Ron   | B          | 101       |
# MAGIC +-----+-------+------------+-----------+
# MAGIC Output: 
# MAGIC +------+
# MAGIC | name |
# MAGIC +------+
# MAGIC | John |
# MAGIC +------+

# COMMAND ----------

employee_df_data= [(101,"John","A","null"),(102,"Dan","A",101),(103,"James","A",101),(104,"Amy","A",101),(105,"Anne","A",101),(106,"Ron","B",101)]

employee_df_schema = ["id","name","department","manager_id"]

employee_df = spark.createDataFrame(data=employee_df_data,schema=employee_df_schema)
employee_df.show()


# COMMAND ----------

temp_df = employee_df.alias('emp1')\
  .join(employee_df.alias('emp2'),col("emp1.id")==col("emp2.manager_id"),"inner")\
    .groupBy("emp1.id","emp1.name")\
      .agg(count("emp1.id").alias("num_reports"))
temp_df.show()


manager_with_reports= temp_df.filter(col("num_reports")>=5)
manager_with_reports.select(col("emp1.name")).show()

  
 

# COMMAND ----------

m_reports = employee_df.alias("e1").join(employee_df.alias("e2"), col("e1.id")==col("e2.manager_id"),"inner")
m_reports.show()
temp= m_reports.groupBy("e1.manager_id","e1.name")\
        .agg(count("e1.manager_id").alias("num_reports"))
temp.show()

temp2 = temp1.select(col("e1.name"))
temp2.show()
