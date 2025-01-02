# Databricks notebook source
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField,DateType,StringType,IntegerType,FloatType
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.master("local").appName("Practice").getOrCreate()

# COMMAND ----------

data = [
    (1, "Alice", 25, 50000.0, "HR", date(2020,1,15)),
    (2, "Bob", 30, 60000.0, "Engineering", date(2019,3,12)),
    (3, "Charlie", 28, 70000.0, "Engineering",date (2021,7,23)),
    (4, "David", 35, 75000.0, "Marketing", date(2018,11,9)),
    (5, "Eve", 40, 80000.0, "Engineering", date(2017,5,30)),
    (6, "Frank", 22, 45000.0, "HR", date(2022,6,11)),
    (7, "Grace", 29, 65000.0, "Marketing", date(2021,8,18))
]


schema = StructType(
    [
    StructField("id",IntegerType(),False),
    StructField("name",StringType(),True),
    StructField("age",StringType(),False),
    StructField("salary",FloatType(),False),
    StructField("department",StringType(),False),
    StructField("join_date",DateType(),False)
    ]
)

emp_df= spark.createDataFrame(data=data,schema=schema)
emp_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### *Questions:*
# MAGIC
# MAGIC 1. *Age Categorization*  
# MAGIC    Create a new column age_group that classifies employees into the following age groups:
# MAGIC    - "Young" for age less than 30
# MAGIC    - "Mid-Aged" for age between 30 and 40 (inclusive)
# MAGIC    - "Senior" for age greater than 40
# MAGIC
# MAGIC 2. *Salary Increase*  
# MAGIC    Create a new column salary_increase that increases an employee's salary by 10% if their current salary is less than 60,000. Otherwise, increase by 5%.
# MAGIC
# MAGIC 3. *Department Based Bonus*  
# MAGIC    Create a new column bonus based on the department:
# MAGIC    - HR gets a 5000 bonus
# MAGIC    - Engineering gets a 7000 bonus
# MAGIC    - Marketing gets a 4000 bonus
# MAGIC    - Others get no bonus (0)
# MAGIC
# MAGIC 4. *Years of Service*  
# MAGIC    Create a new column years_of_service that calculates how many years an employee has been working, based on the join_date. Use the current date for the calculation.
# MAGIC
# MAGIC 5. *Salary Comparison*  
# MAGIC    Create a column salary_comparison that compares the salary with 65000:
# MAGIC    - "Above Average" if salary is greater than 65000
# MAGIC    - "Below Average" if salary is less than 65000
# MAGIC    - "Average" if salary is exactly 65000
# MAGIC
# MAGIC 6. *Promotion Eligibility*  
# MAGIC    Create a column promotion_eligibility that checks if an employee's salary is above 70,000 and has been in the company for more than 3 years. Return "Eligible" or "Not Eligible".
# MAGIC
# MAGIC 7. *Employee Status*  
# MAGIC    Create a column employee_status that marks employees as:
# MAGIC    - "Active" if the employee has been with the company for less than 3 years
# MAGIC    - "Veteran" if the employee has been with the company for more than or equal to 3 years
# MAGIC
# MAGIC 8. *Salary Brackets*  
# MAGIC    Create a new column salary_bracket that places employees into different salary brackets:
# MAGIC    - "Low" for salary less than 50,000
# MAGIC    - "Medium" for salary between 50,000 and 75,000
# MAGIC    - "High" for salary greater than 75,000
# MAGIC
# MAGIC 9. *Department and Age-Based Categories*  
# MAGIC    Create a new column category that assigns an employee to the following categories based on their department and age:
# MAGIC    - HR & Young: "HR - Young"
# MAGIC    - Engineering & Mid-Aged: "Engineering - Mid-Aged"
# MAGIC    - Marketing & Senior: "Marketing - Senior"
# MAGIC    - Others: "Other Category"
# MAGIC
# MAGIC 10. *Job Title Assignment*  
# MAGIC     Based on the salary of employees:
# MAGIC     - If salary is less than 55,000, assign "Junior"
# MAGIC     - If salary is between 55,000 and 75,000, assign "Mid-Level"
# MAGIC     - If salary is greater than 75,000, assign "Senior"
# MAGIC

# COMMAND ----------

#1 Age Categorization
df3 = emp_df.withColumn("age_group",
                  when(col("age")<30,"Young")
                  .when((col("age")>=30)& (col("age")<= 40),"Mid-aged")
                    .otherwise("Senior")
)
df3.show()

# COMMAND ----------

#2 Salary Increase

emp_df.withColumn("salary_increase",
                  when(col("salary")<60000,0.10*col("salary"))
                  .otherwise(0.05*col("salary"))                
                  ).show()

# COMMAND ----------

#3 Department Based Bonus
emp_df.withColumn("bonus",
                  when(col("department")=="HR",5000)
                  .when(col("department")=="Engineering",7000)
                  .when(col("department")=="Marketing",4000)
                   .otherwise(0)

                  ).show()

# COMMAND ----------

#4 Years of Service
emp_df.withColumn("years_of_service",
                  (datediff(current_date(),col("join_date"))/365).cast("int")
                  ).show()

# COMMAND ----------

#5 Salary Comparison

emp_df.withColumn("Salary_Comparison",
                  when(col("salary")>65000,"Above Average")
                  .when(col("salary")<65000,"Below Average")
                  .otherwise("Average")                 
                  ).show()

# COMMAND ----------

#6 Promotion Eligibility

df1=emp_df.withColumn("years_of_service",
                  (datediff(current_date(),col("join_date"))/365).cast("int"))
df1.withColumn("promotion_eligibility",
                  when((col("salary")>70000) & (col("years_of_service") >3) ,"Eligible")
                  .otherwise("Not Eligible")
                  )
df1.show()

# COMMAND ----------

#7 Employee_Status

df2= df1.withColumn("employee_status",
                    when(col("years_of_service")>3,"Veteran")
                    .otherwise("Active")                    
                    )

df2.show()


# COMMAND ----------

#8 salary_bracket

emp_df.withColumn("salary_bracket",
                  when(col("salary")<50000,"low")
                  .when((col("salary")>=50000) & (col("salary")<= 75000),"medium")
                  .otherwise("high")
                  ).show()

# COMMAND ----------

#9 agewise_department
##df3.show()

df4= df3.withColumn("agewise_department",
                    concat(col("department"),lit("-"),col("age_group")
                           )
                    )
df4.show()


# COMMAND ----------

#10 Job_Title_Assignment

emp_df.withColumn("Job_Title_Assignment",
                  when(col("salary")<55000,"Junior")
                  .when((col("salary")>=55000) & (col("salary")<=75000),"Mid-Level")
                  .otherwise("senior")
                  ).show()
