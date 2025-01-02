# Databricks notebook source
# MAGIC %md
# MAGIC Question
# MAGIC
# MAGIC
# MAGIC Table: Person
# MAGIC
# MAGIC +-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | personId    | int     |
# MAGIC | lastName    | varchar |
# MAGIC | firstName   | varchar |
# MAGIC +-------------+---------+
# MAGIC personId is the primary key (column with unique values) for this table.
# MAGIC This table contains information about the ID of some persons and their first and last names.
# MAGIC  
# MAGIC
# MAGIC Table: Address
# MAGIC
# MAGIC +-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | addressId   | int     |
# MAGIC | personId    | int     |
# MAGIC | city        | varchar |
# MAGIC | state       | varchar |
# MAGIC +-------------+---------+
# MAGIC addressId is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains information about the city and state of one person with ID = PersonId.
# MAGIC  
# MAGIC
# MAGIC Write a solution to report the first name, last name, city, and state of each person in the Person table. If the address of a personId is not present in the Address table, report null instead.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Person table:
# MAGIC +----------+----------+-----------+
# MAGIC | personId | lastName | firstName |
# MAGIC +----------+----------+-----------+
# MAGIC | 1        | Wang     | Allen     |
# MAGIC | 2        | Alice    | Bob       |
# MAGIC +----------+----------+-----------+
# MAGIC Address table:
# MAGIC +-----------+----------+---------------+------------+
# MAGIC | addressId | personId | city          | state      |
# MAGIC +-----------+----------+---------------+------------+
# MAGIC | 1         | 2        | New York City | New York   |
# MAGIC | 2         | 3        | Leetcode      | California |
# MAGIC +-----------+----------+---------------+------------+
# MAGIC Output: 
# MAGIC +-----------+----------+---------------+----------+
# MAGIC | firstName | lastName | city          | state    |
# MAGIC +-----------+----------+---------------+----------+
# MAGIC | Allen     | Wang     | Null          | Null     |
# MAGIC | Bob       | Alice    | New York City | New York |
# MAGIC +-----------+----------+---------------+----------+
# MAGIC Explanation: 
# MAGIC There is no address in the address table for the personId = 1 so we return null in their city and state.
# MAGIC addressId = 1 contains information about the address of personId = 2.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Data for the Person table
person_data = [
    Row(personId=1, lastName="Wang", firstName="Allen"),
    Row(personId=2, lastName="Alice", firstName="Bob")
]

# Data for the Address table
address_data = [
    Row(addressId=1, personId=2, city="New York City", state="New York"),
    Row(addressId=2, personId=3, city="Leetcode", state="California")
]

# Create DataFrames
person_df = spark.createDataFrame(person_data)
address_df = spark.createDataFrame(address_data)

person_df.show()
address_df.show()

# COMMAND ----------

join_df = person_df.join(address_df,person_df["personId"]==address_df["personId"],"left").select(person_df["firstName"],person_df["lastName"],address_df["city"],address_df["state"])
join_df.show()
