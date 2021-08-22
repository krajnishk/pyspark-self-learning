#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/22/2021 8:32 PM
# __progname    :-  df17_DataFrameWithColumn.py


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

sc = SparkSession.builder.master("local[1]").appName("df17_DataFrameWithColumn").getOrCreate()

data = [
    ('James', '', 'Smith', '1991-04-01', 'M', 3000),
    ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
    ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
    ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
    ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

df = sc.createDataFrame(data, columns)

print(df.printSchema())
df.show()

df.withColumn("salary", col("salary").cast("Integer")).show()

df.withColumn("SalaryNew", col("salary") * 100).show()

df.withColumn("CopiedColumn", col("salary") * -1).show()

df.withColumn("Country", lit("USA")).show()

df.withColumn("Country", lit("USA")).withColumn("anotherColumn", lit("anotherValue")).show()

df.withColumnRenamed("gender", "sex").show()

df.drop("salary").show()
