#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:38 PM
# __progname    :-  df09_CreateDataFrameToPandas.py


from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("df09_CreateDataFrameToPandas").getOrCreate()

data = [
    ("James", "", "Smith", "36636", "M", 60000),
    ("Michael", "Rose", "", "40288", "M", 70000),
    ("Robert", "", "Williams", "42114", "", 400000),
    ("Maria", "Anne", "Jones", "39192", "F", 500000),
    ("Jen", "Mary", "Brown", "", "F", 0)
]

columns = ["firstname", "middlename", "lastname", "emp_id", "Gender", "salary"]

df = sc.createDataFrame(data, columns)

print(df.printSchema())
print(df.show())

pandasDF = df.toPandas()
print(pandasDF)
