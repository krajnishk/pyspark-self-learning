#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 2:04 PM
# __progname    :-  df02_createDataFrameUsingColumns.py


from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("df02_createDataFrameUsingColumns").getOrCreate()

data = [
    ("Rajnish", "Kumar"),
    ("Manish", "Kumar"),
    ("Apple", "Fruits"),
    ("cricket", "sports")
]

columns = ["name", "category"]

df = sc.createDataFrame(data).toDF(*columns)
print(df.printSchema())
