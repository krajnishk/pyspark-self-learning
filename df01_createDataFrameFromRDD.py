#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 1:52 PM
# __progname    :-  df01_createDataFrameFromRDD.py


from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("df01_createDataFrameFromRDD").getOrCreate()

columns = ["language", "user_count"]
data = [
    ("java", "200000"),
    ("python", "500000"),
    ("c", "300000"),
    ("go", "250000"),
    ("scala", "250000")
]

rdd = sc.sparkContext.parallelize(data)
print(rdd.count())

df1 = rdd.toDF()
print(df1.printSchema())

df2 = rdd.toDF(columns)
print(df2.printSchema())
