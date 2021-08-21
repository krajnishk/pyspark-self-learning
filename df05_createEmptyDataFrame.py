#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:14 PM
# __progname    :-  df05_createEmptyDataFrame.py


from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

sc = SparkSession.builder.master("local[1]").appName("df05_createEmptyDataFrame").getOrCreate()

emptyRDD = sc.sparkContext.emptyRDD()
print(emptyRDD)

rdd = sc.sparkContext.parallelize([])
print(rdd)

schema = StructType(
    [
        StructField("FirstName", StringType(), nullable=True),
        StructField("LastName", StringType(), nullable=True),
        StructField("Salary", StringType(), nullable=True)
    ]
)

df = sc.createDataFrame(emptyRDD, schema)
print(df.printSchema())
print(df.count())

df1 = emptyRDD.toDF(schema)
print(df1.count())

df2 = sc.createDataFrame([], schema)
print(df2.printSchema())

df3 = sc.createDataFrame([], StructType([]))
print(df3.printSchema())
