#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:23 PM
# __progname    :-  df07_createDataFrameColumnClass.py


from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

sc = SparkSession.builder.master("local[1]").appName("df07_createDataFrameColumnClass").getOrCreate()

colObj = lit("sparkbyexamples.com")

data = [
    ("James", 23),
    ("Manish", 34)
]

df = sc.createDataFrame(data).toDF("name.fname", "gender")
df.printSchema()
df.select(df.gender).show()
df.select(df["gender"]).show()
df.select(df["`name.fname`"]).show()
