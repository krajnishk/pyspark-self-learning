#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:48 PM
# __progname    :-  df13_arrayTypeDataFrame.py


from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType

sc = SparkSession.builder.master("local[1]").appName("df13_arrayTypeDataFrame").getOrCreate()

schema = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType(), nullable=True),
        StructField("middlename", StringType(), nullable=True),
        StructField("lastname", StringType(), nullable=True)
    ])),
    StructField("hobbies", ArrayType(StringType()), nullable=True),
    StructField("properties", MapType(StringType(), StringType()), nullable=True)
])

df = sc.createDataFrame([], schema)
df.printSchema()
