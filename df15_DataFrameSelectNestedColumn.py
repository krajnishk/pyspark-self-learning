#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/22/2021 7:57 PM
# __progname    :-  df15_DataFrameSelectNestedColumn.py


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

sc = SparkSession.builder.master("local[1]").appName("df15_DataFrameSelectNestedColumn").getOrCreate()

data = [
    (("Sachin", "Ramesh", "Tendulkar"), "Mumbai", "India", 100),
    (("Brain", "Charles", "Lara"), "Trinidad", "West Indies", 69),
    (("Ricky", "", "Ponting"), "Sydney", "Australia", 68),
    (("Virat", "Kohli", ""), "New Delhi", "India", 72)
]

schema = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Centuries", IntegerType(), True)
])

df2 = sc.createDataFrame(data=data, schema=schema)
print(df2.printSchema())
df2.show(truncate=False)

df2.select("name").show(truncate=False)

df2.select("name.firstname", "name.lastname", "city").show(truncate=False)

df2.select("name.*").show(truncate=False, vertical=True)
