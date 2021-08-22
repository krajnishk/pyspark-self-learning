#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/22/2021 8:14 PM
# __progname    :-  df16_DataFrameCollect.py


from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("df16_DataFrameCollect").getOrCreate()

data = [
    ("India", 7),
    ("China", 3),
    ("Russia", 1),
    ("United State of America", 4),
    ("Canada", 2),
    ("Australia", 6),
    ("Brazil", 5)
]

countryRank = ["Country", "Area_Rank"]

df = sc.createDataFrame(data, countryRank)

print(df.printSchema())
df.show(truncate=False)
print(df.collect())

df1 = df.collect()

df2 = df.select("Country").collect()

print(df2)

for col in df1:
    print(col["Country"] + ", " + str(col["Area_Rank"]))
