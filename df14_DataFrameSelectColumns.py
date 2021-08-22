#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/22/2021 7:38 PM
# __progname    :-  df14_DataFrameSelectColumns.py


from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("df14_DataFrameSelectColumns").getOrCreate()

data = [
    ("James", "Kam", "NewYork", 'USA'),
    ("Rog", "Fed", "Zurich", "Switzerland"),
    ("Tom", "Hanks", "London", "UK")
]

columns = ["FirstName", "LastName", "City", "Country"]

df = sc.createDataFrame(data, columns)
df.show()

df.select("FirstName", "LastName").show()

df.select(df.FirstName, df.LastName).show()

df.select(df["FirstName"], df["LastName"]).show()

df.select(*columns).show()

df.select([col for col in df.columns]).show()

df.select("*").show()

df.select(df.columns[:3]).show()

df.select(df.columns[2:4]).show()
