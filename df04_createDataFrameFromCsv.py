#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 2:51 PM
# __progname    :-  df04_createDataFrameFromCsv.py


from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("df04_createDataFrameFromCsv").getOrCreate()

df_csv = sc.read.csv("data/myCsv.csv")

print(df_csv.count())
print(df_csv.show(20))

df_text = sc.read.text("data/myText.txt")

print(df_text.count())
print(df_text.show())

df_json = sc.read.json("data/myJson.json")

print(df_json.count())
print(df_json.show())
