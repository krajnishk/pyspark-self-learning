#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:43 PM
# __progname    :-  df11_showDataFrame.py


from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("df11_showDataFrame").getOrCreate()

data = [
    ("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")
]

columns = ["sl.no", "quote"]

df = sc.createDataFrame(data, columns)
df.show(3)
df.show(n=3, truncate=30, vertical=True)
df.show(n=3, truncate=30)
