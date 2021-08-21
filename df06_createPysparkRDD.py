#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:20 PM
# __progname    :-  df06_createPysparkRDD.py


from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("df06_createPysparkRDD").getOrCreate()

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
rdd = sc.sparkContext.parallelize(dept)

print(rdd.count())

df = rdd.toDF()
print(df.show())
