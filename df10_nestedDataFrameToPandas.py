#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:41 PM
# __progname    :-  df10_nestedDataFrameToPandas.py


from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

sc = SparkSession.builder.master("local[1]").appName("df10_nestedDataFrameToPandas").getOrCreate()

data = [
    (("James", "", "Smith"), "36636", "M", "3000"),
    (("Michael", "Rose", ""), "40288", "M", "4000"),
    (("Robert", "", "Williams"), "42114", "M", "4000"),
    (("Maria", "Anne", "Jones"), "39192", "F", "4000"),
    (("Jen", "Mary", "Brown"), "", "F", "-1")
]

schema = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType(), nullable=False),
        StructField("middlename", StringType(), nullable=True),
        StructField("lastname", StringType(), nullable=False)
    ])),
    StructField("Emp_id", StringType(), nullable=False),
    StructField("Gender", StringType(), nullable=False),
    StructField("Salary", StringType(), nullable=False)
])

df = sc.createDataFrame(data=data, schema=schema)
print(df.printSchema())
print(df.show())

pandasDF = df.toPandas()
print(pandasDF)
