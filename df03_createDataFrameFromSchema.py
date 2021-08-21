#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 2:06 PM
# __progname    :-  df03_createDataFrameFromSchema.py


from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

sc = SparkSession.builder.master("local[1]").appName("df03_createDataFrameFromSchema").getOrCreate()

data = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1)
]

schema = StructType(
    [
        StructField("Firstname", StringType(), nullable=True),
        StructField("Middlename", StringType(), nullable=True),
        StructField("Lastname", StringType(), nullable=True),
        StructField("EmpID", StringType(), nullable=True),
        StructField("Gender", StringType(), nullable=True),
        StructField("Salary", IntegerType(), nullable=False)
    ]
)

df = sc.createDataFrame(data, schema)
print(df.printSchema())
print(df.count())
print(df.show())
