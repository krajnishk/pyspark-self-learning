#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:45 PM
# __progname    :-  df12_structDataFrame.py


from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.functions import col, struct, when

sc = SparkSession.builder.master("local[1]").appName("df12_structDataFrame").getOrCreate()

data = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1)
]

columns = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

df = sc.createDataFrame(data, columns)
df.show()
"""
    struct: change the structure of exiting dataframe, add a new column
"""

updatedDF = df.withColumn("OtherInfo",
                          struct(col("id").alias("identifier"),
                                 col("gender").alias("gender"),
                                 col("salary").alias("salary"),
                                 when(col("salary").cast(IntegerType()) < 2000, "Low")
                                 .when(col("salary").cast(IntegerType()) < 4000, "Medium")
                                 .otherwise("High").alias("Salary_Grade")
                                 )).drop("id", "gender", "salary")
updatedDF.printSchema()
updatedDF.show(truncate=False)
