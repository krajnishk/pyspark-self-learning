#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Authored By   :-  Rajnish.Kumar 
# Created On    :-  8/21/2021 3:30 PM
# __progname    :-  df08_createDataFrameRowClass.py


from pyspark.sql import SparkSession, Row

sc = SparkSession.builder.master("local[1]").appName("df08_createDataFrameRowClass").getOrCreate()

row = Row("James", str(40))
print(row[0] + "," + str(row[1]))
row2 = Row(name="Alice", age=11)
print(row2.name)

Person = Row("name", "age")
p1 = Person("James", 40)
p2 = Person("Alice", 35)
print(p1.name + "," + p2.name)

data = [Row(name="James,,Smith", lang=["Java", "Scala", "C++"], state="CA"),
        Row(name="Michael,Rose,", lang=["Spark", "Java", "C++"], state="NJ"),
        Row(name="Robert,,Williams", lang=["CSharp", "VB"], state="NV")]

# RDD Example 1
rdd = sc.sparkContext.parallelize(data)
collData = rdd.collect()
print(collData)
for row in collData:
    print(row.name + "," + str(row.lang))

# RDD Example 2
Person = Row("name", "lang", "state")
data = [Person("James,,Smith", ["Java", "Scala", "C++"], "CA"),
        Person("Michael,Rose,", ["Spark", "Java", "C++"], "NJ"),
        Person("Robert,,Williams", ["CSharp", "VB"], "NV")]
rdd = sc.sparkContext.parallelize(data)
collData = rdd.collect()
print(collData)
for person in collData:
    print(person.name + "," + str(person.lang))

# DataFrame Example 1
columns = ["name", "languagesAtSchool", "currentState"]
df = sc.createDataFrame(data)
df.printSchema()
df.show()

collData = df.collect()
print(collData)
for row in collData:
    print(row.name + "," + str(row.lang))

# DataFrame Example 2
columns = ["name", "languagesAtSchool", "currentState"]
df = sc.createDataFrame(data).toDF(*columns)
df.printSchema()
