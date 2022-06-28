---
layout: post
title: "Apply schema from an existing dataframe to a new dataframe in spark"
subtitle: "Maintain consistent schema across dataframes while cloud migration"
date: 2022-06-15 11:11:11 -0400
background: "/img/posts/01.jpg"
---

## My use case for maintaining consistent schemas across dataframes

One of the ways we migrate ETL jobs from tools like Informatica/Datastage to pyspark code is by converting every stage/transformation we have in the ETL job to a dataframe in pyspark code. While every stage/transformation in the ETL job has its own unique value, most stages/transformations retain the schema (unless explicitly changed). To replicate this logic in pyspark code becomes tedious as manual intervention is required to ensure consistency of datatypes, scales and precisions while we derive subsequent dataframes from the source dataframes. The notebook shown below addresses the issue by providing a simple function which takes the source dataframe and casts the target dataframe into the same datatype.

## TL;DR

The below notebook is uploaded [here](./notebooks/2022-06-15-consistent_schemas.ipynb) with comments from the blog.

This notebook has been tested on Databricks Community Edition

## Assumptions

I have made the following assumptions, since this is a simple example.

1. Your source dataframe (the one with a defined schema) and the target dataframe have the same column names
2. This is a simple example, you could extend the logic based on your specific requirement

## Imports & setting up initial data frame

Lets set up the dataframes **init_df_with_schema** and **new_df_without_schema** & initialize a dataframe with a specified schema denoted by **init_schema**.
Note that **new_df_without_schema** does not have a specific schema, and the columns in **new_df_without_schema** need to be converted to appropriate datatypes to match the column datatypes in **init_df_with_schema**

```python
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from datetime import datetime

data1 = [
    [1, "John1", "Doe1", datetime.now(), 31.5],
    [2, "John2", "Doe2", datetime.now(), 37.5],
    [3, "John3", "Doe3", datetime.now(), 62.5],
    [4, "John4", "Doe4", datetime.now(), 74.5]
]

data2 = [
    ['1', "Jane1", "Doe1", '10:05:00.00', '31.5'],
    ['2', "Jane2", "Doe2", '13:10:00.12', '37.5'],
    ['3', "Jane3", "Doe3", '18:30:00.30', '62.5'],
    ['4', "Jane4", "Doe4", '21:45:00.44', '74.5']
]

init_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("SnapshotTime", TimestampType(), True),
    StructField("Metric", FloatType(), True),
])

init_df_with_schema = spark.createDataFrame(data=data1, schema=init_schema)
init_df_with_schema.schema

new_df_without_schema = spark.createDataFrame(data2,["id", "FirstName", "LastName", "SnapshotTime", "Metric"])
new_df_without_schema.schema
```

<br>

## Using the Initial DataFrame to cast the columns of the New DataFrame

We would use a simple for loop to iterate through the initial dataframes datatypes and column names and cast the new/target dataframe columns with the appropriate datatypes

```python
from pyspark.sql.functions import col
for metadata in init_df_with_schema.dtypes:
    new_df_without_schema = new_df_without_schema.withColumn(metadata[0], col(metadata[0]).cast(metadata[1]))
```

<br>

## Testing

Compare the schemas using **dataframe.schema**.

```python
if(new_df_without_schema.schema == init_df_with_schema.schema):
    print("Schema matches!")
else:
    print("Schema does not match")
    print(new_df_without_schema.schema)
    print('***************************')
    print(init_df_with_schema.schema)
```

<br>
