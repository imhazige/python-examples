from __future__ import print_function

import sys
import os
import log
from operator import add
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import LongType

from pyspark.sql import SparkSession


def test1(spark):
    # refer to https://github.com/apache/spark/blob/master/examples/src/main/python/wordcount.py
    dirname = os.path.dirname(__file__)
    # http://spark.apache.org/docs/latest/sql-programming-guide.html#creating-dfs
    filename = os.path.join(dirname, './resources/person.json')
    log.info('load json')
    df = spark.read.json(filename)
    df.show()
    # or
    df = spark.read.load(filename, format="json")
    df.show()
    log.info(f'there are {df.count()} rows')
    # Select everybody, but increment the age by 1
    df.select(df['name'],
              df['age'], df['age'] + 1).show()

    df.createOrReplaceTempView("people")

    sqlDF = spark.sql("SELECT name,age FROM people")
    sqlDF.show()

    # http://spark.apache.org/docs/latest/sql-programming-guide.html#run-sql-on-files-directly
    log.info('sql from file directly')
    sqlDF = spark.sql(f"SELECT name,age FROM json.`{filename}`")
    sqlDF.show()

    # as text
    df = spark.read.text(filename)

    lines = df.rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        log.info(f'{word} -- {count}')

    # now json
    # http: // spark.apache.org/docs/latest/sql-programming-guide.html  # json-datasets
    log.info(f'json again')
    df = spark.read.json(filename)
    df.printSchema()
    df.show()

    # Alternatively, a DataFrame can be created for a JSON dataset represented by
    # an RDD[String] storing one JSON object per string
    sc = spark.sparkContext
    jsonStrings = [
        '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
    otherPeopleRDD = sc.parallelize(jsonStrings)
    otherPeople = spark.read.json(otherPeopleRDD)
    otherPeople.show()


def testJdbc(spark):
    dirname = os.path.dirname(__file__)
    # http://spark.apache.org/docs/latest/sql-programming-guide.html#creating-dfs
    # //load the jdbc driver
    jarname = os.path.join(dirname, './postgresql-42.2.5.jar')
    # set at here did not work
    spark.conf.set('spark.driver.extraClassPath', jarname)
    # spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    # make sure you have a local postgres database testpy and a table sparktest
    #
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/testpy")\
        .option("dbtable", "sparktest") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .load()

    # can not do this
    # jdbcDF = spark.sql(f"SELECT * from sparktest")
    jdbcDF.show()


def testArrowPandas(spark):
    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    # Generate a Pandas DataFrame
    pdf = pd.DataFrame(np.random.rand(100, 3))

    # Create a Spark DataFrame from a Pandas DataFrame using Arrow
    df = spark.createDataFrame(pdf)
    df.show()

    # Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
    result_pdf = df.select("*").toPandas()
    log.info(f'pandas-- {result_pdf}')

    # Scalar test
    # http://spark.apache.org/docs/latest/sql-programming-guide.html#scalar
    def multiply_func(x, y):
        return x * y
    multiply = pandas_udf(multiply_func, returnType=LongType())

    # The function for a pandas_udf should be able to execute with local Pandas data
    x = pd.Series([1, 2, 3])
    log.info(f'multiply_func(x, x) \n x = {x} \n {multiply_func(x, x)}')
    # 0    1
    # 1    4
    # 2    9
    # dtype: int64

    # Create a Spark DataFrame, 'spark' is an existing SparkSession
    df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

    log.info(f'df before {df.show()}')
    # Execute function as a Spark vectorized UDF
    df.select(multiply(col("x"), col("x"))).show()

    # groupmap
    # http://spark.apache.org/docs/latest/sql-programming-guide.html#grouped-map
    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
    def substract_mean(pdf):
        # pdf is a pandas.DataFrame not spark dataframe, can not use show
        v = pdf.v
        log.info(f'grouped {pdf}')
        return pdf.assign(v=v - v.mean())

    df.groupby("id").apply(substract_mean).show()


if __name__ == "__main__":
    # http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
    dirname = os.path.dirname(__file__)
    # http://spark.apache.org/docs/latest/sql-programming-guide.html#creating-dfs
    # //load the jdbc driver
    jarname = os.path.join(dirname, './postgresql-42.2.5.jar')
    spark = SparkSession\
        .builder\
        .appName("SimpleExample1")\
        .config('spark.driver.extraClassPath', jarname)\
        .getOrCreate()

    # test1(spark)
    # testJdbc(spark)
    testArrowPandas(spark)

    spark.stop()
