from __future__ import print_function

import sys
import os
import log
from operator import add

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




if __name__ == "__main__":
    # TODO: http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
    dirname = os.path.dirname(__file__)
    # http://spark.apache.org/docs/latest/sql-programming-guide.html#creating-dfs
    # //load the jdbc driver
    jarname = os.path.join(dirname, './postgresql-42.2.5.jar')
    spark = SparkSession\
        .builder\
        .appName("SimpleExample1")\
        .config('spark.driver.extraClassPath',jarname)\
        .getOrCreate()

    # test1(spark)
    testJdbc(spark)

    spark.stop()
