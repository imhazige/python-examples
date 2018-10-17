from __future__ import print_function

import sys
import os
import log
from operator import add

from pyspark.sql import SparkSession


def workdcount(spark):
    # refer to https://github.com/apache/spark/blob/master/examples/src/main/python/wordcount.py
    dirname = os.path.dirname(__file__)
    # http://spark.apache.org/docs/latest/sql-programming-guide.html#creating-dfs
    filename = os.path.join(dirname, './resources/person.json')
    df = spark.read.json(filename)
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


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("SimpleExample1")\
        .getOrCreate()

    workdcount(spark)

    spark.stop()
