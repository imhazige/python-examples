from __future__ import print_function

import sys
import os
import log
from operator import add

from pyspark.sql import SparkSession


def workdcount(spark):
    # refer to https://github.com/apache/spark/blob/master/examples/src/main/python/wordcount.py
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, './resources/person.json')
    dataframe = spark.read.text(filename)
    log.info(f'there are {dataframe.count()} rows')
    lines = dataframe.rdd.map(lambda r: r[0])
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
