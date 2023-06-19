#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession

def run_sql(sql):
    with SparkSession.builder.appName(name)\
    .config('spark.jars', '/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar')\
    .enableHiveSupport().getOrCreate() as spark:
                 spark.sql(sql)

if __name__ == "__main__":
    run_sql()
			