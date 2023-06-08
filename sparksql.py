import argparse
from pyspark.sql import SparkSession

def run_sql(name, sql):
    with SparkSession.builder.appName(name)\
    .config('spark.sql.catalogImplementation', 'hive')\
    .config('spark.jars', '/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar')\
    .enableHiveSupport().getOrCreate() as spark:
                 spark.sql("show tables").show()
                 spark.sql(sql)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--name', help="Name")
    parser.add_argument(
        '--sql', help="SQL")
    args = parser.parse_args()

    run_sql(args.name, args.sql)
			