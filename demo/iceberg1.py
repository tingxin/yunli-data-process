import argparse
from pyspark.sql import SparkSession

def run_sql():
    with SparkSession.builder.appName('demo')\
    .config('spark.jars', '/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar')\
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport().getOrCreate() as spark:
        ## Create a DataFrame.
        spark.catalog.setCurrentDatabase("study")
        data = spark.createDataFrame([
        ("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
        ("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
        ("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
        ("103", "2015-01-01", "2015-01-01T13:51:40.519832Z")
        ],["id", "creation_date", "last_update_time"])

        ## Write a DataFrame as a Iceberg dataset to the Amazon S3 location.
        spark.sql("""CREATE TABLE IF NOT EXISTS AwsDataCatalog.study.iceberg_table2 (id string,creation_date string,last_update_time string) USING iceberg location 's3://tx-emr/icebergdemo/study/iceberg_table'""")

        data.writeTo("AwsDataCatalog.study.iceberg_table2").append()


        df = spark.read.format("iceberg").load("AwsDataCatalog.study.iceberg_table2")
        df.show()

if __name__ == "__main__":
    run_sql()