#!/bin/bash

warehouse='s3://tx-emr/icebergdemo'
default_catalog='AwsDataCatalog'

sql="insert into demo2.quick2 values (current_timestamp(), 12.12, 9, '测试中文999', '2023-01','2023-01-01')"

echo $sql

spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.sql.defaultCatalog=$default_catalog \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.$default_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.$default_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
--conf spark.sql.catalog.$default_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.hadoop.hive.$default_catalog.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory \
--conf spark.sql.catalog.$default_catalog.warehouse=$warehouse \
--conf spark.sql.iceberg.handle-timestamp-without-timezone=true \
--conf spark.driver.extraJavaOptions='-Dfile.encoding=UTF-8' \
--conf spark.executor.extraJavaOptions='-Dfile.encoding=UTF-8' \
s3://tx-emr/yunlisql/sparksql.py \
--name demo112  \
--sql $sql