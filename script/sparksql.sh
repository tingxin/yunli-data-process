spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.0\
    --conf spark.sql.defaultCatalog=AwsDataCatalog \
    --conf spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory \
    --conf spark.sql.iceberg.handle-timestamp-without-timezone=true  \
    -f s3://tx-emr/yunlisql/demo.sql