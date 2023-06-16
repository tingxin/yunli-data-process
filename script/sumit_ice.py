spark-submit \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo \
--conf spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
--conf spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
demo.py



pyspark \
--conf spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo \
--conf spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
--conf spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.AwsDataCatalog.lock-impl=org.apache.iceberg.aws.dynamodb.DynamoDbLockManager \
--conf spark.sql.catalog.AwsDataCatalog.lock.table=myGlueLockTable