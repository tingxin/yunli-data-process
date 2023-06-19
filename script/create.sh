aws emr create-cluster 
--applications Name=Hadoop Name=Hive Name=Spark Name=Trino Name=HCatalog Name=Ganglia Name=Tez Name=Zeppelin Name=Phoenix Name=HBase Name=Flink Name=Pig Name=Sqoop 
--tags 'name=demo' 
--ec2-attributes '{"KeyName":"tingxin_dev","AdditionalSlaveSecurityGroups":["sg-0cec8b48cd93b0397"],"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-32aa255b","EmrManagedSlaveSecurityGroup":"sg-0bf2eae38501b4fac","EmrManagedMasterSecurityGroup":"sg-0d1fe3d3562546cf0","AdditionalMasterSecurityGroups":["sg-048ffd9d801d5587a","sg-0cec8b48cd93b0397"]}' 
--release-label emr-6.10.0 --log-uri 's3n://tx-emr/log/' 
--steps '[{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into AwsDataCatalog.study.ice_demo4 select * from AwsDataCatalog.default.order_demo2 limit 100"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into AwsDataCatalog.study.ice_demo4 select * from AwsDataCatalog.default.order_demo2 limit 100"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into AwsDataCatalog.study.test5 select * from AwsDataCatalog.default.order_demo2 limit 100"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into AwsDataCatalog.study.test0 select * from AwsDataCatalog.study.test5 limit 10"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into AwsDataCatalog.study.test0 select * from AwsDataCatalog.study.test5 limit 10"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\n                    CREATE TABLE study.test1 (\n  city string,\n  amount double)\nPARTITIONED BY (city) \nLOCATION '\''s3://tx-emr/icebergdemo/study/test1'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''format'\''='\''parquet'\''\n);\n                    "],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE yunli_db.test_athena_icebert_v1 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY ('\''col_month'\'', '\''col_dt'\'')\nLOCATION '\''s3://yl-bi-datalake/icebergdemo/study/test_athena_icebert_v1'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into yunli_db.test_athena_icebert_v1 values (current_timestamp, 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE study.test_athena_icebert_v1 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY ('\''col_month'\'', '\''col_dt'\'')\nLOCATION '\''s3://yl-bi-datalake/icebergdemo/study/test_athena_icebert_v1'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into yunli_db.test_athena_icebert_v1 values (current_timestamp, 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE study.test_athena_icebert_v1 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY (col_month, col_dt)\nLOCATION '\''s3://yl-bi-datalake/icebergdemo/study/test_athena_icebert_v1'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE study.test_athena_icebert_v1 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY (col_month, col_dt)\nLOCATION '\''s3://tx-emr/icebergdemo/study/test_athena_icebert_v1'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into study.test_athena_icebert_v1 values (current_timestamp, 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into study.test_athena_icebert_v1 values (current_timestamp(), 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into study.test_athena_icebert_v1 values (1686901852404, 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into study.test_athena_icebert_v1 values (CAST(current_timestamp() AS TIMESTAMP), 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into study.test_athena_icebert_v1 values (CAST('\''current_timestamp()'\'' AS TIMESTAMP), 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","insert into study.test_athena_icebert_v1 values (CAST('\''current_timestamp()'\'' AS TIMESTAMP), 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE study.test_athena_icebert_v2 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY (col_month, col_dt)\nLOCATION '\''s3://tx-emr/icebergdemo/study/test_athena_icebert_v2'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE demo2.test_athena_icebert_v2 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY (col_month, col_dt)\nLOCATION '\''s3://tx-emr/icebergdemo/demo2/test_athena_icebert_v2'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\n\"insert into demo2.test_athena_icebert_v2 values (CAST('\''current_timestamp()'\'' AS TIMESTAMP), 12.12, 9, '\''测试9'\'', '\''2023-01'\'','\''2023-01-01'\'')\"\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\n\"insert into demo2.test_athena_icebert_v2 values (CAST('\''current_timestamp()'\'' AS TIMESTAMP), 12.12, 9, '\''9999'\'', '\''2023-01'\'','\''2023-01-01'\'')\"\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\ninsert into demo2.test_athena_icebert_v2 values (CAST('\''current_timestamp()'\'' AS TIMESTAMP), 12.12, 9, '\''9999'\'', '\''2023-01'\'','\''2023-01-01'\'')\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\ninsert into demo2.test_athena_icebert_v2 values (CAST('\''current_timestamp()'\'' AS TIMESTAMP), 12.12, 9, '\''测试中文999'\'', '\''2023-01'\'','\''2023-01-01'\'')\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\ninsert into demo2.test_athena_icebert_v2 values (CAST('\''current_timestamp()'\'' AS TIMESTAMP), 12.12, 9, '\''测试中文999'\'', '\''2023-01'\'','\''2023-01-01'\'')\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE demo2.test_athena_icebert_v3 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY (col_month, col_dt)\nLOCATION '\''s3://tx-emr/icebergdemo/demo2/test_athena_icebert_v2'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\'',\n  '\''write.metadataLocation'\'' = '\''s3://tx-emr/iceberg/metadata/'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE demo2.test_athena_icebert_v4 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY (col_month, col_dt)\nLOCATION '\''s3://tx-emr/icebergdemo/demo2/test_athena_icebert_v4'\''\nUSING iceberg\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\'',\n  '\''write.metadataLocation'\'' = '\''s3://tx-emr/iceberg/metadata/'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE demo2.test_athena_icebert_v4 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nPARTITIONED BY (col_month, col_dt)\nUSING iceberg\nLOCATION '\''s3://tx-emr/icebergdemo/demo2/test_athena_icebert_v4'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\'',\n  '\''write.metadataLocation'\'' = '\''s3://tx-emr/iceberg/metadata/'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"},{"Args":["spark-submit","--master","yarn","--deploy-mode","cluster","--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","--conf","spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog","--conf","spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog","--conf","spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO","--conf","spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory","--conf","spark.sql.catalog.AwsDataCatalog.warehouse=s3://tx-emr/icebergdemo","s3://tx-emr/yunlisql/sparksql.py","--name","demo112","--sql","\nCREATE TABLE demo2.test_athena_icebert_v4 (\n  col_timestamp timestamp COMMENT '\'''\'',\n  col_decimal decimal(18, 2) COMMENT '\'''\'',\n  col_int int COMMENT '\'''\'',\n  col_str string COMMENT '\'''\'',\n  col_month string COMMENT '\'''\'',\n  col_dt string COMMENT '\'''\'')\nUSING iceberg\nPARTITIONED BY (col_month, col_dt)\nLOCATION '\''s3://tx-emr/icebergdemo/demo2/test_athena_icebert_v4'\''\nTBLPROPERTIES (\n  '\''table_type'\''='\''iceberg'\'',\n  '\''vacuum_max_snapshot_age_seconds'\''='\''3600000'\'',\n  '\''write.metadataLocation'\'' = '\''s3://tx-emr/iceberg/metadata/'\''\n);\n"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark SQL Job"}]' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Configurations":[{"Classification":"core-site","Properties":{"s3a.endpoint":"s3.cn-northwest-1.amazonaws.com.cn"}},{"Classification":"hive-site","Properties":{"hive.exec.dynamic.partition.mode":"nonstrict"}}],"Name":"Master"},{"InstanceCount":0,"BidPrice":"OnDemandPrice","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"TASK","InstanceType":"m5.xlarge","Name":"Task_m5.xlarge_SPOT_By_Managed_Scaling"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Configurations":[{"Classification":"core-site","Properties":{"s3a.endpoint":"s3.cn-northwest-1.amazonaws.com.cn"}},{"Classification":"hive-site","Properties":{"hive.exec.dynamic.partition.mode":"nonstrict"}}],"Name":"Core"}]' --configurations '[{"Classification":"iceberg-defaults","Properties":{"iceberg.enabled":"true"}},{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' --bootstrap-actions '[{"Path":"s3://tx-emr/plugin/iceberg/boots.sh","Name":"set trino"}]' --ebs-root-volume-size 50 --service-role EMR_DefaultRole --managed-scaling-policy '{"ComputeLimits":{"MaximumOnDemandCapacityUnits":2,"UnitType":"Instances","MaximumCapacityUnits":4,"MinimumCapacityUnits":2,"MaximumCoreCapacityUnits":2}}' --name 'emr4' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region cn-northwest-1