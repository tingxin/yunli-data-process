#!/bin/bash
set -ex
sudo aws s3 cp s3://tx-emr/plugin/iceberg/iceberg.properties /etc/trino/conf/catalog/iceberg.properties --region cn-northwest-1