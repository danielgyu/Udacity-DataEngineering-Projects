aws emr create-cluster --name spark-emr \
--use-default-roles \
--release-label emr-5.21.0 \
--instance-count 4 \
--applications Name=Spark \
--bootstrap-actions Path=s3://udacity-emr/spark_bootstrap.sh \
--ec2-attributes KeyName=udacity-spark \
--instance-type m5.xlarge \

