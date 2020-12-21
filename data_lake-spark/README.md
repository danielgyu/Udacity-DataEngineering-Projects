# Project Summary
  A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

  As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

  You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Data Flow

  All transforms are done in the data lake by using spark. No concret tables are created. Raw data resides in json format, they're retrieved and processed by using pyspark.sql. After processing is done in pyspark dataframe and sql, the results are writte to a separate S3 bucket in parquet format.

## How to Run

  `etl.py` contains all the necessary steps to Extract from S3, Transform in Spark, and Load to S3 back again. In order to have access to the necessary S3 buckets, AWS credentials are defined in a separate file `dl.cfg`. This file contains the access key and secret key for AWS, and thus it is excluded.
