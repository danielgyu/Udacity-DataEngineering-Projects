# Project Purpose
  This project aims to make use of AWS Redshift as a Data Warehouse solution. All the raw data resides in a S3 bucket, and their destination is specified in a .cfg file for reference. Data is first copied to two staging tables created in Redshift. Then, necessary data is inserted in to each of the dimension tables and a fact table(songplays) based on the dimension models.

# Flow Design
  Two staging tables are created to act as an intermediary between the data source and data warehouse. `COPY FROM` statement allows for a more efficient batch processing into a columnar database - AWS Redshift. It is also chosen as the data warehouse for future MPP(massively parallel processing). It allows faster query on large datasets. 
Dimension tables are given `DISTKEY` for future filters and joins. `SORTKEY` is given for future order queries.

  `create_tables.py` first drops existing tables related to the project then creates new ones. Then, by executing `etl.py`, data is copied from S3 to the redshift staing tables, then from staging tables to dimension tables. All the necessary query statements can be found in `sql_queries.py`.
