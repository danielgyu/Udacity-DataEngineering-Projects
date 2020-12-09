# Purpose
In this project, an etl process is designed for our startup 'Sparkify'. It is constructed in order to provide efficient and easier tool for analyzing user data and song data. These two datasets originally reside in JSON formats, and the goal is to extract information from the JSON, extract the necessary fields and load them in an organized format in a PostgreSQL database.

# Schema design
The design follows a star schema, with one fact table and four dimension tables. In the fact table(songplays), it contains details of all the log data that was collected. Various analysis can be done based on different information, and for data related to user, song, artist, and time, there are supporting details in the each of the dimension tables.

## ETL process
`create_tables.py` is ran first. This ensures a clean database work-through by creating a postgres database called 'sparkifydb' using `psycopg2`. Then, it creates the tables specified in the `sql_queries.py` file. These tables will be used to load cleaned data.<br />

`etl.py` is then ran to extract and transform the raw JSON data. All the files in the data folder are collected, then data are injected to the designated tables after being transformed using `pandas` library.<br />

