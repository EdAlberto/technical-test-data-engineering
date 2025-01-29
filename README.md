# technical-test-data-engineering

This technical test is about retrieve data from an api that has info of spsaceflights, then process with pyspark, generate insights with data and then upload in a realtional (dimentional) model.

Is orchestrated in airflow (just the code set but not tested).

the files are:

- extract_data_api.py (it extracts data via api)
- process_articles.py (it process data with pyspark)
- load_tables.py (it crates the dimentional model an upload the data)
- dag_process.py (orchestrate the flow but not run)
- architecture (there is the architecture diagram in aws of possible solution when this excersise would be deployed)
