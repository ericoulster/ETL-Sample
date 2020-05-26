This is an example of a live ETL script I made which transferred bank data packaged in a JSON file into an Azure-based SQL Server
The ETL accounts for omitted tables in the data. It also accounts for what type of form is sent, based on one of 4 configurations.
The service I build this ETL for has since been discontinued, and the credentials/password variables have been scrubbed.
This is provided strictly as a sample of my Data Engineering, and is not to be used without my (Eric Oulster's) expressed permission

Since developing this code, I have learned more about utilizing parallel processing using PySpark or Dask. While I still consider this to be an effective solution for a one-node network (which this solution demanded), I wanted to note that this does not reflect my most recent data engineering work. In comparison to newer approaches I would take, this benchmarks poorly.
