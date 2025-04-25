This is an example of a live ETL script I made which transferred bank data packaged in a JSON file into an Azure-based SQL Server.

Since developing this code, I have learned more about utilizing parallel processing using PySpark or Dask. While I still consider this to be an effective solution for a one-node network (which this solution demanded), I wanted to note that this does not reflect my most recent data engineering work. In comparison to newer approaches I would take, this benchmarks poorly.
