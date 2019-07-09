from superQuery import superQuery

client = superQuery.Client()
#----------------------------------------------------
# Select a project, otherwise the default is used
#----------------------------------------------------
# client.set_project("yourproject")

#----------------------------------------------------
# Select a destination dataset and table if you want
#----------------------------------------------------
# dataset_id = 'DEST_DATASET'
# job_config = superQuery.QueryJobConfig()
# table_ref = client.dataset(dataset_id).table('testPythonDestination')
# job_config.destination = table_ref

QUERY = """SELECT name FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 10"""

#----------------------------------------------------
# Run without job configuration
#----------------------------------------------------
query_job = client.query(QUERY) 

#----------------------------------------------------
# Run with job configuration
#----------------------------------------------------
# query_job = client.query(sql=QUERY, job_config=job_config) 

rows = query_job.result()

print ("---------DATA---------")
#----------------------------------------------------
# Option A: Get data directly as rows of objects
#----------------------------------------------------
for row in rows:
    print(row.name)

#----------------------------------------------------
# Option B: Get data into a pandas dataframe 
#----------------------------------------------------
# import pandas as pd
# df = pd.DataFrame(data=[x.to_dict() for x in rows])

print ("---------STATISTICS---------")
if (not query_job.superParams["isDryRun"]):
    print("Data rows:", query_job.totalRows)
    print("Workflow:", "Query")
    print("Cost: $ %.2f" % round(query_job.superQueryTotalCost, 2))
    print("Savings %:", query_job.saving)
    print("Was cache used?:", query_job.cacheUsed if hasattr(query_job, "cacheUsed") else False)
    print("Cache type:", query_job.cacheType if hasattr(query_job, "cacheUsed") else "None")
    print("DryRun flag: ", query_job.superParams["isDryRun"])
else:
    print("Workflow:", "DryRun")
    print("Potential BQ bytes scanned: ", query_job.bigQueryTotalBytesProcessed)
    print("Potential Data rows:", query_job.totalRows)
    print("DryRun flag: ", query_job.superParams["isDryRun"])