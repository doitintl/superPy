from superQuery import superQuery

client = superQuery.Client()

dryrun = False

QUERY = """SELECT name FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 50"""

query_job = client.query(QUERY, username="xxxxxx", password="xxxxxx", project_id=None) 

rows = query_job.result()

for row in rows:
    print(row.name)

print ("---------STATISTICS---------")
if (not dryrun):
    print("Data rows:", query_job.totalRows)
    print("Workflow:", "Query")
    print("Cost:", query_job.superQueryTotalCost)
    print("Savings %:", query_job.saving)
    print("Was cache used?:", query_job.cacheUsed if hasattr(query_job, "cacheUsed") else False)
    print("DryRun flag: ", query_job.superParams["isDryRun"])
else:
    print("Workflow:", "DryRun")
    print("Potential BQ bytes scanned: ", query_job.bigQueryTotalBytesProcessed)
    print("Potential Data rows:", query_job.totalRows)
    print("DryRun flag: ", query_job.superParams["isDryRun"])