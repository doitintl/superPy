from superQuery import superQuery

client = superQuery.Client()
# Select a project, otherwise the default project is selected
# client.set_project_id("yourproject") 

QUERY = """SELECT name FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 10"""

query_job = client.query(QUERY) 

rows = query_job.result()

print ("---------DATA---------")
for row in rows:
    print(row.name)

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