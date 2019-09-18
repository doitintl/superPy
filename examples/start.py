from superQuery import superQuery

client = superQuery.Client()
#----------------------------------------------------
# Set your Google Cloud billing project
#----------------------------------------------------
client.project("yourBillingProjectId")

#----------------------------------------------------
# Select a destination dataset and table if you want
#----------------------------------------------------
# dataset_id = 'DEST_DATASET'
# job_config = superQuery.QueryJobConfig()
# client = client.dataset(dataset_id).table('testPythonDestination')

QUERY = """SELECT name FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 10"""

result = client.query(QUERY) 

print ("---------DATA---------")
#----------------------------------------------------
# Option A: Get data directly as rows of objects
#----------------------------------------------------
for row in result:
    print(row.name)

#----------------------------------------------------
# Option B: Get data into a pandas dataframe 
#----------------------------------------------------
# df = result.to_df()

print ("---------STATISTICS---------")
if (not result.stats.superParams["isDryRun"]):
    print("Data rows:", result.stats.totalRows)
    print("Workflow:", "Query")
    print("Cost: $ %.2f" % round(result.stats.superQueryTotalCost, 2))
    print("Savings %:", result.stats.saving)
    print("Was cache used?:", result.stats.cacheUsed if hasattr(result.stats, "cacheUsed") else False)
    print("Cache type:", result.stats.cacheType if hasattr(result.stats, "cacheUsed") else "None")
    print("DryRun flag: ", result.stats.superParams["isDryRun"])
else:
    print("Workflow:", "DryRun")
    print("Potential BQ bytes scanned: ", result.stats.bigQueryTotalBytesProcessed)
    print("Potential Data rows:", result.stats.totalRows)
    print("DryRun flag: ", result.stats.superParams["isDryRun"])
