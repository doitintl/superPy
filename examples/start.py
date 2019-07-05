from superQuery import SuperQuery

# Set to True if you wish to do a dryrun
dryrun = False 

sq = SuperQuery()

# For project_id: If you don't specify a project_id, your default project will be chosen
mydata = sq.get_data(
    """SELECT name FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 50000""", 
    username="R14SnKpLY2", 
    password="F4rY_Ovvsj", 
    project_id=None) 

print ("---------STATS---------")
if (not dryrun):
    print("Data rows:", mydata.totalRows)
    print("Workflow:", "Query")
    print("Cost:", mydata.superQueryTotalCost)
    print("Savings %:", mydata.saving)
    print("Was cache used?:", mydata.cacheUsed if hasattr(mydata, "cacheUsed") else False)
    print("DryRun flag: ", mydata.superParams["isDryRun"])
else:
    print("Workflow:", "DryRun")
    print("Potential BQ bytes scanned: ", mydata.bigQueryTotalBytesProcessed)
    print("Potential Data rows:", mydata.totalRows)
    print("DryRun flag: ", mydata.superParams["isDryRun"])

print ("---------DATA---------")
i = 1
for i, row in enumerate(mydata):
    print("Row " + str(i) + " :", row)
    if (i > 10):
        break;

del sq