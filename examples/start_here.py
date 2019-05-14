from superQuery import SuperQuery

# Set to True if you wish to do a dryrun
dryrun = False 

sq = SuperQuery()
mydata = sq.get_data(
    "SELECT field1, field2 FROM `projectId.datasetId.tableId` WHERE _PARTITIONTIME = \"2019-05-04\" ORDER BY field1 ASC", 
    username="xxxxxxxxxx", 
    password="xxxxxxxxxx", 
    start=1, 
    stop=10)

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

print ("---------10 ROWS OF DATA---------")
for row in mydata:
    print("Row :", row)

del sq