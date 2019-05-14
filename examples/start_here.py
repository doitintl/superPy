from superQuery import SuperQuery

# Set to True if you wish to do a dryrun
dryrun = False 

sq = SuperQuery()
mydata = sq.get_data(
    "SELECT field FROM `projectId.datasetId.tableID` WHERE _PARTITIONTIME = \"20xx-xx-xx\"", 
    get_stats=True, 
    dry_run=dryrun, 
    username="xxxxxxxxx", 
    password="xxxxxxxxx")

print ("---------RESULTS---------")
if (not dryrun):
    print("Data rows:", mydata.stats["totalRows"])
    print("Workflow:", "Query")
    print("Cost:", mydata.stats["superQueryTotalCost"])
    print("Savings %:", mydata.stats["saving"])
    print("Was cache used?:", mydata.stats["cacheUsed"] if "cacheUsed" in mydata.stats else False)
    print("DryRun flag: ", mydata.stats["superParams"]["isDryRun"])
else:
    print("Workflow:", "DryRun")
    print("Potential BQ bytes scanned: ", mydata.stats["bigQueryTotalBytesProcessed"])
    print("Potential Data rows:", mydata.stats["totalRows"])
    print("DryRun flag: ", mydata.stats["superParams"]["isDryRun"])

del sq

