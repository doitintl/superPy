from superQuery import SuperQuery

sq = SuperQuery()
sq.authenticate_connection("xxxxxxxx", "xxxxxxxxx", "yourProjectId", "yourDatasetId")
mydata = sq.get_data("SELECT COUNT(*) as totalRows FROM `yourProjectId.yourDatasetId.yourTable`", get_stats=True)

print("Data:", mydata)
print("Cost:", sq.stats["superQueryTotalCost"])
print("Savings %:", sq.stats["saving"])

del sq

