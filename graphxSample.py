from pyspark.sql.functions import window, column, desc, col
from graphframes import *
stationGraph = GraphFrame(stationVertices, tripEdges)

staticDataFrame.selectExpr("CustomerId","(UnitPrice * Quantity) as total_cost" ,"InvoiceDate" ).groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")) .sum("total_cost").show(5)