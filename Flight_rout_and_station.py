#import relevant libraries for Graph Frames
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from graphframes import *

#Read the csv files 
verticesRDD = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load("in/station.csv")
edgesRDD = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load("in/routes.csv")

#Renaming the id columns to enable GraphFrame 
verticesRDD = verticesRDD.withColumnRenamed("name", "id,")
edgesRDD = edgesRDD.withColumnRenamed("Trip ID", "sid")
edgesRDD = edgesRDD.withColumnRenamed("Start Station", "src")
edgesRDD = edgesRDD.withColumnRenamed("End Station", "dst")

#Register as temporary tables for running the analysis
verticesRDD.registerTempTable("verticesRDD")
edgesRDD.registerTempTable("edgesRDD")
#Note: whether i register the RDDs as temp tables or not, i get the same results... so im not sure if this step is really needed

#Make the GraphFrame
g = GraphFrame(verticesRDD, edgesRDD)
g.pageRank(resetProbability=0.15, maxIter=10)
results = g.pageRank(resetProbability=0.15, maxIter=10, sourceId="id,")
ranks = g.pageRank.resetProbability(0.15).maxIter(10).run()
ranks = g.pageRank(resetProbability=0.15, maxIter=10).run()