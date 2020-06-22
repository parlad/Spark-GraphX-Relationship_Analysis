from graphframes import *


personsDf = spark.read.csv('in/persion.csv',header=True, inferSchema=True)
personsDf.createOrReplaceTempView("persons")
spark.sql("select * from persons").show()

relationshipDf = spark.read.csv('in/relationship.csv',header=True, inferSchema=True)
relationshipDf.createOrReplaceTempView("relationship")
spark.sql("select * from relationship").show()

graph = GraphFrame(personsDf, relationshipDf)

#Here you are going to find all the edges connected to Andrew.
graph.degrees.filter("id = 1").show()


personsTriangleCountDf.createOrReplaceTempView("personsTriangleCount")
maxCountDf = spark.sql("select max(count) as max_count from personsTriangleCount")
maxCountDf.createOrReplaceTempView("personsMaxTriangleCount")
spark.sql("select * from personsTriangleCount P JOIN (select * from personsMaxTriangleCount) M ON (M.max_count = P.count) ").show()


graph.bfs(fromExpr="Name='Bob'",toExpr="Name='William'").show()


graph.bfs(
   fromExpr = "name = 'Bob'",
   toExpr = "name = 'William'",
   ).show()