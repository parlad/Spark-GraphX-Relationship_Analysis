# Set File Paths
tripdelaysFilePath = "in/departuredelays.csv"
airportsnaFilePath = "in/airport-codes-na.txt"

# Obtain airports dataset
airportsna = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true', delimiter='\t')\
    .load(airportsnaFilePath)
airportsna.registerTempTable("airports_na")

# Obtain departure Delays data
departureDelays = sqlContext.read.format("com.databricks.spark.csv").options(header='true').load(tripdelaysFilePath)
departureDelays.registerTempTable("departureDelays")
departureDelays.cache()

# Available IATA codes from the departuredelays sample dataset
tripIATA = sqlContext\
    .sql("select distinct iata from (select distinct origin as iata from departureDelays union all" 
         "select distinct destination as iata from departureDelays) a")
tripIATA.registerTempTable("tripIATA")

# Only include airports with atleast one trip from the departureDelays dataset
airports = sqlContext.sql("select f.IATA, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
airports.registerTempTable("airports")
airports.cache()



# Finding the longest Delay
longestDelay = tripGraph.edges.groupBy().max("delay")
longestDelay.show()


tripGraph.edges\
  .filter("src = 'SFO' and delay > 0")\
  .groupBy("src", "dst")\
  .avg("delay")\
  .sort(desc("avg(delay)"))
tripGraph.edges.filter("src = 'SFO' and delay > 0").groupBy("src", "dst").avg("delay").sort(desc("avg(delay)")).show()



# After displaying tripDelays, use Plot Options to set `state_dst` as a Key.

tripDelays = tripGraph.edges.filter("delay > 0")
tripDelays.show()




# Using tripGraphPrime to more easily display 
#   - The associated edge (ab, bc) relationships 
#   - With the different the city / airports (a, b, c) where SFO is the connecting city (b)
#   - Ensuring that flight ab (i.e. the flight to SFO) occured before flight bc (i.e. flight leaving SFO)
#   - Note, TripID was generated based on time in the format of MMDDHHMM converted to int
#       - Therefore bc.tripid < ab.tripid + 10000 means the second flight (bc) occured within approx a day of the first flight (ab)
# Note: In reality, we would need to be more careful to link trips ab and bc.
motifs = tripGraphPrime.find("(a)-[ab]->(b); (b)-[bc]->(c)")\
  .filter("(b.id = 'SFO') and (ab.delay > 500 or bc.delay > 500) and bc.tripid > ab.tripid and bc.tripid < ab.tripid + 10000")
motifs.show()





# Determining Airport ranking of importance using `pageRank`
ranks = tripGraph.pageRank(resetProbability=0.15, maxIter=5)
ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(20).show()





# Determine the most popular flights (single city hops)
import pyspark.sql.functions as func
topTrips = tripGraph \
  .edges \
  .groupBy("src", "dst") \
  .agg(func.count("delay").alias("trips")) 
  
topTrips.orderBy(topTrips.trips.desc()).limit(20)


# Calculate the inDeg (flights into the airport) and outDeg (flights leaving the airport)
inDeg = tripGraph.inDegrees
outDeg = tripGraph.outDegrees

# Calculate the degreeRatio (inDeg/outDeg)
degreeRatio = inDeg.join(outDeg, inDeg.id == outDeg.id) \
  .drop(outDeg.id) \
  .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio") \
  .cache()

# Join back to the `airports` DataFrame (instead of registering temp table as above)
nonTransferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio") \
  .filter("degreeRatio < .9 or degreeRatio > 1.1")

# List out the city airports which have abnormal degree ratios.
nonTransferAirports.show()


# Join back to the `airports` DataFrame (instead of registering temp table as above)
transferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio") \
  .filter("degreeRatio between 0.9 and 1.1")
  
# List out the top 10 transfer city airports
transferAirports.orderBy("degreeRatio").limit(10).show



# Example 1: Direct Seattle to San Francisco 
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SEA'",
  toExpr = "id = 'SFO'",
  maxPathLength = 1)
filteredPaths.show()