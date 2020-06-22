//Importing the necessary classes
import org.apache.spark._

import java.io.File
 
object airport {
  
    def main(args: Array[String]){ 
    //Creating a Case Class Flight
    case class Flight(dofM:String, dofW:String ,dist:Int)

    //Defining a Parse String function to parse input into Flight class
    def parseFlight(str: String): Flight = {
        val line = str.split(",")
        Flight(line(0), line(1), ... , line(16).toInt)
    }
    val conf = new SparkConf().setAppName("airport").setMaster("local[2]")
    val sc = new SparkContext(conf) 
    //Load the data into a RDD 
 
val textRDD = sc.textFile("in/Airports2.csv")
 
//Parse the RDD of CSV lines into an RDD of flight classes 
val flightsRDD = Map ParseFlight to Text RDD
 
//Create airports RDD with ID and Name
val airports = Map Flight OriginID and Origin
airports.take(1)
 
//Defining a default vertex called nowhere and mapping Airport ID for printlns
val nowhere = "nowhere"
val airportMap = Use Map Function .collect.toList.toMap
 
//Create routes RDD with sourceID, destinationID and distance
val routes = flightsRDD. Use Map Function .distinct
routes.take(2)
 
//Create edges RDD with sourceID, destinationID and distance
val edges = routes.map{( Map OriginID and DestinationID ) => Edge(org_id.toLong, dest_id.toLong, distance)}
edges.take(1)
 
//Define the graph and display some vertices and edges
val graph = Graph( Airports, Edges and Nowhere )
graph.vertices.take(2)
graph.edges.take(2)
 
//Query 1 - Find the total number of airports
val numairports = Vertices Number
 
//Query 2 - Calculate the total number of routes?
val numroutes = Number Of Edges
 
//Query 3 - Calculate those routes with distances more than 1000 miles
graph.edges.filter { Get the edge distance )=> distance > 1000}.take(3)
 
//Similarly write Scala code for the below queries
//Query 4 - Sort and print the longest routes
//Query 5 - Display highest degree vertices for incoming and outgoing flights of airports
//Query 6 - Get the airport name with IDs 10397 and 12478
//Query 7 - Find the airport with the highest incoming flights
//Query 8 - Find the airport with the highest outgoing flights
//Query 9 - Find the most important airports according to PageRank
//Query 10 - Sort the airports by ranking
//Query 11 - Display the most important airports
//Query 12 - Find the Routes with the lowest flight costs
//Query 13 - Find airports and their lowest flight costs
//Query 14 - Display airport codes along with sorted lowest flight costs
