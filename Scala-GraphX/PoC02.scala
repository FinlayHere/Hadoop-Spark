// starting the spark shell
cd $SPARK_HOME
spark-shell

// Import the required libraries for proper functionality
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
// Read file
val df = spark.read.option("header","false").csv("/home/usr/Documents/Datasets/test.csv.bz2")

// select OriginAirportID and OriginAirport as ori
// select DestinationAirportID and DestinationAirport as des
val ori = df.select($"_c5", $"_c6")
val des = df.select($"_c7", $"_c8")

// Concatenate distinct ori and des into one dataframe
val aInfo = ori.union(des).toDF("ID", "Name").distinct()
aInfo.show(10)

// select unique OriginAirportID,DestinationAirportID and their distances
val flightsFromTo = df.select($"_c5", $"_c7", $"_c16").distinct

// convert airportVertices to RDD.
// VertexId required to cast to Long
val airportVertices: RDD[(VertexId, String)] = aInfo.rdd.map(x => (x(0).toString.toLong, x(1).toString))
// For each data in flightsFromTo transfer to Edge(origin flight number, desination flight number, the flight distance)
val flightEdges = flightsFromTo.rdd.map(x => Edge(x(0).toString.toLong,x(1).toString.toLong,x(2).toString.toInt))
//Define default airport
val defaultAirport = ("Missing")

val graph = Graph(airportVertices, flightEdges, defaultAirport)
graph.persist()
//Print 10 shortest Route in ascending order
val shortestRoute = graph.triplets.map(t=>(t.srcAttr,t.dstAttr,t.attr)).sortBy(_._3, ascending=true).take(20)
