import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
// suppress verbose spark output
sc.setLogLevel("OFF")
sc.stop()
// parameters for the K-means streaming algorithm
// put the gData.csv file to /home/user/ please
val dataPath = "/home/user/gData.csv"
// Create a local StreamingContext with 2 working threads 
// The master requires 2 cores to prevent from a starvation scenario.
val conf = new SparkConf().setMaster("local[2]").setAppName("KMeans")
// Set batch interval of 3 seconds
val ssc = new StreamingContext(conf, Seconds(3))
// K-means streaming code adapted from
//    https://spark.apache.org/docs/latest/mllib-clustering.html#k-means
val tSet = ssc.textFileStream(dataPath).map(Vectors.parse)
// Define model with a window memory of half the data (DecayFactor)
val model = new StreamingKMeans().
    setK(3).
    setDecayFactor(0.1).
    setRandomCenters(2, 50.0)
// train the model and repeatedly output the latest cluster centres
model.trainOn(tSet)
model.latestModel().clusterCenters.toArray.foreach(println)
// Start the computation
ssc.start()
// Also, wait for 40 seconds or termination signal
ssc.awaitTerminationOrTimeout(40000)
ssc.stop()
// end!
