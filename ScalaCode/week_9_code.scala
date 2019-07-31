// Importing necessary packages
// An iterable representation of source data
import scala.io.Source
// Provides for system input and output through data streams, serialization and the file system.
import java.io._
// Provides classes for reading and writing the standard ZIP and GZIP file formats.
import java.util.zip._
// An implementation of the Buffer class using an array
import scala.collection.mutable.ArrayBuffer


// ArrayBuffer is resizable, Array isn't. If you append an element to an ArrayBuffer, it gets larger.
// If you try to append an element to an Array, you get a new array.

// create an empty ArrayBuffer to store Title and Indices.
var sample_titles = ArrayBuffer[String]()
var sample_indices = ArrayBuffer[Int]()

// define the number of records of result
val space = 10

// The variable will hold the object for random number generator
val myrandnum = scala.util.Random
// This will give us the same output every time
// the random number generator will start generating sequesnce from this.
myrandnum.setSeed(21054365)

// This function will convert GZIP file to a stream
// GZIPInputStream reads compressed gzip file by implementing a stream filter
// The function takes FileInputStream object as a parameter
// The FileInputStream object also has a string parameter and returns input bytes
def gis(s: String) = new GZIPInputStream(new FileInputStream(s))

// The actual function where RESERVOIR SAMPLING happens
// We use fromInputStream() function that will call the gis() function to return the input bytes
// for each input bute stream w will call getLines() function whihc will return the given lines
// and finally zipWithIndex will create a pair of received line and their respective index as a collection
// Setting a for loop to add all titles and indices to the arrays
for ((line, index) <- Source.fromInputStream(gis(args(0))).getLines().zipWithIndex) {
   // fill in the ArrayBuffer from 0 until nth pairs
   if(index < space){
     sample_titles += line
     sample_indices += index
    } else {
	// create a random number after nth term hits
     val randnum = myrandnum.nextInt(index+1) 
	// if the number less than the length of result space
	// repace the current value
     if(randnum < space) {
        sample_titles(randnum) = line
        sample_indices(randnum) = index
     }
   }
}

// printing the result  about the sampled data
for(i <- sample_titles.indices)
	println(s"sample_titles("+i+") -> ["+sample_indices(i)+"]  "+sample_titles(i))
