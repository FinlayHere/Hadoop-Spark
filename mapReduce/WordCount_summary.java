import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

//A Map operation is called once for each of the input split.
//Then we split each of the input split into tokens.
//We iterate over every token and each token becomes a word to be written into context with a IntWritable value 1
// and hence creating a key value pair (word, one)

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	  StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  // A Reduce operation is called once for each key.
  // The main task of the reduce function is to add up all the occurences of a particular key 
  // .i.e. incrememting the count of key "foo" by 1 each time it occurs.
  // and finally write to context as key and number of time it occuered.
  // eg:- (foo, 4)
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
	  int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	// A new configuration.
    Configuration conf = new Configuration();
	//Creates a new Job with no particular Cluster and a given jobName.
	//A Cluster will be created from the conf parameter only when it's needed.
	//The Job makes a copy of the Configuration so that any necessary internal modifications do not reflect on the incoming parameter.
    Job job = Job.getInstance(conf, "word count");
	//Set the Jar by finding where a given class came from.
    job.setJarByClass(WordCount.class);
	//Set the Mapper for the job.
    job.setMapperClass(TokenizerMapper.class);
	//Set the combiner class for the job.
    job.setCombinerClass(IntSumReducer.class);
	//Set the Reducer for the job.
    job.setReducerClass(IntSumReducer.class);
	//Set the key class for the job output data.
    job.setOutputKeyClass(Text.class);
	//Set the value class for job outputs.
    job.setOutputValueClass(IntWritable.class);
	//Add a Path to the list of inputs for the map-reduce job.
    FileInputFormat.addInputPath(job, new Path(args[0]));
	//Set the Path of the output directory for the map-reduce job.
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	//Exit the job once the job is completed.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
