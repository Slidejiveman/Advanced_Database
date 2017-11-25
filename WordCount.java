//import specific java standard classes
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//import classes from the hadoop class space
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//start of the program
public class WordCount {
  private static ArrayList<String> filterWords = new ArrayList<String>();

  // Maps the strings
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{ // define object and text as types it takes in
	                                                    // define text and IntWritable as output types
    private static IntWritable length = new IntWritable(1); // initialization of length for words
    private Text word = new Text();                            // text value called word

    // This function does the actual mapping.
    // It has the input Key and input value here. The Context is the job context,
    // which is where everything gets written to
    // called for every line in the texts and produces (word,one) tuples
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); // This takes a string and allows us to
      Text tempWord = new Text();
      String tempString;// in a given line of text
      while (itr.hasMoreTokens()) {        // if we have more tokens
          tempWord.set(itr.nextToken().toLowerCase().replaceFirst("^[^a-zA-Z]+", "").replaceAll("[^a-zA-Z]+$",""));
    	    if(!filterWords.contains(tempWord.toString())) {
    	      if(tempWord.toString().length() >= 4 ) {
    	    	  tempString = tempWord.toString().substring(0,4);
    	      } else {
    	    	  tempString = tempWord.toString();
    	      }
    		  word.set(tempString);         // pull it out
    		  length.set(tempWord.toString().length()); // doesn't return a value, so must be set and passed
              context.write(word, length);          // and write it to the job context, which is word key with one value
        }
      }
    }
  }
  
  // Mapper responsible for finding the top ten highest values
  public static class TopTenMapper
      extends Mapper<Object, Text, Text, DoubleWritable> {
	  
	  private Text word = new Text();
	  private DoubleWritable average = new DoubleWritable();
	  
	  public void map(Object key, Text value, Context context)
	                  throws IOException, InterruptedException {
		  StringTokenizer itr = new StringTokenizer(value.toString());            // input is tab delimited line with key and value
		  
		  if(itr.hasMoreTokens())
		      word.set(itr.nextToken());
		  if(itr.hasMoreTokens())
		      average.set(Double.parseDouble(itr.nextToken()));
		  
		  context.write(word, average);
	  }
	  
  }

  // This is our reducer. This method will add up all of the tuples that have the same
  // key. All words with the same key go to the same reducer. The reducer, then, will
  // add all of the constant ones.
  public static class LengthAverageReducer 
       extends Reducer<Text,IntWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values, // takes the key and all of the values with it
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int total = 0;                   // need total number of four character strings
      for (IntWritable val : values) { // add up all of the values in your values
        sum += val.get();
        total++;
      }
      
      // calculate the average from the total number of words and the length
      double average = ((double) sum) / total;
      result.set(average);              // set up the result with the sum
      context.write(key, result);       // output the key with the summed one's as a result
    }
  }
  
  public static class TopTenReducer
      extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	 
	  private TreeMap<Double, String> topTen = new TreeMap<Double, String>();
	  private Text word = new Text();
	  private DoubleWritable average = new DoubleWritable();
	  
	  public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
              throws IOException, InterruptedException {
  
          topTen.put(values.iterator().next().get(), text.toString());                           // add the string and the average value to DS
  
          if(topTen.size() > 10) {
	          topTen.remove(topTen.firstKey());                   // if we are over size 10, then remove the first one
          }
        }

		public void cleanup(Context context)
		              throws IOException, InterruptedException {
		  // write context in here because top ten is shifting until finished
		  for (Double dbl : topTen.keySet()) {
			  word.set(topTen.get(dbl));
			  average.set(dbl);
			  context.write(word,average);
		  }
		}
  }

  public static void initializeFilterWords() {
    filterWords.add("i");
    filterWords.add("me");
    filterWords.add("you");
    filterWords.add("him");
    filterWords.add("her");
    filterWords.add("they");
    filterWords.add("is");
    filterWords.add("are");
    filterWords.add("was");
    filterWords.add("were");
    filterWords.add("am");
    filterWords.add("will");
    filterWords.add("their");
    filterWords.add("his");
    filterWords.add("hers");
    filterWords.add("my");
    filterWords.add("its");
    filterWords.add("a");
    filterWords.add("an");
    filterWords.add("the");
  }

  //sets up a job to be run on the server cluster
  public static void main(String[] args) throws Exception {
    initializeFilterWords();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(new Path(args[1])))
    	fs.delete(new Path(args[1]),true);                  // delete output files if they exist!
    Job job = Job.getInstance(conf, "word count");          // create instance with name
    job.setJarByClass(WordCount.class);                     // set class name associated with the job
    job.setMapperClass(TokenizerMapper.class);              // give the job a given Mapper
    //job.setCombinerClass(LengthAverageReducer.class);              // set the combiner (optional); does reducing locally minimizes communication
    job.setReducerClass(LengthAverageReducer.class);               // set the reducer (these can be the same)
    job.setOutputKeyClass(Text.class);                      // set the key for the output class. The key is a String here
    job.setOutputValueClass(IntWritable.class);             // the count of the String occurrences appear here
    FileInputFormat.addInputPath(job, new Path(args[0]));   // the input is where the text is coming from
    FileOutputFormat.setOutputPath(job, new Path("temp"));  // the output is where the text is going to
    if(!job.waitForCompletion(true))
    	System.exit(1);                                     // exit with an error if first job isn't successful
    //Assume that SecondMapper and SecondReducer exist! 
    Job job2 = Job.getInstance(conf, "top ten");
    job2.setJarByClass(WordCount.class);
    job2.setMapperClass(TopTenMapper.class); // doesn't exist yet
    //job2.setCombinerClass(SecondReducer.class);             // There may be a time when we need to get rid of this line b/c problems!!!
    job2.setReducerClass(TopTenReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job2, new Path("temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    if(!job2.waitForCompletion(true))
    	System.exit(1);                                     // exit with an error if secondv job isn't successful
    fs.delete(new Path("temp"), true);
    System.exit(0);                                         // We completed successfully
  }
}
