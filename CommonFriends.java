//import specific java standard classes
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.*;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.HashSet;
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

//start program
public class CommonFriends {

    // Mapper
    public static class FirstMapper
        extends Mapper<Object, Text, Text, Text> {

          // will hold the first value in each row
          private Text id = new Text();
          // will hold the rest of the values in the row
          private Text friends = new Text();

          // mapper function
          public void map(Object key, Text value, Context context)
              throws IOException, InterruptedException {

                  // Split the values received into discrete values
                  Text idPair = new Text();
                  Text friendList = new Text();
                  StringTokenizer itr = new StringTokenizer(value.toString());
                  String user = new String();
                  String[] friends = new String[2];

                  // split the friends off from the user
                  friends = value.toString().split(" ", 2);

                  // collect the first token. This is the user.
                  if(itr.hasMoreTokens()) {
                      user = itr.nextToken();
                  }

                  // create key value pairs with the user's friends.
                  // these will all be written to the context
                  while(itr.hasMoreTokens()) {
                      String friend = itr.nextToken();
                      String pair = new String();
                      if (Integer.parseInt(user) < Integer.parseInt(friend)) {
                          pair = user + " " + friend;
                      } else {
                          pair = friend + " " + user;
                      }
                      idPair.set(pair);
                      friendList.set(friends[1]);
                      context.write(idPair, friendList);
                  }
          }
    }

    public static class FirstReducer
        extends Reducer<Text, Text, Text, Text> {

            public void reduce(Text key, Iterable<Text> values,
                              Context context)
                              throws IOException, InterruptedException {


                  LinkedList<TreeSet<String>> friendLists = new LinkedList<TreeSet<String>>();
                  TreeSet<String> commonFriends = new TreeSet();
                  Text result = new Text();

                  // Each key that is the same will go to the same reducer
                  // This will allow the two friends to have access to Each
                  // others' friend lists in the iterable<Text>.
                  for (Text val : values) {
                        StringTokenizer itr = new StringTokenizer(val.toString());
                        TreeSet<String> friendList = new TreeSet();
                        while (itr.hasMoreTokens()) {
                            friendList.add(itr.nextToken());
                        }
                        friendLists.push(friendList);
                  }

                  // the trick now is accessing the inner sets from the list
                  commonFriends = friendLists.pop();
                  commonFriends.retainAll(friendLists.pop());
                  // if the result isn't empty, then write it to context
                  if(!commonFriends.isEmpty()) {
                        StringBuilder resultStr = new StringBuilder();
                        for (String friend : commonFriends) {
                            resultStr.append(friend);
                            resultStr.append(" ");
                        }

                        // need to set the result as a Text object
                        result.set(resultStr.toString());

                        // Add parentheses to key
                        String tempString = new String();
                        tempString = "(" + key.toString() + ")";
                        key.set(tempString);

                        // Write key and results out
                        context.write(key, result);
                  }
                }
         }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1])))
            fs.delete(new Path(args[1]), true);
        Job job = Job.getInstance(conf, "friend finder");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(FirstMapper.class);
        // no combiner for now
        job.setReducerClass(FirstReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
