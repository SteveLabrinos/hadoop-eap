import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/** @author Steve Labrinos [stalab at linux.org] on 27/3/21 */

public class InvertedIndex {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    //  assure that the program is executed with 3 arguments for word length
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Program must have 3 arguments");
      System.exit(-1);
    }
    //  set configuration options bases on args for mapper and reducer
    conf.set("wordLength", otherArgs[2]);

    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(InvIndexReducer.class);
    job.setReducerClass(InvIndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private final Text word = new Text();
    private final Text file = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      //  get the words min length
      Configuration conf = context.getConfiguration();
      int wordLength = Integer.parseInt(conf.get("wordLength"));

      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

      //  convert line to lower case and remove character other than letters
      String line = value.toString().toLowerCase().replaceAll("[^a-z\\s]", "");

      //  split the words into array based on spaces or multiple spaces
      //  the line is trimmed to remove spaces in the start or end of the line
      String[] words = line.trim().split("\\s+");

      //  for every word in lines words list check if the length is above
      //  the filter and map the current file as value
      for (String w : words) {
        if (w.length() < wordLength) continue;
        word.set(w);
        file.set(fileName);
        context.write(word, file);
      }
    }
  }

  public static class InvIndexReducer extends Reducer<Text, Text, Text, Text> {
    private final Text resultValue = new Text();
    private final Text resultKey = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      Map<String, String> filesMap = new HashMap<String, String>();
      //  use a histogram to append all the files of the key word
      for (Text file : values) {
        if (filesMap.containsKey(key.toString())) {
          //  if the file is already in the histogram don't append it
          if (filesMap.containsValue(file.toString())) continue;
          filesMap.put(key.toString(), filesMap.get(key.toString()) + ", " + file.toString());
        } else {
          filesMap.put(key.toString(), file.toString());
        }
      }

      //  sort the histogram by keys and provide the reducer
      SortedSet<String> keys = new TreeSet<String>(filesMap.keySet());
      for (String k : keys) {
        String value = filesMap.get(k);
        resultKey.set(k);
        resultValue.set(value);
        context.write(resultKey, resultValue);
      }
    }
  }
}
