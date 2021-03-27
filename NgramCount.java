import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/** @author Steve Labrinos [stalab at linux.org] on 26/3/21 */

public class NgramCount {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    //  assure that the program is executed with 4 arguments for ngrams and count filter
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4) {
      System.err.println("Program must have 4 arguments");
      System.exit(-1);
    }
    //  set configuration options bases on args for mapper and reducer
    conf.set("ngramsLength", otherArgs[2]);
    conf.set("countFilter", otherArgs[3]);

    Job job = Job.getInstance(conf, "n-Gram Count");
    job.setJarByClass(NgramCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    //  remaining line buffer to correctly calculate the n-grams of the next line
    StringBuilder remainingText = new StringBuilder();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      //  get the length of n-grams
      Configuration conf = context.getConfiguration();
      int n = Integer.parseInt(conf.get("ngramsLength"));

      //  convert line to lower case and remove character other than letter or numbers
      String line = value.toString().toLowerCase().replaceAll("[^a-z0-9\\s]", "");

      //  concat the remaining text containing the remaining (n - 1) words of the previous line
      line = remainingText + line;
      //  reset the remaining text for the next line
      remainingText = new StringBuilder();

      //  split the words into array based on spaces or multiple spaces
      //  the line is trimmed to remove spaces in the start or end of the line
      String[] words = line.trim().split("\\s+");

      //  get the n -1 last words to be concatenated in the next line
      int len = words.length > n ? words.length - n + 1 : 0;
      for (int i = len; i < words.length; i++) {
        remainingText.append(words[i]).append(" ");
      }

      //  for all the words begin the n-gram algorithm
      for (int i = 0; i + n <= words.length; i++) {
        StringBuilder text = new StringBuilder();
        //  create an n-gram for every every word of the line up to n - 1 last words
        //  because the n-gram contains up the the last word of the line
        for (int j = i; j < i + n; j++) {
          text.append(words[j]).append(" ");
        }
        word.set(String.valueOf(text));
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      //  get the counter filter from configuration
      int k = Integer.parseInt(conf.get("countFilter"));
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      //  first compute the total counts of the key and then only write
      //  the keys with values greater that the filter
      if (sum >= k) {
        result.set(sum);
        context.write(key, result);
      }
    }
  }
}
