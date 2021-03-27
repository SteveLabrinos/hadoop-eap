import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** @author Steve Labrinos [stalab at linux.org] on 27/3/21 */

public class MovieTypes {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Movies-Types-Count");
    job.setJarByClass(MovieTypes.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /*
  Mapper
  The method collects movie types for every movie
  then produces a key for all the different movie types 2-combinations
   */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      //  split the line and get the final part with the movies types
      String[] movie = value.toString().split("::");
      //  discarding lines that are missing the mandatory fields
      if (movie.length <= 2) return;
      String movieTypes = movie[movie.length - 1];

      //  getting all the different movie types and sort them
      //  to assure the the 2-combinations are not interchangeable
      String[] types = movieTypes.split("\\|");
      List<String> typesList = new ArrayList<String>(Arrays.asList(types));
      Collections.sort(typesList);

      //  assure that the movie contains al least 2 different movie types
      int size = typesList.size();
      if (size <= 1) return;

      //  create keys with all the different unique combinations
      //  and pass them to the reducer
      for (int i = 0; i < size - 1; i++) {
        String firstType = typesList.get(i);
        for (int j = i + 1; j < typesList.size(); j++) {
          String secondType = typesList.get(j);
          word.set(firstType + "_" + secondType);
          context.write(word, one);
        }
      }
    }
  }

  /*
  Reducer
  The method provides a single count for all the different keys of the mapper
   */
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
}
