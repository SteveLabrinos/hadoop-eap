import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/** @author Steve Labrinos [stalab at linux.org] on 27/3/21 */

public class CovidReport {

  private static final String TEMPPATH = "./tmp";
  private static int average = 0;

  //  static method that provides the average number of cases from the first MapReduce cycle
  private static int readAndCalcAvg(Path path, Configuration conf) throws IOException {
    int average = 0;
    FileSystem fs = FileSystem.get(conf);

    //  parsing the temp file with the output of the first reducer
    BufferedReader br = null;
    try {
      Path file = new Path(path, "part-r-00000");
      if (!fs.exists(file)) throw new IOException("Output not found!");

      br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
      String[] words = br.readLine().split("\\s+");
      //  getting the numeric value
      for (String w : words) {
        if (NumberUtils.isCreatable(w)) {
          average = Integer.parseInt(w);
        }
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }
    return average;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //  first cycle of MapReduce that calculates the general average
    Job jobAvg = Job.getInstance(conf, "CovidCasesAverage");
    jobAvg.setJarByClass(CovidReport.class);
    jobAvg.setMapperClass(AverageMapper.class);
    jobAvg.setCombinerClass(AverageReducer.class);
    jobAvg.setReducerClass(AverageReducer.class);
    jobAvg.setOutputKeyClass(Text.class);
    jobAvg.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(jobAvg, new Path(args[0]));
    //  temporary path for the first MapReduce cycle
    Path tempDir = new Path(TEMPPATH);
    FileOutputFormat.setOutputPath(jobAvg, tempDir);
    //  wait util the first job is completed to chain the second MapReduce cycle
    jobAvg.waitForCompletion(true);

    //  chain the second MapReduce cycle
    average = readAndCalcAvg(tempDir, conf);
    FileSystem.get(conf).delete(tempDir, true);

    Job jobCountiesCases = Job.getInstance(conf, "CovidCasesCount");
    jobCountiesCases.setJarByClass(CovidReport.class);
    jobCountiesCases.setMapperClass(CountryCasesMapper.class);
    jobCountiesCases.setCombinerClass(CountriesReducer.class);
    jobCountiesCases.setReducerClass(CountriesReducer.class);
    jobCountiesCases.setOutputKeyClass(Text.class);
    jobCountiesCases.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(jobCountiesCases, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobCountiesCases, new Path(args[1]));
    System.exit(jobCountiesCases.waitForCompletion(true) ? 0 : 1);
  }

  /*
  1rst cycle Mapper
  the method parses the csv report, providing the same key "cases"
  and the cases of each line
  */
  public static class AverageMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable cases = new IntWritable();
    private final Text word = new Text("cases");

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      //  split the csv fields to an array of Strings
      String[] fields = value.toString().split(",");

      //  check that the 4rth value is numeric to avoid exceptions
      //  discarding first line headers
      if (NumberUtils.isCreatable(fields[4])) {
        cases.set(Integer.parseInt(fields[4]));
        context.write(word, cases);
      }
    }
  }

  /*
  1rst cycle Reducer
  the method takes one key with the list of all the cases
  and calculates the general average as integer
  */
  public static class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      int cnt = 0;
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
        cnt++;
      }

      result.set(sum / cnt);
      System.out.println("General Average Cases: " + result);
      context.write(key, result);
    }
  }

  /*
  2nd cycle Mapper
  the method parses the csv report and if the cases of a specific date
  is greater than the general average it produces a key
  YYYY-MM-COUNTRY with the value of 1
  */
  public static class CountryCasesMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      //  split the csv fields to an array of Strings
      String[] fields = value.toString().split(",");

      //  check that the 4rth value is numeric to avoid exceptions
      //  discarding first line headers
      if (!NumberUtils.isCreatable(fields[4])) return;

      int cases = Integer.parseInt(fields[4]);
      //  the mapper only produces key, values for cases greater than average
      if (cases > average) {
        String keyWord = fields[3] + "-" + fields[2] + "-" + fields[6];
        word.set(keyWord);
        context.write(word, one);
      }
    }
  }

  /*
  2nd cycle Reducer
  the method takes the different keys of the mapper
  and counts the cases, providing a group count of country per month
  for cases that are greater than the general average
  */
  public static class CountriesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      //  the avg as integer to compare it with the cases of the second mapper
      result.set(sum);
      context.write(key, result);
    }
  }
}
