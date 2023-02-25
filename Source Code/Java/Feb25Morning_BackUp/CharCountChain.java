import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CharCountChain {

  public static class CharTokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      for (char c : line.toCharArray()) {
        if (Character.isLetter(c)) {
          word.set(Character.toString(c));
          context.write(word, one);
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    String[] otherArgs = args;
    if (otherArgs.length != 2) {
      System.err.println("Usage: charcountchain <in> <out>");
      System.exit(2);
    }

    Job job1 = Job.getInstance(conf, "char count chain 1");
    job1.setJarByClass(CharCountChain.class);
    job1.setMapperClass(CharTokenizerMapper.class);
    job1.setReducerClass(IntSumReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

    Job job2 = Job.getInstance(conf, "char count chain 2");
    job2.setJarByClass(CharCountChain.class);
    job2.setMapperClass(CharTokenizerMapper.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    ChainMapper.addMapper(job2, CharTokenizerMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf);
    ChainReducer.setReducer(job2, IntSumReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

    FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "_out"));

    Job[] jobs = new Job[]{job1, job2};
    for (Job job : jobs) {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        System.exit(1);
      }
    }
    System.exit(0);
  }
}
