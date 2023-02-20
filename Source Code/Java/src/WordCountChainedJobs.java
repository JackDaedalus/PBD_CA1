import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountChainedJobs {
    
    // First MapReduce Job
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        
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
    
    // Second MapReduce Job
    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text>{
        
        private IntWritable count = new IntWritable();
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] fields = value.toString().split("\t");
            word.set(fields[0]);
            count.set(Integer.parseInt(fields[1]));
            context.write(count, word);
        }
    }
    
    public static class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            for (Text val : values) {
                context.write(val, key);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(WordCountChainedJobs.class);
        
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        boolean success1 = job1.waitForCompletion(true);
        
        if (success1) {
            Job job2 = Job.getInstance(conf, "sort by count");
            job2.setJarByClass(WordCountChainedJobs.class);
            
            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(SortReducer.class);
            
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
