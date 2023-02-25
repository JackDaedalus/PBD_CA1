import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AlphaCharFrequency {

    public static class CharMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                for (int i = 0; i < token.length(); i++) {
                    char c = token.charAt(i);
                    if (Character.isLetter(c)) {
                        word.set(String.valueOf(c));
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class CharReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException, InterruptedException {
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
        Job job = Job.getInstance(conf, "alpha char frequency");

        // set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set mapper and reducer classes for the first job
        job.setMapperClass(CharMapper.class);
        job.setReducerClass(CharReducer.class);

        // set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // create the second job using the first job's output as input
        Job job2 = Job.getInstance(conf, "alpha char frequency - sort");
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_sort"));

        // set mapper and reducer classes for the second job
        ChainMapper.addMapper(job2, CharMapper.class, Object.class, Text.class, Text.class, IntWritable.class, conf);
        ChainReducer.setReducer(job2, CharReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        // set output key and value classes
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // run the jobs in chain
//        Job[] jobs = { job, job2 };
//        org.apache.hadoop.mapreduce.JobControl jobControl = new org.apache.hadoop.mapreduce.JobControl("alpha char frequency");
//        for (Job j : jobs) {
//            jobControl.addJob(j);
//        }
//        Thread jobControlThread = new Thread(jobControl);
//        jobControlThread.start();
        
    }}
