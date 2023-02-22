// Week Three Class Exercise - Average Word Length for each character using Counters
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class AverageLetterFrequency {

	public static void main(String[] args) throws Exception {
		
		System.out.println("Looking for Feb 21-2 CA1:AverageLetterFrequency Driver inputs...");
		
		if (args.length != 2) {
			System.err.println("Usage: AverageLetterFrequency <input path> <output path>");
			System.exit(-1); }
		
		System.out.println("In the CA1:AverageLetterFrequency Driver now!");
		
		// Set Up language from input command line glob
		String cmdInputArg0 = args[0];
		int iUndrScrIndex = cmdInputArg0.indexOf('_');
		String strLang = cmdInputArg0.substring(iUndrScrIndex+1).trim();
		strLang = strLang + '\t';
		
		// Check Parse of Input globs for language description
		System.out.println("\n\nInput glob... args[0] - language substring is: " + strLang + "\n\n");
		
		
        Configuration conf = new Configuration();     
        
        // Set up Job1
        Job job1 = Job.getInstance(conf, "AverageLetterFrequency");
        
        job1.setJarByClass(AverageLetterFrequency.class);
        
		job1.setMapperClass(AverageLetterMapper.class);
		job1.setCombinerClass(AverageLetterReducer.class);
		job1.setReducerClass(AverageLetterReducer.class);
        
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		// Check Input globs
		System.out.println("\n\nInput glob... args[0]: " + args[0] + "\n\n");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1);
             
		
        // Set up Job12   
        Job job2 = Job.getInstance(conf, "AverageLetterFrequency Two");
        
        job2.setJarByClass(AverageLetterFrequency.class);
        
        job2.setMapperClass(FreqDistrbMapper.class);
        job2.setReducerClass(FreqDistrbReducer.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        // Set a custom configuration property that will track the language
        // being analysed for character frequency distribution in this 
        // Map Reduce process
        job2.getConfiguration().set("language.text", strLang);
        
        ChainMapper.addMapper(job2, FreqDistrbMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf);
        ChainReducer.setReducer(job2, FreqDistrbReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+ "_out"));
        
		ControlledJob cJob2 = new ControlledJob(conf);
		cJob2.setJob(job2);
                
        
        // Set up Job sequence execution
        Job[] jobs = new Job[]{job1, job2};
        for (Job job : jobs) {
          boolean success = job.waitForCompletion(true);       
          
          // Exit process if one of the jobs fails
          if (!success) {
            System.exit(1);
          }
          
        }
        
        // Add language description to the final output from the 
        // Reducer job after it has completed.
        
        System.out.println("\nPreparing to end Driver process...\n");           
              
        // No jobs have failed - exit process successfully 
        System.exit(0);

        
//		JobControl jobctrl = new JobControl("jobctrl");
//		jobctrl.addJob(cJob1);
//		jobctrl.addJob(cJob2);
//		cJob2.addDependingJob(cJob1);
//		
//		Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
//		jobRunnerThread.start();
//		
//		while (!jobctrl.allFinished()) {
//			System.out.println("Still running...");
//			Thread.sleep(5000);
//		}
//		System.out.println("Done!");
//		jobctrl.stop();
		
		// Set up Counter for Word sizes
		// Counter cCharCounter = job.getCounters().findCounter(AverageLetterMapper.LangCharCount.LANG_CHAR_COUNT);
		// System.out.println("Total number of characters is: " + cCharCounter.getValue());
		
		}

}
