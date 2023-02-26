
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// The driver process for the average character frequency MapReduce process
// A single MR process is created and input files on HDFS are read based on 
// the command line (glob) input and the naming convention of the book files
// stored on HDFS.


// The book files are all stored with a naming convention of <bookname>_<language>
// For example 'TheFox_English'.
public class AverageLetterFrequency {

	public static void main(String[] args) throws Exception {
		
		// check command line is correctly - two parameters
		if (args.length != 2) {
			System.err.println("Usage: AverageLetterFrequency <input path> <output path>");
			System.exit(-1); }	
		
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
        
        
        // Set Up Mapper and Combiner
		job1.setMapperClass(AverageLetterMapper.class);
		job1.setCombinerClass(AverageLetterReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		
		// Set Up Partitioner
		job1.setPartitionerClass(AverageLetterPartitioner.class);
		
		// Set Up Reducer
		job1.setReducerClass(AverageLetterReducer.class);
		
		// Set number of reducer tasks
		job1.setNumReduceTasks(2);
        

		// Input and Output format for data
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		// Check Input globs
		System.out.println("\n\nInput glob... args[0]: " + args[0] + "\n\n");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		
		
		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1);
             
		
        // Set up Job2   
        Job job2 = Job.getInstance(conf, "AverageLetterFrequency Two");
        
        job2.setJarByClass(AverageLetterFrequency.class);
        
        // Set Up Mapper - no need for Combiner; data volumes are much lower 
        job2.setMapperClass(FreqDistrbMapper.class);
        job2.setReducerClass(FreqDistrbReducer.class);
        
        // Input and Output format for data
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        // Set a custom configuration property that will track the language
        // being analyzed for character frequency distribution in this 
        // Map Reduce process
        job2.getConfiguration().set("language.text", strLang);
        
        ChainMapper.addMapper(job2, FreqDistrbMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf);
        ChainReducer.setReducer(job2, FreqDistrbReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        
        // Job2 reads the files produced on HDFS by Job 1
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        // Job2 output is marked with a suffix
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+ "_out"));
        
		ControlledJob cJob2 = new ControlledJob(conf);
		cJob2.setJob(job2);
                
        
        // Set up Job sequence execution
		// The purpose of the chaining is that Job1 must first generate a count of each character 
		// read from the language book files.
		//
		// That data must be in place before Job2 is invoked to generate a count of ALL characters
		// read from the language book files and then calculate the average character distribution
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

		
		}

}
