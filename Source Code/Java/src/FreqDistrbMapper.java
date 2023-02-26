import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// This is the second (chained) Mapper process
// This reads the output from the first MapReduce Job, which is a file with  
// a list of each character found in the language book files on HDFS, along 
// with the count of the occurrence of those characters.
public class FreqDistrbMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(FreqDistrbMapper.class);

	// The purpose of this Map process is to create a Key/Value pair that uses a static key text to 
	// record a value that represents the total count of ALL characters and which will be used for
	// average frequency distribution calculations in the Job 2 Reducer function.
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String s = value.toString();
		String strSplit = "\\W+";
		String strTotalLabel = "Total_Chars"; // Static Key Value set to record ALL characters 
		
		// Each line in the Job 1 output is read.
		// Each line represents a character and character count
		String[] parts = s.split(strSplit);
		
		// The first line input represents the individual language character 
		String sCharCapture = parts[0].trim();
		// The second line represents the count of the character
		int iCharCntInt = Integer.parseInt(parts[1].trim());
		
		// 'Local' (non-English) characters will not be read - but their count is
		// The blank character is interpreted as one of these non-English characters
		// (such as ä, ö, ü) and they are grouped under a default key value
		if (sCharCapture.isEmpty()){
			sCharCapture = "Local_Char";
			LOG.info("Checking for empty key-value: " + sCharCapture + "-" + iCharCntInt);
		}
		
		// This line rewrites the character and count to preserve this data for the Job 2 Reducer
		context.write(new Text(sCharCapture), new IntWritable(iCharCntInt));
		// This line writes out a static key value to ensure the Reducer is fed a count of 
		// total characters read from the language book files.
		context.write(new Text(strTotalLabel), new IntWritable(iCharCntInt));	

	}
}	
