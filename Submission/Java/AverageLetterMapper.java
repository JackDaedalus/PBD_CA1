import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageLetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(AverageLetterMapper.class);

	static enum LangCharCount{LANG_CHAR_COUNT};
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String s = value.toString();
		String strSplit = "";
		String sRegex = "^\\p{L}";
		HashMap<Character, Integer> charCountMap = new HashMap<Character, Integer>();
		
		System.out.println("In CA1:AverageLetterFrequency Mapper now!");
		
		// Mapper reads HDFS input line by line
        for (String sInput : s.split(strSplit)) {// Split input into characters
        	// Filter out grammar symbols but allow local language characters such as -  ä, ö, ü (for example)
			if (sInput.length() > 0 && sInput.matches(sRegex)) { 

				// Convert to lower case - analysis will not differentiate based on case 
				String strLCaseWord = sInput.toLowerCase();
				
		        char ch = strLCaseWord.charAt(0);
		            
		        // Check that character is valid alpha character - Log output for Hadoop dashboard
		        LOG.info("Pre-screening for Character.isLetter() Check..: " + ch);
		        
		        if (Character.isLetter(ch)) { // Exclude non-alphabet characters

			            // If the character is already in the map, increment its count...
			            if (charCountMap.containsKey(ch)) {
			                charCountMap.put(ch, charCountMap.get(ch) + 1);
			            }
			            // ...otherwise, add the character to the map with a count of 1
			            else {
			                charCountMap.put(ch, 1);
			            }
		         } 
		        
			}
        }	
        
        // Loop through the count of characters in the input read by the Map process
        for (Character ch : charCountMap.keySet()) {
        	
        	// Convert character to string for Mapper Output
        	String sCharInWord = String.valueOf(ch);
        	int ichrCnt = charCountMap.get(ch);
        	
        	// Set up data for logging output to first Mapper job
        	LOG.info("Mapper output key: " + sCharInWord);
            LOG.info("Mapper output values: " + ichrCnt);
        	
        	// Output from Mapper is the count of each character in the word
        	context.write(new Text(sCharInWord), new IntWritable(ichrCnt));

        }
			
		
	}
}
