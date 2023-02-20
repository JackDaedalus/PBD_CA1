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
		//String strSplit = "\\W+";
		String strSplit = "";
		String sRegex = "^\\p{L}";
		HashMap<Character, Integer> charCountMap = new HashMap<Character, Integer>();
		
		//System.out.println("In CA1:AverageLetterFrequency Mapper now!");
		
		
        for (String sInput : s.split(strSplit)) {
			if (sInput.length() > 0 && sInput.matches(sRegex)) {
				
				// Convert Word to lower case
				String strLCaseWord = sInput.toLowerCase();
				
				// Display Word
				//System.out.println("Letter in sentence is = " + strLCaseWord);
        
		        // iterate through each character in the word
		        //for (int i = 0; i < strLCaseWord.length(); i++) {


		        //char ch = strLCaseWord.charAt(i);
		        char ch = strLCaseWord.charAt(0);
		            
		        // Check that character is valid alpha character
		        if (Character.isLetter(ch)) {
			            // if the character is already in the map, increment its count
			            if (charCountMap.containsKey(ch)) {
			                charCountMap.put(ch, charCountMap.get(ch) + 1);
			            }
			            // otherwise, add the character to the map with a count of 1
			            else {
			                charCountMap.put(ch, 1);
			            }
		         } // Char is alpha if
		        
		        //}
		        
			}
        }	
        
        // print the character count map
        // System.out.println("Character count for sentence \"" + sStrLine + "\":");
        for (Character ch : charCountMap.keySet()) {
        	
        	// Convert character to string for Mapper Output
        	String sCharInWord = String.valueOf(ch);
        	int ichrCnt = charCountMap.get(ch);
        	
        	// Do not count very infrequent 'strange' characters
        	
        	//LOG.info("Mapper output key: " + sCharInWord);
//          //LOG.info("Mapper output values: " + ichrCnt);
        	
        	// Output from Mapper is the count of each character in the word
        	context.write(new Text(sCharInWord), new IntWritable(ichrCnt));
        	

        	// Convert character to string for Mapper Output
        	//String sCharInWord = String.valueOf(ch);
        	
        	
            //System.out.println(sCharInWord + " = " + charCountMap.get(ch));
        }
		

//		for (String word : s.split(strSplit)) {
//			if (word.length() > 0) {
//				
//				// Convert Word to lower case
//				String strLCaseWord = word.toLowerCase();
//				
//				// Count letters in each word
//				// iterate through each character in the word
//		        //for (int i = 0; i < strLCaseWord.length(); i++) {
//		        	
//
//		        //char ch = strLCaseWord.charAt(i);
//		        char ch = strLCaseWord.charAt(0);
//		            
//		        // Check that character is valid alpha character
//		        if (Character.isLetter(ch)) {
//			            // If the character is already in the map, increment its count
//			            if (charCountMap.containsKey(ch)) {
//			                charCountMap.put(ch, charCountMap.get(ch) + 1);
//			            }
//			            // otherwise, add the character to the map with a count of 1
//			            else {
//			                charCountMap.put(ch, 1);
//			            }
//		         }
//		        //}
//		        
//		        
//		        
//		        // Mapper writes out count of each character in the word 
//		        for (Character ch : charCountMap.keySet()) {
//					// Output from Mapper is the first character and length of word
//		        	// System.out.println(ch + " = " + charCountMap.get(ch));
//		        	
//		        	// Convert character to string for Mapper Output
//		        	String sCharInWord = String.valueOf(ch);
//		        	int ichrCnt = charCountMap.get(ch);
//		        	
//		            LOG.info("Mapper output key: " + sCharInWord);
//		            //LOG.info("Mapper output values: " + ichrCnt);
//		        	
//		            // Output from Mapper is the count of each character in the word
//					context.write(new Text(sCharInWord), new IntWritable(ichrCnt));
//					
//					// Count each character output - for use in average calculation in Reducer
//					// REDUNDANT as MapReduce already calculates this value
//					// context.getCounter(LangCharCount.LANG_CHAR_COUNT).increment(1);
//		            
//		        }
//				
//				
//				
//				// Select the first character of word 
//				//char firstCharInWord = word.charAt(0);
//				//String sFirstChar = String.valueOf(firstCharInWord);
//				
//				// Calculate the length of the word
//				//int wlength = word.length();
//				
//				// Use Word length to drive Counters
//				//if (wlength < 4) {
//				//	context.getCounter(WordSize.SMALL_W).increment(1);
//				//} else if ((wlength > 3) && (wlength < 6)) {
//				//	context.getCounter(WordSize.MEDIUM_W).increment(1);
//				//} else {
//				//	context.getCounter(WordSize.LARGE_W).increment(1);
//				//}
//				
//				//context.write(new Text(word), new IntWritable(1));
//				
//				// Output from Mapper is the first character and length of word
//				// context.write(new Text(sFirstChar), new IntWritable(wlength));
//
//			}
//		}
		
		
		
		
	}
}
