
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


// This is the second (chained) Reducer job
// The Job 2 Mapper function uses a static key value to allow for a count of all
// characters. This allows the Job 2 Reducer function here to calculate the 
// average frequent for each character and generate the final output 
public class FreqDistrbReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private static final Log LOG = LogFactory.getLog(FreqDistrbReducer.class);
	    private HashMap<Text, Integer> totals = new HashMap<Text, Integer>();
	    private String strTotalLabel = "Total_Chars";
	    private Text languageText;
	    
	    // The Driver function sets up a language text output for the final HSFS Reducer
	    // output based on the command line (glob) used to run the MR jar file 
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	// Read the language description used as the parameter to run the jar file 
	    	// from the command line.
	    	languageText = new Text(context.getConfiguration().get("language.text"));
	    }

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	            throws IOException, InterruptedException {
	        
	    	// This is similar to the reducer in Job 1 as a total is counted
	    	// of all occurrences of the Key values
	    	
	    	// For Job2, one of the inputs reflects the count of ALL characters,
	    	// which has been recorded against a static 'Total_Chars' key. 
	        int sum = 0;
	        
	        for (IntWritable value : values) {
	            sum += value.get();
	        }
	        
	        if (totals.containsKey(key)) {
	            sum += totals.get(key);
	        }
	        
	        totals.put(new Text(key.toString()), sum);
	    }
	    
	    
	    // This function is run to process the end result Reducer data and perform the 
	    // calculations to generate the frequency distribution of characters
	    protected void cleanup(Context context) throws IOException, InterruptedException {       
	    	float iTotalCharsCnt = 0;
	    	
	    	// Create a HashMap with the Reducer dataset
	    	for (Map.Entry<Text, Integer> entry : totals.entrySet()) {	        	
	        	String sChkTotal =  entry.getKey().toString().trim();
	  
	        	// Store the value that represents the total count of ALL
	        	// characters read by the MapReduce process
	        	if (sChkTotal.equals(strTotalLabel)){
	        		iTotalCharsCnt = entry.getValue();	
	        	}       
	        }
	    	
    		// Use the count of ALL characters to loop through the count for individual 
	    	// characters and produce a frequency distribution value for each one and 
	    	// output the result as the final Reducer result to HDFS.
	    	for (Map.Entry<Text, Integer> entry2 : totals.entrySet()) {      	
	        	float iEnt = (entry2.getValue());   	
	        	float iCalField = ((iEnt) / iTotalCharsCnt);
	        	int iDistrb = (int) (iCalField * 10000);
	        		        	
	        	// Output LOG data for the Hadoop dashboard logs on the second (Job2) 
	        	// reducer output.
	        	LOG.info("Working through second for-loop - Key-Value-Calc-Int: " 
	        										+ entry2.getKey()+ "-" 
	        										+ entry2.getValue() + "-" 
	        										+ iCalField + "-" + iDistrb);
	        	
	        	// Datatype manipulation to format output of Reducer job
	        	String sTxt1 = languageText.toString();
	        	String sTxt2 = entry2.getKey().toString().trim();
	        	String sTxt3 = sTxt1+sTxt2;
	        	Text keyText = new Text(sTxt3);
	        	
	        	// If character is very infrequent it can be ignored as a spurious 
	        	// inclusion in the language documents
	        	if (iDistrb > 10) { // Appears less that 0.1%
	        		//Generate final Job 2 Reducer outputs
	        		context.write(keyText, new IntWritable(iDistrb));      		
	        	}
	        }
	    }
}


