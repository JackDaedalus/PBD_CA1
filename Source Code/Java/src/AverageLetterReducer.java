
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class AverageLetterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(AverageLetterReducer.class);

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("In CA1: Feb 19 AverageLetterFrequency Reducer now!");

		
		//System.out.println("Attempt to copy Reducer list input...");
		List<IntWritable> valueList = new ArrayList<IntWritable>();
		
		for (IntWritable val : values) {
            valueList.add(new IntWritable(val.get()));
        }
		
        StringBuilder sb = new StringBuilder();
        for (IntWritable v : valueList) {
            sb.append(v.get());
            sb.append(", ");
        }
        if (sb.length() > 0) {
            sb.delete(sb.length() - 2, sb.length());
        }
        
        //System.out.println("Build List String for Reducer log output...");
        String valuesAsString = sb.toString();
		
        //LOG.info("Reducer input key: " + key);
        //LOG.info("Reducer input values: " + valuesAsString);
		
		// Code to add up all the values in the Sort/Shuffle Output 
		// and calculate the total value for each character in the language
	    
	    //System.out.println("Starting Main Reducer Loop now!");
	    
		int iChrCount = 0;
	    
	    // Count the total number of characters in the book
		for (IntWritable vi : valueList) {
	      
	      // sum += vi.get();
	      // count++;
			iChrCount += vi.get();
	    	
	    }
		
		    
	    //System.out.println("Calculating Average after Main Reducer Loop now!");
	    //int average = sum / count;
	    //System.out.println("Successfull divide by Zero in Main Reducer Loop now!");
	    
	    // Write Reducer Output - ignore very infrequent characters
		if (iChrCount > 20) {
			context.write(key, new IntWritable(iChrCount));
		}
	}

}