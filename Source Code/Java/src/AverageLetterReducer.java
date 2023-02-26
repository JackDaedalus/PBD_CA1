
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class AverageLetterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(AverageLetterReducer.class);

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		List<IntWritable> valueList = new ArrayList<IntWritable>();
		
		
		// This code block takes the Iterable list input and reads into 
		// a new list
		for (IntWritable val : values) {
            valueList.add(new IntWritable(val.get()));
        }
		
		// The purpose of this code is to read the key/value inputs into
		// new variables and use the LOG function to ouput to the Hadoop
		// dashboard to show that the Mapper input to the first Reducer 
		// process is correct.
        StringBuilder sb = new StringBuilder();
        for (IntWritable v : valueList) {
            sb.append(v.get());
            sb.append(", ");
        }
        if (sb.length() > 0) {
            sb.delete(sb.length() - 2, sb.length());
        }

        // Output to log file in Hadoop dashboard
        String valuesAsString = sb.toString();
        LOG.info("Reducer input key: " + key);
        LOG.info("Reducer input values: " + valuesAsString);
	    
        
        // Initialize counter variable
		int iChrCount = 0;
	    
	    // Count the total number of characters in the book
		// The use of the Iterable list in the LOG output requires
		// the use of the 'new' list to provide Reducer function
		// output.
		//
		// (Once the Mapper Iterable list is processed for the LOG
		// it cannot be reset).
		for (IntWritable vi : valueList) {
			
			// Read the Mapper/Combiner output and increment counter
			iChrCount += vi.get();
	    	
	    }

	    
	    // Write Reducer Output - ignore very infrequent characters
		// If, after reading entire books in the language, the character
		// count is less than 5 then the character can be considered spurious
		if (iChrCount > 5) { 
			context.write(key, new IntWritable(iChrCount));
		}
	}

}