
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class FreqDistrbReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(FreqDistrbReducer.class);


	    private HashMap<Text, Integer> totals = new HashMap<Text, Integer>();
	    private String strTotalLabel = "Total_Chars";

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	            throws IOException, InterruptedException {
	        
	        int sum = 0;
	        
	        for (IntWritable value : values) {
	            sum += value.get();
	        }
	        
	        if (totals.containsKey(key)) {
	            sum += totals.get(key);
	        }
	        
	        totals.put(new Text(key.toString()), sum);
	    }
	    
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        
	    	float iTotalCharsCnt = 0;
	    	
	    	for (Map.Entry<Text, Integer> entry : totals.entrySet()) {
	        	
	        	//LOG.info("Working through key-value Hash-Map in Reducer: " + entry.getKey());
	        	String sChkTotal =  entry.getKey().toString().trim();
	        	
	        	if (sChkTotal.equals(strTotalLabel)){
	        		
	        		LOG.info("Still Working through key-value Hash-Map in Reducer: " + entry.getKey() + "-" + entry.getValue());
	        		
	        		int iNewVal =  ((entry.getValue()) * 2);
	        		iTotalCharsCnt = entry.getValue();
	        		
	        		LOG.info("Changed Totals Value to (x2): " + entry.getKey() + "-" + iNewVal);
	        		
	        	}
	            
	            
	        }
	    	
	    	for (Map.Entry<Text, Integer> entry2 : totals.entrySet()) {
	        	
	        	LOG.info("Working through second for-loop - Total Chars is: " + iTotalCharsCnt);
	        	
	        	//int iCalField = Math.round((entry2.getValue() / iTotalCharsCnt) * 100);
	        	float iEnt = (entry2.getValue());
	        	
	        	float iCalField = ((iEnt) / iTotalCharsCnt);
	        	
	        	int iDistrb = (int) (iCalField * 10000);
	        	
	        	LOG.info("Working through second for-loop - Key-Value-Calc-Int: " + entry2.getKey()+ "-" + entry2.getValue() + "-" + iCalField + "-" + iDistrb);
	        		        	
	            //context.write(entry2.getKey(), new IntWritable(entry2.getValue()));
	        	context.write(entry2.getKey(), new IntWritable(iDistrb));
	            
	            
	        }
	    }
}


