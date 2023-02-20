import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FreqDistrbMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(FreqDistrbMapper.class);

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String s = value.toString();
		String strSplit = "\\W+";
		String strTotalLabel = "Total_Chars";
		
		
		System.out.println("In CA1:FreqDistrbMapper Mapper now!");
		
		//String sDummyWord = "z";
		//int iDummyInt = 998101;
		
		String[] parts = s.split(strSplit);
		
		String sDummyWord = parts[0].trim();
		int iDummyInt = Integer.parseInt(parts[1].trim());
		
		if (sDummyWord.isEmpty()){
			sDummyWord = "Local_Char";
			LOG.info("Checking for empty key-value: " + sDummyWord + "-" + iDummyInt);
		}
		
		//LOG.info("Mapper output key-value: " + sDummyWord + "-" + iDummyInt);
			
		context.write(new Text(sDummyWord), new IntWritable(iDummyInt));
		context.write(new Text(strTotalLabel), new IntWritable(iDummyInt));

		

	}
	
}	
