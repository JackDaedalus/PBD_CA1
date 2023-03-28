
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class AverageLetterPartitioner extends Partitioner<Text, IntWritable>{
	private static final Log LOG = LogFactory.getLog(AverageLetterPartitioner.class);
	String partitionkey;
	
	@Override
	// Input is partitioned into different Reducers based on a check as to 
	// whether the character is a vowel or not.
	//
	// Vowels are more commonly used but are a small subset of a Latin alphabet
	// The Reducers are split between processing vowels and non-vowels
	public int getPartition(Text key, IntWritable value, int numPartitions){
		
		// Set up vowel string
		String vowelCharsToMatch = "aeiou";
		
		if(numPartitions == 2){
			String partitionKey = key.toString();
			
			// Compare input key character against vowel string
			if (vowelCharsToMatch.contains(String.valueOf(partitionKey.charAt(0)))){
				LOG.info("numPars: 2 - Partition 0"); // Send Output to Log to indicate Partitioner is working 
				return 0; // Partition 0 will be used for vowels
			}
			else {
				LOG.info("numPars: 2 - Partition 1"); // Send Output to Log to indicate Partitioner is working 
				return 1; // Partition 1 will be used for non-vowels
			}
		} else if (numPartitions == 1) { // Default
			return 0;
		}
		else {
			System.err.println("AverageLetterPartitioner can only handle 1 or 2 partitions");
			return 0;
		}
	}
}
