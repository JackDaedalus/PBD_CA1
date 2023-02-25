
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
			
			LOG.info("numPars: 2 - Partitioner input key (as String): " + partitionKey);
			LOG.info("numPars: 2 - Partitioner input key (as Char(0)): " + partitionKey.charAt(0));
			
			// Compare input key character against vowel string
			//if (partitionKey.charAt(0) > 'a')
			if (vowelCharsToMatch.contains(String.valueOf(partitionKey.charAt(0)))){
				LOG.info("numPars: 2 - Partition 0");
				return 0;
			}
			else {
				LOG.info("numPars: 2 - Partition 1");
				return 1;
			}
		} else if (numPartitions == 1) {
			
			LOG.info("numPars: 1 - Partitioner input key: " + key.toString());
			return 0;
		}
		else {
			System.err.println("AverageLetterPartitioner can only handle 1 or 2 partitions");
			return 0;
		}
	}
}
