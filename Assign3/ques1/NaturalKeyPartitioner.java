package indexer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<TermKey, TermValue> {

	@Override
	public int getPartition(TermKey key, TermValue val, int numPartitions) 
	{
		int hash = key.getTerm().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}
