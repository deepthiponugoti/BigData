package indexer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {

	protected NaturalKeyGroupingComparator() 
	{
		super(TermKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) 
	{
		TermKey k1 = (TermKey)w1;
		TermKey k2 = (TermKey)w2;
		
		return k1.getTerm().compareTo(k2.getTerm());
	}
}
