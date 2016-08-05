package indexer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {


	protected CompositeKeyComparator() 
	{
		super(TermKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) 
	{
		TermKey k1 = (TermKey)w1;
		TermKey k2 = (TermKey)w2;
		
		int result = k1.getTerm().compareTo(k2.getTerm());
		if(result==0) 
		{
			result = -1* (k1.getFrequency() < k2.getFrequency() ? -1 : (k1.getFrequency()==k2.getFrequency() ? 0 : 1));
		}
		return result;
	}
}
