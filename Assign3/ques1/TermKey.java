package indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class TermKey implements WritableComparable<TermKey> {

	private String term;
	private int frequency; 
	public TermKey(){}
	
	public TermKey(String term, int frequency) 
	{
		this.term = term;
		this.frequency = frequency;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder())
				.append(frequency)
				.append(' ')
				.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		term = WritableUtils.readString(in);
		frequency = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, term);
		out.writeInt(frequency);
	}

	@Override
	public int compareTo(TermKey o) 
	{
		int result = term.compareTo(o.term);
		if(result==0) {
			result = this.frequency < o.frequency ? -1 : (this.frequency==o.frequency ? 0 : 1);
		}
		return result;
	}

	public String getTerm() {
		return (String)term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

}
