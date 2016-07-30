package indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class TermValue implements WritableComparable<TermValue> {

	private String file;
	private int frequency;
	
	public TermValue(){}
	
	public TermValue(String file, int frequency) 
	{
		this.file = file;
		this.frequency = frequency;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		file = WritableUtils.readString(in);
		frequency = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, file);
		out.writeInt(frequency);
	}

	@Override
	public int compareTo(TermValue o) 
	{
		return -1;
	}

	public String getFile() {
		return (String)file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

}
