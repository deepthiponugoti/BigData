package indexer;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.lang.ArithmeticException;
import java.lang.NullPointerException;
import java.lang.NumberFormatException;
import java.util.HashMap;
import java.util.Map;
import java.lang.StringBuilder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class IndexReducer2 extends Reducer<TermKey,TermValue,Text,Text>
{
	private MultipleOutputs<Text, Text> out;
 
	public void setup(Context context) 
	{
		out = new MultipleOutputs<Text, Text>(context);
	}
    public void reduce(TermKey key, Iterable<TermValue> values, Context context) throws IOException, InterruptedException, ArithmeticException 
	{
	  StringBuilder buf = new StringBuilder();
      for(TermValue val : values)
      {
		  String x=print(key,val);
		  buf.append(x);
      }
	  out.write(new Text((String)key.getTerm()), new Text(buf.toString()), generateFileName(new Text((String)key.getTerm())));
    }
	private String print(TermKey key, TermValue value)
	{
        StringBuilder builder = new StringBuilder();
        builder.append(value.getFile())          
			   .append(":")
               .append(value.getFrequency())
			   .append(" ");
        return builder.toString();
	}
	
	private String generateFileName(Text k) 
	{
		String key=k.toString();
		char f=key.charAt(0);
		String x=f+"-WORDS";
		return x;
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{
		out.close();
	}
}
