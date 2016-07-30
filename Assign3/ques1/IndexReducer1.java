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

public class IndexReducer1 extends Reducer<Text,Text,Text,Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, NoSuchElementException, ArithmeticException 
	{
	  int count=0;
	  for(Text val : values)
	  {
		count+=1;
	  }
	  try
	  {
		StringTokenizer str=new StringTokenizer(key.toString(),":");
		String a=str.nextToken();
		String y=str.nextToken()+":"+count;
		context.write(new Text(a), new Text(y));
	  }
	  catch(NoSuchElementException ex)
	  {
	  }
	}
}
