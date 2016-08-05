package indexer;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.lang.ArithmeticException;
import java.lang.NullPointerException;
import java.lang.NumberFormatException;
import java.lang.StringBuilder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexMapper2 extends Mapper<LongWritable, Text, TermKey, TermValue> 
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,NoSuchElementException, NullPointerException, NumberFormatException 
	{
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line.toLowerCase());
      while (itr.hasMoreTokens()) 
	  {
		String term=itr.nextToken();
		StringTokenizer token=new StringTokenizer(itr.nextToken().toString(), ":");
		String file=token.nextToken();
		String freq=token.nextToken();
        TermKey k1=new TermKey(term,Integer.parseInt(freq));
		TermValue v1=new TermValue(file,Integer.parseInt(freq));
        context.write(k1, v1);
      }
    }
}
