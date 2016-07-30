package indexer;
import java.io.IOException;
import java.util.*;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexMapper1 extends Mapper<LongWritable, Text, Text, Text> 
{
    private final static Text word = new Text();
    private final static Text location = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,NoSuchElementException, NullPointerException, NumberFormatException 
	{
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line.toLowerCase());
	  ArrayList<String> filler=new ArrayList<String>(Arrays.asList("the","a","an","has","if","and","or","i","you","we","that","they","them","are", "as", "at", "be", "but", "by","for", "if", "in", "into", "is", "it","no", "not", "of", "on", "or", 
	  "such","that", "the", "their", "then", "there", "these","they", "this", "to", "was", "will", "with","your","yours","yourself","yourselves","cant","cannot","could",	  
	  "couldnt","did","didnt","do","does","doesnt","doing","dont","down","during","each","few","further","had","hadnt","has","hasnt","have","havent","having",
	   "he","hed","hes","her","here","heres","hers","herself","him","himself","his","how","hows","me","more","most","mustnt","my","myself"));
      while (itr.hasMoreTokens()) 
	  {
		String str=itr.nextToken();
		if(filler.contains(str))
		{
		}
		else
		{
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			str = str.toLowerCase();
			str=str.replaceAll("[^a-z0-9]", "");
			if(str.equals(""))
			{}
			else
			{
				String x=str+":"+fileName;
				word.set(x);
				String token="1";
				location.set(token);
				context.write(word, location);
			}
		}
      }
    }
}
