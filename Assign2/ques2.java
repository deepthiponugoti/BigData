package stackexchange;

import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.lang.ArithmeticException;
import java.lang.NullPointerException;
import java.lang.NumberFormatException;
import java.lang.IllegalArgumentException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;  
import org.apache.hadoop.hbase.util.Bytes; 
import org.apache.hadoop.hbase.filter.CompareFilter.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.TableName;

public class HbaseAverage 
{
  public static class HbaseMapper extends TableMapper<Text, IntWritable>
  {
	private static IntWritable count;    
    
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, IllegalArgumentException, InterruptedException,NoSuchElementException, NullPointerException, NumberFormatException 
    {
    	Text word=new Text("AverageAns");
		try
    	{
    		count=new IntWritable();
    		String results=new String(values.getValue(Bytes.toBytes("PostInfo"),Bytes.toBytes("AnswerCount")));
    		count= new IntWritable(Integer.parseInt(results));
    		context.write(word,count);
    	}
    	catch(NoSuchElementException ex)
    	{
			
    	}
    	catch(NullPointerException ex)
    	{
    			
    	}	
    	catch(NumberFormatException ex)
    	{
    		
    	}
    }
  
  }
  public static class HbaseReducer extends Reducer<Text,IntWritable,Text,FloatWritable> 
  {
	private FloatWritable result = new FloatWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException, ArithmeticException 
    {
      float sum = 0;
      float avg=0,num=0;
      Text average=new Text("Average_no_of_answers");
      //Add all the answercounts and perform average
      for (IntWritable val : values) 
      {
        sum += val.get();
        num+=1.0;
      }
      avg=sum/(num);
      result.set(avg);
	  context.write(average,result);
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = HBaseConfiguration.create();
    Job job = new Job(conf, "AverageAns");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) 
    {
      System.err.println("Usage: Average answers using hbase <in> <out>");
      System.exit(2);
    }
    Scan scan=new Scan();
    scan.setCaching(500);        
    scan.setCacheBlocks(false);
	scan.addFamily(Bytes.toBytes("PostInfo"));
    FilterList li=new FilterList(FilterList.Operator.MUST_PASS_ALL);
    SingleColumnValueFilter filter=new SingleColumnValueFilter(Bytes.toBytes("PostInfo"),Bytes.toBytes("PostTypeId"),CompareOp.EQUAL,Bytes.toBytes("1"));
    li.addFilter(filter);
    scan.setFilter(li);
    
    job.setMapOutputKeyClass(Text.class);  
    job.setMapOutputValueClass(IntWritable.class); 
    
    job.setJarByClass(HbaseAverage.class);
    
    TableMapReduceUtil.initTableMapperJob(
    		otherArgs[1],      // input table
    		scan,	          // Scan instance to control CF and attribute selection
    		HbaseMapper.class,   // mapper class
    		Text.class,	          // mapper output key
    		IntWritable.class,	          // mapper output value
    		job);
    job.setReducerClass(HbaseReducer.class);
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    System.exit(job. waitForCompletion(true) ? 0 : 1);
  }
}