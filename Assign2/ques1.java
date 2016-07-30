package stackexchange;

import java.io.IOException;
import java.util.*;
import java.lang.ArithmeticException;
import java.lang.NullPointerException;
import java.lang.NumberFormatException;

import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;  
import org.apache.hadoop.hbase.util.Bytes;  

public class BulkUploader 
{
  public static class BloadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
  {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,NoSuchElementException, NullPointerException, NumberFormatException 
    {
    	String delim="=",delim_quote="\"";
    	String temp,family_name;
    	String rowkey;
    	ArrayList<String> post1_info=new ArrayList<String>(Arrays.asList("PostTypeId", "AcceptedAnswerId","CreationDate","Score","Body","Title","Tags","AnswerCount","CommentCount","FavoriteCount","ViewCount","ParentId"));
    	ArrayList<String> post1_editing_info=new ArrayList<String>(Arrays.asList("OwnerUserId","LastEditorUserId","LastEditDate","LastActivityDate"));
    	try
    	{
    		StringTokenizer itr = new StringTokenizer(value.toString());
    		while (itr.hasMoreTokens()) 
    		{
    			String token=itr.nextToken();
    			StringTokenizer eq1=new StringTokenizer(token.toString(),delim);
    			if(eq1.nextToken().equals("Id"))		// Check for "Id" token and use the value of it as rowid
    			{	
    				rowkey=eq1.nextToken().replace("\"","");
    				ImmutableBytesWritable HKey = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
    				Put HPut = new Put(Bytes.toBytes(rowkey));  
    				while(itr.hasMoreTokens())
    				{
    					String tmp="";
    					StringTokenizer token1=new StringTokenizer(itr.nextToken().toString(),delim);
    					String fam_token=token1.nextToken();
    					if(fam_token.equals("Body") || fam_token.equals("Title"))
    					{
    						String tk=itr.nextToken();
    						while(!tk.contains("\""))
    						{
    							tmp=tmp+" "+tk;
    							tk=itr.nextToken();
    						}
    						tmp=token1.nextToken()+" "+tmp+" "+tk;
    					}
    					if(post1_info.contains(fam_token))
   						{
   							family_name="PostInfo";
   						}
   						else if(post1_editing_info.contains(fam_token))
   							{
    							family_name="PostEditingInfo";
   							}
    						else
    						{
    							family_name="Info";
    						}
    					if(fam_token.equals("Body")|| fam_token.equals("Title"))
    					{
    						String xa=tmp.replace("\"","");
    	    				HPut.add(Bytes.toBytes(family_name),Bytes.toBytes(fam_token),Bytes.toBytes(xa));
    	    				context.write(HKey,HPut);
    					}
    					else
    					{
    						String x1=token1.nextToken();
    						String x=x1.replace("\"","");
    						HPut.add(Bytes.toBytes(family_name),Bytes.toBytes(fam_token),Bytes.toBytes(x));
    						context.write(HKey,HPut);
    					}
    				}	
    			}
    		}	
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
  
  public static void main(String[] args) throws Exception 
  {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) 
    {
      System.err.println("Usage: Bulk import of data <in> <out>");
      System.exit(2);
    }
    HTable hTable = new HTable(conf, otherArgs[2]);
    Job job = new Job(conf, "bulk_import");
    
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
    job.setMapOutputValueClass(Put.class); 
    
    job.setJarByClass(BulkUploader.class);
    job.setMapperClass(BloadMapper.class);
	
    job.setInputFormatClass (TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    job.setOutputFormatClass(HFileOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    HFileOutputFormat.configureIncrementalLoad(job, hTable);
    job.waitForCompletion(true);
    LoadIncrementalHFiles lihf = new LoadIncrementalHFiles(conf);
    lihf.doBulkLoad(new Path(otherArgs[1]), hTable);
  }
}