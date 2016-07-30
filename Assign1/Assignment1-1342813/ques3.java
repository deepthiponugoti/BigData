package stackexchange;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.lang.ArithmeticException;
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

public class AnsperId {

//int num=1;
  public static class TokenizerMapper 
       extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
    private static IntWritable one= new IntWritable(1);
    private IntWritable id=new IntWritable();
    //= new IntWritable(1);
    private Text word = new Text();
    //private Text word1 = new Text();
      
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,NoSuchElementException, NullPointerException 
    {
	String delim="=",delim_quote="\"";
	String temp,subtoken,idnum;
	try
	{
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) 
      {
    	String token=itr.nextToken();
    	if(token.equals("PostTypeId=\"2\""))				//check for the post type of answer and send the value of "OwnerUserId" tag as output
    	{	//num++;
    		while(itr.hasMoreTokens())
    		{
    			word.set(itr.nextToken());
    			StringTokenizer eq=new StringTokenizer(word.toString(),delim);
    			subtoken=eq.nextToken();
    			if(subtoken.equals("OwnerUserId"))
    			{
    				idnum=eq.nextToken();
    				temp=idnum.replace("\"","");
    				id.set(Integer.parseInt(temp));
    				context.write(id,one);
    				break;
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

    }
  }
  
  
  public static class AnsReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> 
  {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException, ArithmeticException 
    {
      int count = 0;
      //float avg=0;
      //float num=0;
      //Text num_ans=new Text("No_of_answers_per_Userid");
      for (IntWritable val : values) 		//Aggregate all the values for a owneruserid and sum them
      {
        count += val.get();
        //num+=1.0;
      }
      //avg=sum/(num);
      result.set(count);
	  context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) 
    {
      System.err.println("Usage: Number of answers per userid <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "Number_of_answers_per_id");
    job.setJarByClass(AnsperId.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(AnsReducer.class);
    job.setReducerClass(AnsReducer.class);

    job.setInputFormatClass (TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}