package stackexchange;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.lang.ArithmeticException;
import java.lang.NullPointerException;
import java.lang.NumberFormatException;
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

public class AverageAns {

//int num=1;
  public static class TokenizerMapper 
       extends Mapper<LongWritable, Text, Text, FloatWritable>{
    private static FloatWritable count;
    private static Text ans=new Text("AnswerCount");
    //= new IntWritable(1);
    private Text word = new Text();
    //private Text word1 = new Text();
      
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,NoSuchElementException, NullPointerException, NumberFormatException 
    {
	String delim="=",delim_quote="\"";
	String temp;
	try
	{
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) 
      {
    	String token=itr.nextToken();
    	if(token.equals("PostTypeId=\"1\""))		//if post type is a question then check for AnswerCount and send the count as output
    	{	//num++;
    		while(itr.hasMoreTokens())
    		{
    			word.set(itr.nextToken());
    			StringTokenizer eq=new StringTokenizer(word.toString(),delim);       
    			if(eq.nextToken().equals("AnswerCount"))
    			{
    				count=new FloatWritable();
    				temp=eq.nextToken().replace("\"","");
        	//split(delim_quote);
    				count.set(Float.parseFloat(temp));
    				context.write(ans, count);
    				break;
    			}
    		}
    	}
      }
	}			
	catch(NoSuchElementException ex)
	{
		//count.set(Float.parseFloat("0"));
		//context.write(ans,count);
	}
	catch(NullPointerException ex)
	{
			
	}
	
	catch(NumberFormatException ex)
	{
		
	}

    }
  }
  
  
  public static class AvgReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> 
  {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException, ArithmeticException {
      float sum = 0;
      float avg=0,num=0;
      Text average=new Text("Average_no_of_answers"); 		//Add all the answercounts and perform average
      for (FloatWritable val : values) {
        sum += val.get();
        num+=1.0;
      }
      avg=sum/(num);
      result.set(avg);
	  context.write(average,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: average number of answers per question <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "average_number_of_answers");
    job.setJarByClass(AverageAns.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(AvgReducer.class);
    job.setReducerClass(AvgReducer.class);

    job.setInputFormatClass (TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}