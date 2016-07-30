package stackexchange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.lang.ArithmeticException;
import java.lang.NumberFormatException;
import java.util.Collections;
import java.lang.Object;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageAuAns {

//public static int acc_num=1,unacc_num=1;
	private static ArrayList<Integer> acceptid=new ArrayList<Integer>();
  public static class FirstMapper 
       extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
    private static IntWritable score=new IntWritable();
    private static IntWritable id=new IntWritable();
      
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,NoSuchElementException, NullPointerException, NumberFormatException 
    {
	String delim="=",delim_quote="\"";
	String temp1,temp2;
	try
	{
      int id1,i=0,k=0;
      int rowid;
      String temp3,token1="";
      StringTokenizer itr1 = new StringTokenizer(value.toString());
      while (itr1.hasMoreTokens()) {
    	  String token=itr1.nextToken();
	       // word.set(token);
	        if(token1.equals("PostTypeId=\"1\""))			//if post type is a question and if it has AcceptedAnswerId tag then add the value of it to the arraylist
	        {
	        	
	        	StringTokenizer eq=new StringTokenizer(token.toString(),delim);
	        	if(eq.nextToken().equals("AcceptedAnswerId"))
	        	{
	        		//acc_num++;       		
	        			temp1=eq.nextToken().replace("\"","");
	        			//score.set(Integer.parseInt(temp1));
	        			acceptid.add(Integer.parseInt(temp1));
	        			//context.write(acc,score);
	        			break;
	        	}
	        }
   	  
	       // word.set(token);
   	  StringTokenizer eq1=new StringTokenizer(itr1.nextToken().toString(),delim);
   	  token1=itr1.nextToken();
   	  if(eq1.nextToken().equals("Id") && token1.equals("PostTypeId=\"2\""))		//if post type is a answer then check for "score" tag and send that value as output
   	  {
    		  temp3=eq1.nextToken().replace("\"","");
 			  rowid=Integer.parseInt(temp3);
 			  id.set(rowid);
    		  while(itr1.hasMoreTokens())
    		  {
    			 StringTokenizer eq2=new StringTokenizer(itr1.nextToken().toString(),delim); 
    			 if(eq2.nextToken().equals("Score"))
    			 {
    				 temp2=eq2.nextToken().replace("\"","");
    				 id1=Integer.parseInt(temp2);
    				 score.set(id1);
    				 context.write(id,score);
    				 break;
    			 }
    		  }
    	  }
      }
	}
	catch(NullPointerException ex)
	{
			
	}
	catch(NoSuchElementException ex)
	{
		
	}
	catch(NumberFormatException ex)
	{
		
	}

    }
  }
  
  public static class SecondMapper 
  extends Mapper<IntWritable, IntWritable, Text, FloatWritable>{
private static FloatWritable score1=new FloatWritable();
private static FloatWritable score2=new FloatWritable();
private static Text acc=new Text("AcceptedScore_avg");
private static Text unacc=new Text("UnacceptedScore_avg");
//= new IntWritable(1);
//private Text word1 = new Text();
//private Text word1 = new Text();
 
public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException,NoSuchElementException, NullPointerException 
{
//String delim1="=",delim_quote1="\"";
String temp3,temp4;
int i=0;
try
{
	 i=0;
	 while(i<acceptid.size())
	 {
       if(Integer.parseInt(key.toString())==acceptid.get(i))		//check if the key is in the arraylist. If it is, then the score is accepted score.
       {
       		String x=value.toString();
    	   	score1.set(Float.parseFloat(x));
       		context.write(acc,score1);
         	break;
       	}
       else
       {
    	  i++;
       }
     }
	 if(i==acceptid.size())				//else unaccepted score
	 {
		 String y=value.toString();
 	   	 score2.set(Float.parseFloat(y));
		 //score2.set(value);
		 context.write(unacc,score2);
	 }
}			
catch(NullPointerException ex)
{
		
}

}
}

public static class SecondReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> 
{
private FloatWritable result2 = new FloatWritable();

public void reduce(Text key, Iterable<FloatWritable> values, 
                  Context context
                  ) throws IOException, InterruptedException, ArithmeticException {
 float sum = 0;
 float avg=0,num=0;
 //Text average1=new Text("Average_of_unaccepted_answers");
 for (FloatWritable val : values) 		//aggregate all the values for a key and find the average
 {
   sum += val.get();
   num+=1;
 }
 avg=sum/num;
 result2.set(avg);
 context.write(key,result2);
}
}


  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    //conf1.setStrings("fs.default.name", "http://shark25.pstl.uh.edu:47345");
    String[] otherArgs1 = new GenericOptionsParser(conf1, args).getRemainingArgs();
    if (otherArgs1.length != 2) {
      System.err.println("Usage: average number of accepted and unaccepted answers<in> <out>");
      System.exit(2);
    }

    Job job1 = new Job(conf1, "average_number_of_accepted_unaccepted_answers");
    //Job job2 = new Job(conf1, "average_number_of_unaccepted_answers");
    Configuration FirstMapConfig = new Configuration(false);
    ChainMapper.addMapper(job1, FirstMapper.class, LongWritable.class,Text.class, IntWritable.class, IntWritable.class, FirstMapConfig);	//using a chain mapper class
    Configuration SecondMapConfig = new Configuration(false);
    ChainMapper.addMapper(job1, SecondMapper.class, IntWritable.class,IntWritable.class, Text.class, FloatWritable.class,SecondMapConfig);
    
    job1.setJarByClass(AverageAuAns.class);
    //job1.setMapperClass(FirstMapper.class);
    job1.setCombinerClass(SecondReducer.class);
    job1.setReducerClass(SecondReducer.class);

    job1.setInputFormatClass (TextInputFormat.class);
    FileInputFormat.addInputPath(job1, new Path(otherArgs1[0]));

    FileOutputFormat.setOutputPath(job1, new Path(otherArgs1[1]));
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(FloatWritable.class);


    System.exit(job1.waitForCompletion(true) ? 0 : 1);

    //System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
