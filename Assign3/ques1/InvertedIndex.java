package indexer;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.lang.StringBuilder;

import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

public class InvertedIndex {

  public static void main(String[] args) throws Exception 
  {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) 
		{
		System.err.println("Usage: Inverted Index <in> <out>");
		System.exit(3);
		}
        //initiating job1
		Job job1 = new Job(conf, "InvertedIndex");

		job1.setJarByClass(InvertedIndex.class);
        //setting mapper and reducer class for job1
		job1.setMapperClass(IndexMapper1.class);
		job1.setReducerClass(IndexReducer1.class);
        //setting the output key values
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
        //defining the file path for input and output files for job1
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		job1.waitForCompletion(true);
		//System.exit(job1.waitForCompletion(true) ? 0 : 1);
		//ControlledJob cJob1 = new ControlledJob(conf);
		//Job1.setJob(job1);
        //initiating job2
		Job job2 = new Job(conf, "InvertedIndex");

		job2.setJarByClass(InvertedIndex.class);
		//setting mapper and reducer class for job2
		job2.setMapperClass(IndexMapper2.class);
		job2.setReducerClass(IndexReducer2.class);
		
		job2.setMapOutputKeyClass(TermKey.class);
		job2.setMapOutputValueClass(TermValue.class);
		
		job2.setPartitionerClass(NaturalKeyPartitioner.class);
		job2.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job2.setSortComparatorClass(CompositeKeyComparator.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
		//defining the file path for input and output files for job2
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	  }
   }

