package com.martinkl.warc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;   
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class HelloWarc  extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new HelloWarc(), args);
		System.exit(exitCode);

	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HelloWarc using built in mappers and reducers");
		job.setJarByClass(getClass());
				
		WARCFileReader warc = new WARCFileReader(job,new Path(args[0]));
		WARCRecord record = warc.read();		
		warc.close();
				
		return job.waitForCompletion(true)? 0 : 1;
	}

}