package com.demo.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountTest {
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration c=new Configuration();
	String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
	Path input=new Path(files[1]);
	Path output=new Path(files[2]);
	Job j=new Job(c,"wordcount");
	j.setJarByClass(WordCountTest.class);
	j.setMapperClass(Mapper.class);
	j.setReducerClass(Reducer.class);
	j.setPartitionerClass(WordPartitioner.class);
	j.setNumReduceTasks(4);
	j.setOutputKeyClass(Text.class);
	j.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(j, input);
	FileOutputFormat.setOutputPath(j, output);
	System.exit(j.waitForCompletion(true)?0:1);	
}
}
