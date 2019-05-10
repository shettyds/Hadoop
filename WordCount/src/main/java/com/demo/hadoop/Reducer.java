package com.demo.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>
{
	public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
	{
		int sum = 0;
		for(IntWritable value : values)
		{
			sum += value.get();
		}
		con.write(word, new IntWritable(sum));
	}
}
