package com.demo.vowel;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VowelMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String words = value.toString().toUpperCase();
		for(String word: words.split(" "))
		{
			for(int i =0;i<word.length();i++)
			{
				Text outputKey = new Text(word);
				if(word.charAt(i) == 'A' || word.charAt(i)=='E' || word.charAt(i)=='I' || word.charAt(i)=='O' || word.charAt(i)=='U')
				{
					context.write(outputKey, outputKey);
				}
			}
		}
	}
}
