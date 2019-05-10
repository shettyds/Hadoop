package com.demo.vowel;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VowelReducer extends Reducer<Text,Text, Text,IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		
			String word = value.iterator().next().toString().toUpperCase();
			for(int i =0;i<word.toString().length();i++)
			{
				if(word.charAt(i) == 'A' || word.charAt(i)=='E' || word.charAt(i)=='I' || word.charAt(i)=='O' || word.charAt(i)=='U')
				{
					sum++;
				}
			}
		context.write(key, new IntWritable(sum));;
	}
}
