package com.demo.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

public class WordPartitioner extends org.apache.hadoop.mapreduce.Partitioner<Text, IntWritable> {

	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	public int getPartition(Text text, IntWritable arg1, int arg2) {
		if(text.toString().length()==1)
			return 0;
		else if(text.toString().length()==2)
			return 1;
		else if (text.toString().length()==3)
			return 2;
		else 
			return 3;
		
	}

}
