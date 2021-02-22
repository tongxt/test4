package com.beifeng.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
		IntWritable values = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable intWritable : value) {
				sum  = intWritable.get()+sum;
			}
			values.set(sum);
			System.out.println(key.toString() + "--->"+sum);
			context.write(key, values);
		}
}


