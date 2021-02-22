package com.beifeng.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MyPVMapReduce extends Configured implements Tool{
	//自定义的Map类
	//KEYIN, VALUEIN, KEYOUT, VALUEOUT
	//输入进来的key和value，输出的key和value
	public static class  MyPVMap extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		IntWritable keys = new IntWritable();
		IntWritable values = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			
			String[] words = line.split("	");
			if(words.length<23){
				context.getCounter("MyCount", "字段长度小于23").increment(1);
				return;
			}
			if(StringUtils.isBlank(words[1])){
				context.getCounter("MyCount", "url为空").increment(1);
				return;
			}
			String proid_String = words[23];
			int proid_int = Integer.MAX_VALUE;
			
			
			if(StringUtils.isBlank(proid_String)){
				context.getCounter("MyCount", "省份ID是否为空").increment(1);
				return;
			}
			
			try{
				proid_int = Integer.parseInt(proid_String);
				if(proid_int == 0 ){
					context.getCounter("MyCount", "省份ID是否为0").increment(1);
					return;
				}
			}catch(Exception e){
				context.getCounter("MyCount", "省份ID不是数字").increment(1);
				return;
			}
			
			keys.set(proid_int);
			context.write(keys, values);
		}
		
	}
	//自定义的Reduce类
	public static class MyPVReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		IntWritable values = new IntWritable();
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> value,Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable intWritable : value) {
				sum  = intWritable.get()+sum;
			}
			values.set(sum);
			context.write(key, values);
		}
			
		
		
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		args = new String[]{
				"hdfs://bigdata-04:8020/2015082818",
				"hdfs://bigdata-04:8020/user/beifeng/output"
		};
		int status = 0;
		
		
		//status =ToolRunner.run(conf, new MyMR(), args);
		//status = ToolRunner.run(conf, new MyMR(), args);
		try {
		 status = new MyPVMapReduce().run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(status);
	}

}
	public int run(String[] args) throws Exception {
			// TODO Auto-generated method stub
			
					//生成配置
					//Configuration conf = this.getConf();
					Configuration conf = new Configuration();
					//conf.set("fs.defaultFS", "hdfs://bigdata-04:8020");
					//生成Job任务
					Job job = Job.getInstance(conf, this.getClass()  
			                .getSimpleName());  
					//Job job = Job.getInstance(conf, "wc",);
					
					job.setJarByClass(MyPVMapReduce.class);
					
					
					//=========combiner===============
					
					//job.setCombinerClass(MyCombiner.class);
					//=============================
					
					//输入
					Path input = new Path(args[0]);
					FileInputFormat.addInputPath(job, input);
					
					//输出
					Path output = new Path(args[1]);
					FileOutputFormat.setOutputPath(job, output);
					
					//当output存在的时候，将他删除
					FileSystem fs = output.getFileSystem(conf);
					if(fs.exists(output)){
						fs.delete(output, true);
					}
					
					//map参数设置
					job.setMapperClass(MyPVMap.class);
					job.setMapOutputKeyClass(IntWritable.class);
					job.setMapOutputValueClass(IntWritable.class);
					
					
					//reduce参数设置
					job.setReducerClass(MyPVReduce.class);
					job.setOutputKeyClass(IntWritable.class);
					job.setOutputValueClass(IntWritable.class);
					
					boolean isSuccess = job.waitForCompletion(true);
				
					return isSuccess? 0:1;
		
	}
}



