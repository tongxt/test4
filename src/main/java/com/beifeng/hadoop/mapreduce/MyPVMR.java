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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;







public class MyPVMR extends Configured implements Tool{
	//KEYIN, VALUEIN这个是map的输入，KEYOUT, VALUEOUT输出
public static class MyPVMap extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	IntWritable keyOut = new IntWritable();
	IntWritable valueOut = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String lineValue = value.toString();
		String[] keys = lineValue.split("\t");
		if(30>keys.length){
			Counter counter = context.getCounter("MyCount", "字段长度小于30");
			counter.increment(1);
			return;
		}
		String Url = keys[1];
		if(StringUtils.isBlank(Url)){
			Counter counter = context.getCounter("MyCount", "Url是空");
			counter.increment(1);
			return;
		}
		String proID = keys[23];
		if (StringUtils.isBlank(proID)){
			Counter counter = context.getCounter("MyCount", "是空白");
			counter.increment(1);
			return ;
		}
		Integer proid = Integer.MAX_VALUE;
		try {
			proid = Integer.valueOf(proID);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			Counter counter = context.getCounter("MyCount", "不是数字");
			counter.increment(1);
		}
			
		
		if(proid == 0 ){
			Counter counter = context.getCounter("MyCount", "省份ID是0？？");
			counter.increment(1);
			return;
		} 
	
			keyOut.set(proid);
			context.write(keyOut, valueOut);
		}
		
	}
	public static class MyPVReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		IntWritable OutValue = new IntWritable();
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			
			 int sum = 0;
				for (IntWritable value : values) {
					sum += value.get();
				}
				OutValue.set(sum);
				context.write(key, OutValue);
		}
		}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://bigdata-03:8020");
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		//这行没写可以运行，但是打包会报错~！！
		job.setJarByClass(MyMR.class);
		
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		FileSystem fs =  outpath.getFileSystem(conf);
		if(fs.exists(outpath)){
			fs.delete(outpath, true);
		}
		//map
		job.setMapperClass(MyPVMap.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		//reduce
		job.setReducerClass(MyPVReduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置combiner
		//job.setCombinerClass(MyCombiner.class);
		//设置
		//job.setPartitionerClass(cls);
		
		//job.setNumReduceTasks(2);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess?0:1;
		
	}
	public static void main(String[] args){
		args = new String[]{
				"hdfs://bigdata-04:8020/2015082818",
				"hdfs://bigdata-04:8020/user/beifeng/output3"
		};
		int status = 0;
		
		
		//status =ToolRunner.run(conf, new MyMR(), args);
		try {
		 status = new MyPVMR().run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(status);
	}
	
}
