package com.beifeng.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataJoinMapReduce extends Configured implements Tool {

	// 预期的结果：
	// 001 29000

	// step 1: Mapper Class
	//输出key就是CID，value就是Cinfo或者orderinfo
	public static class JoinMapper extends Mapper<LongWritable, Text, LongWritable, DataJoinWritable> {

		private LongWritable mapOutputKey = new LongWritable();
		private DataJoinWritable mapOutputValue = new DataJoinWritable();

		// map就是从文件中读取数据的<keyvalue>，默认按照文件中一行行读取的
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 第一步：获取每一行的值
			String lineValue = value.toString();

			// 第二步：分割单词，变成<hadoop,1> <spark,1>
			String[] vals = lineValue.split(",");

			int length = vals.length;
			if ((3 != length) && (4 != length)) {
				return;
			}

			// get cid
			Long cid = Long.valueOf(vals[0]);
			// get name
			String name = vals[1];

			// set customer
			if (3 == length) {
				String phone = vals[2];
				// set
				mapOutputKey.set(cid);
				mapOutputValue.set("customer", name + "," + phone);
			}

			// ser order
			if (4 == length) {
				String price = vals[2];
				String date = vals[3];
				// set
				mapOutputKey.set(cid);
				mapOutputValue.set("order", name + "," + price + "," + date);
				System.out.println("<" + mapOutputKey + "," + mapOutputValue + ">");
			}
			context.write(mapOutputKey, mapOutputValue);

		}

	}

	// step 2: Reducer Class
	public static class JoinReducer extends Reducer<LongWritable, DataJoinWritable, NullWritable, Text> {

		private Text outputValue = new Text();

		@Override
		protected void reduce(LongWritable key, Iterable<DataJoinWritable> values, Context context)
				throws IOException, InterruptedException {

			String customerInfo = null;
			List<String> orderList = new ArrayList<String>();

			for (DataJoinWritable value : values) {
				if ("customer".equals(value.getTag())) {
					customerInfo = value.getData();
				} else if ("order".equals(value.getTag())) {
					orderList.add(value.getData());
				}
			}

			for (String order : orderList) {
				// set output value
				outputValue.set(key.get() + "," + customerInfo + "," + order);
				System.out.print(key.get() + " ");
				// output
				context.write(NullWritable.get(), outputValue);
			}
		}

	}

	/**
	 * Execute the command with the given arguments.
	 * 
	 * @param args
	 *            command specific arguments.
	 * @return exit code.
	 * @throws Exception
	 *             int run(String [] args) throws Exception;
	 */

	// step 3: Driver
	public int run(String[] args) throws Exception {

		// 封装了hadoop的配置项
		Configuration configuration = this.getConf();

		// 设置job
		Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
		// mapreduce程序jar的入口
		job.setJarByClass(this.getClass());

		// input path
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);

		// output path
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);

		// Mapper
		job.setMapperClass(JoinMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DataJoinWritable.class);

		// ==============shuffle=================
		// 1、分区
		// job.setPartitionerClass(cls);

		// 2、排序
		// job.setSortComparatorClass(cls);

		// 3、分区
		// job.setGroupingComparatorClass(cls);

		// 4、combiner
		// job.setCombinerClass(WordCountCombiner.class);
		// ==============shuffle=================

		// Reducer
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 设置reduce数目
		// job.setNumReduceTasks(2);

		// submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		args = new String[] { "hdfs://bigdata01-ibeifeng.com:8020/join/input",
				"hdfs://bigdata01-ibeifeng.com:8020/join/out" };

		Configuration configuration = new Configuration();
		// configuration.set("mapreduce.map.output.compress", "true");
		// configuration.set("mapreduce.map.output.compress.codec",
		// "org.apache.hadoop.io.compress.SnappyCodec");

		int status = ToolRunner.run(configuration, new DataJoinMapReduce(), args);

		System.exit(status);
	}

}
