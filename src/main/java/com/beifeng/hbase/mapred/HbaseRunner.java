package com.beifeng.hbase.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseRunner extends Configured implements Tool{

	
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration config = HBaseConfiguration.create();
		Job job = Job.getInstance(config, "mr-hbase");
				
		job.setJarByClass(HbaseRunner.class);     // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
		  "stu_info",        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  HbaseMapper.class,   // mapper
		  ImmutableBytesWritable.class,             // mapper output key
		  Put.class,             // mapper output value
		  job);
		
		TableMapReduceUtil.initTableReducerJob("t5", null, job);
		boolean b = job.waitForCompletion(true);
		return b?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();
		try{
			int status = ToolRunner.run(configuration, new HbaseRunner(), args);
			System.exit(status);
		}catch(Exception e){
			
		}
		
	}

}
