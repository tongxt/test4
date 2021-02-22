package com.beifeng.hbase.mapred;

import java.io.IOException;
import java.util.List;

import javax.swing.text.AbstractDocument.Content;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

public class HbaseMapper extends TableMapper<ImmutableBytesWritable,Put>{
	@Override
	protected void map(
			ImmutableBytesWritable key,
			Result value,
			Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		Put put = new Put(key.get());
		List<Cell> listCells = value.listCells();
		for (Cell cell : listCells) {
			if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
				if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					put.add(cell);
				}
			}
		}
		context.write(key, put);
	}
}
