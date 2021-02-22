package com.beifeng.hadoop.mapreduce;



import java.io.File;
import java.io.FileInputStream;

import javax.swing.filechooser.FileNameExtensionFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.google.common.io.Files;

public class MyPutOrCat {
	public static FileSystem getFileSystem() throws Exception {
		//creat configuration  default  and  site.xml
				Configuration configuration = new Configuration();
				//configuration.set("fs.defaultFS", "hdfs://bigdata-01:8020");
				//get filesystem
				FileSystem fileSystem = FileSystem.get(configuration);
		return fileSystem;
		
	}
	public void readFile() throws Exception {
		FileSystem fileSystem = getFileSystem();
		//read Path
		String filename = "/user/beifeng/input/1.txt";
		Path readPath = new Path(filename);
		
		//inputStream
		FSDataInputStream inStream = fileSystem.open(readPath);
		try {
			IOUtils.copyBytes(inStream, System.out, 4096,false);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(inStream);
		}
	}
	public static void main(String[] args) throws Exception {
		FileSystem fileSystem = getFileSystem();
		//write Path
		String filename = "/user/beifeng/input/2.txt";
		Path wPath = new Path(filename);
		FileInputStream inputStream = new FileInputStream(new File("/opt/datas/1.txt"));
		FSDataOutputStream outputStream = fileSystem.create(wPath);
		
		try {
			IOUtils.copyBytes(inputStream,outputStream, 4096,false);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
		}
		
	}
}
