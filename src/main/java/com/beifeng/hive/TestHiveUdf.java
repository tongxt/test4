package com.beifeng.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * 需求：转换大小写，默认转换小写，0代表小写，1代表大写
 * @author ibf
 *
 */
public class TestHiveUdf extends UDF{

	public Text evaluate(Text str){
		return this.evaluate(str,new IntWritable(0));
	}
	
	//text表示这一列的值
	public Text evaluate(Text str,IntWritable flag){
		
		//先判断值是否为null
		if(str !=null ){
			if(flag.get() == 0){
				return new Text(str.toString().toLowerCase());
			}else if(flag.get() == 1){
				return new Text(str.toString().toUpperCase());
			} else 
				return null;
		}else
		return null;
	}
	
	public static void main(String[] args) {
		System.out.println(new TestHiveUdf().evaluate(new Text("hadoop"),new IntWritable(1)));
	}
}
