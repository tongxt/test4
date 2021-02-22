package com.beifeng.hive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 日期格式转换
 * 转换前："31/Aug/2015:00:04:37 +0800" 
 * 转换后：2015-08-31 00:00:37或者20150831000037
 * @author ibf
 *
 */
public class TestDateUdf extends UDF{
	
	//定义输入日期格式，傳遞給time進行解析
	public SimpleDateFormat inputDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	
	//定义输出日期格式
	public SimpleDateFormat outputDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public Text evaluate(Text time){
		
		String output = null;
		
		//判断列中是否有null
		if(time == null){
			return null;
		}
		
		if(StringUtils.isBlank(time.toString())){
			return null;
		}
		
		//去除數據中的雙引號
		String parser = time.toString().replaceAll("\"", "");
		
		//保存值
		try {
			Date parseDate = inputDate.parse(parser);
			output = outputDate.format(parseDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new Text(output);
	}
	public static void main(String[] args) {
		System.out.println(new TestDateUdf().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
	}
}
