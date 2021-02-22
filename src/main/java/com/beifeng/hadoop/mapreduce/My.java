package com.beifeng.hadoop.mapreduce;

import org.apache.commons.lang.StringUtils;

public class My {
public static void main(String[] args) {
	boolean falses = StringUtils.isBlank("  -   ");
	System.out.println(falses);
}
}
