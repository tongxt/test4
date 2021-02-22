package com.beifeng.hive;


import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class TestHiveJavaClient {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	 
	  /**
	   * @param args
	   * @throws SQLException
	   */
	  public static void main(String[] args) throws SQLException {
	      try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	      System.exit(1);
	    }
	    //replace "hive" here with the name of the user the queries should run as
	    Connection con = DriverManager.getConnection("jdbc:hive2://bigdata01-ibeifeng.com:10000/db_emp", "beifeng", "hdhdhfdhdfhdf");
	    Statement stmt = con.createStatement();
	    
	    String sql = "select ename , deptno from emp";
	    System.out.println("Running: " + sql);
	    ResultSet res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1) + "======>" + res.getString(2));
	    }
	  }
}
