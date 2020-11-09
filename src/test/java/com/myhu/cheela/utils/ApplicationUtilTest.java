package com.myhu.cheela.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class ApplicationUtilTest {
	ApplicationUtil appUtil=new ApplicationUtil();
	static SparkConf conf = new SparkConf().setAppName("ApplicationUtilTest").setMaster("local[*]").set("spark.driver.host", "localhost");
	static SparkSession session = SparkSession.builder().config(conf).getOrCreate();
	
	@Test
	public void test_readData() {
		
		Dataset<Row> actualdataSet=ApplicationUtil.readData("src/test/resources/transaction_test.csv");
		actualdataSet.show();
	}
}
	
	