package com.myhu.cheela.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

import com.myhu.cheela.utils.ApplicationUtil;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ImportBasicDataToMysql {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		UDF4<Double,Double,Double,Double,Double> distance = new UDF4<Double,Double,Double,Double,Double>(){
			//@Override
			public Double call(Double lat, Double longi, Double merch_lat, Double merch_long) throws Exception {
				int r= 6371;//radius of earth
				double latDist= Math.toRadians(merch_lat - lat);
				double lonDist= Math.toRadians(merch_long - longi);
				double a= Math.sin(latDist / 2) * Math.sin(latDist / 2) + Math.cos(Math.toRadians(lat)) * Math.cos(Math.toRadians(merch_lat)) * Math.sin(lonDist / 2) * Math.sin(lonDist / 2);
				double c= 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
				double distance=  r * c;
				return distance;	
			}
		};
		
	List<String> newlist= new ArrayList<String>();
	newlist.add("cc_num");

    SparkSession session = SparkSession.builder().appName("ImportBasicDataToMysql").master("local[1]").getOrCreate();
    Dataset<Row> customerDataSet=ApplicationUtil.readData("src/main/resources/customer.csv"); 
    JavaRDD<Row> customerData = customerDataSet.javaRDD();
    
    Dataset<Row> transDataSet=ApplicationUtil.readData("src/main/resources/transactions.csv"); 
    JavaRDD<Row> transData = transDataSet.javaRDD();
    
    Dataset<Row> customerDF=ApplicationUtil.createDataSet(customerData, Schemas.custSchema())
    		.withColumn("lat", col("lat").cast(DataTypes.DoubleType))
    		.withColumn("long", col("long").cast(DataTypes.DoubleType))
    		.withColumn("dob", date_format(col("dob"),"YYYY-MM-dd HH:mm:ss"));
    		
    Dataset<Row> customerDFAge= customerDF
    		.withColumn("age", (datediff(current_date(),to_date(col("dob"))).divide(365).cast(DataTypes.IntegerType)));
    
    Dataset<Row> transDF=ApplicationUtil.createDataSet(transData, Schemas.transSchema())
    		.withColumn("trans_date", split(col("trans_date"), "T").getItem(0))
    	    .withColumn("trans_time", concat_ws(" ",col("trans_date"),col("trans_time")))
    	    .withColumn("trans_time", date_format(col("trans_time"), "YYYY-MM-dd HH:mm:ss"));
  
    session.udf().register("getDistance", distance,DataTypes.DoubleType);
    Seq<String> seqcol=   JavaConverters.asScalaIteratorConverter(newlist.iterator()).asScala().toSeq();
    
    Dataset<Row> processedDF = transDF
		 .join(broadcast(customerDFAge), seqcol)
    	     .withColumn("distance", callUDF("getDistance",customerDFAge.col("lat").cast("double"), customerDFAge.col("long").cast("double"), col("merch_lat").cast("double"),col("merch_long").cast("double")))
    	     .select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud");
    
    processedDF.cache();
    Dataset<Row> fraudDF = processedDF.filter(col("is_fraud").equalTo("1"));
    	Dataset<Row> nonFraudDF = processedDF.filter(col("is_fraud").equalTo("0"));
    Properties properties= new Properties();
        properties.put("user", "root");
        properties.put("password", "root");
        properties.put("driver", "com.mysql.cj.jdbc.Driver");
        customerDF.write().mode("overwrite").jdbc("jdbc:mysql://localhost:3306/transactions_db?useSSL=false", "customer", properties); 
        fraudDF.write().mode("overwrite").jdbc("jdbc:mysql://localhost:3306/transactions_db?useSSL=false", "fraud", properties); 
        nonFraudDF.write().mode("overwrite").jdbc("jdbc:mysql://localhost:3306/transactions_db?useSSL=false", "nonfraud", properties);
	}
	
}