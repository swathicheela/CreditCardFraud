package com.myhu.cheela.model;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.myhu.cheela.spark.Schemas;
import com.myhu.cheela.utils.ApplicationUtil;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class CreditCardTransactionFraudDetection {

	static SparkConf conf = new SparkConf().setAppName("ApplicationUtil").setMaster("local[*]").set("spark.driver.host", "localhost");
	static SparkSession session = SparkSession.builder().config(conf).getOrCreate();
	
	public static void main(String[] args) {
		
		@SuppressWarnings("serial")
		UDF4<Double,Double,Double,Double,Double> distance = new UDF4<Double,Double,Double,Double,Double>(){
			@Override
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
		
		PipelineModel preprocessingModel = PipelineModel.load("src/main/resources/models/preprocessingModel");
		RandomForestClassificationModel randomForestModel = RandomForestClassificationModel.load("src/main/resources/models/RandomForestModel");

		
		Dataset<Row> transactions= ApplicationUtil.readDataFromMySQL("jdbc:mysql://localhost:3306/transactions_db?useSSL=false","transaction_record");
		transactions.cache();
		JavaRDD<Row> transactionData = transactions.javaRDD();
		Dataset<Row> customerDataSet = ApplicationUtil.readData("src/main/resources/customer.csv");
		customerDataSet.cache();
		JavaRDD<Row> customerData = customerDataSet.javaRDD();

		Dataset<Row> customerDF = ApplicationUtil.createDataSet(customerData, Schemas.custSchema())
				.withColumn("lat", col("lat").cast(DataTypes.DoubleType))
				.withColumn("long", col("long").cast(DataTypes.DoubleType))
				.withColumn("dob", date_format(col("dob"), "YYYY-MM-dd HH:mm:ss"));
		
		Dataset<Row> customerDFAge = customerDF.withColumn("age",
				(datediff(current_date(), to_date(col("dob"))).divide(365).cast(DataTypes.IntegerType)));
		customerDFAge.cache();
		
		
		Dataset<Row> transactionDS = ApplicationUtil.createDataSet(transactionData, Schemas.realtimeTransSchema())
				.withColumn("merch_lat", lit(col("merch_lat")).cast(DataTypes.DoubleType))
				.withColumn("merch_long", lit(col("merch_long")).cast(DataTypes.DoubleType))
				.withColumn("trans_time", lit(col("trans_time")).cast(DataTypes.TimestampType))
				.withColumnRenamed("first","firstname")
				.withColumnRenamed("last","lastname");
		
		List<String> newlist= new ArrayList<String>();
		newlist.add("cc_num");
		
		session.udf().register("getDistance", distance,DataTypes.DoubleType);
	    Seq<String> seqcol=   JavaConverters.asScalaIteratorConverter(newlist.iterator()).asScala().toSeq();
	    
	    Dataset<Row> processedDF = transactionDS
	   		 .join(broadcast(customerDFAge), seqcol)
	   		.withColumn("distance", callUDF("getDistance",col("lat").cast("double"), col("long").cast("double"), col("merch_lat").cast("double"),col("merch_long").cast("double")));
	       
	   processedDF.cache();
	   Dataset<Row> featureTransactionDS = preprocessingModel.transform(processedDF);
	   Dataset<Row> predictionDS = randomForestModel.transform(featureTransactionDS)
	    	.withColumnRenamed("prediction", "is_fraud");
	   
	   Dataset<Row> fraudtrans=	predictionDS
	    .select("cc_num","trans_num","trans_time","category","merchant", "merch_lat","merch_long","amt","age","is_fraud");
	   
	   Properties properties= new Properties();
       properties.put("user", "root");
       properties.put("password", "root");
       properties.put("driver", "com.mysql.cj.jdbc.Driver");
       fraudtrans.write().mode("overwrite").jdbc("jdbc:mysql://localhost:3306/transactions_db?useSSL=false", "predictions", properties);
  
	}

}
