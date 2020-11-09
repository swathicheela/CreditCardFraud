package com.myhu.cheela.utils;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class ApplicationUtil {
	
	static SparkConf conf = new SparkConf().setAppName("ApplicationUtil").setMaster("local[*]").set("spark.driver.host", "localhost");
	static SparkSession session = SparkSession.builder().config(conf).getOrCreate();
	
	public static Dataset<Row> readData(String path) {
		
		Dataset<Row> dataSet=session.read().option("header", "true").csv(path); 
		return dataSet;
	}
	
	public static Dataset<Row> readDataFromMySQL(String path,String table) {
		
		Dataset<Row> dataSet= session.read().format("jdbc")
				  .option("url", path)
				  .option("dbtable", table)
				  .option("user", "root")
				  .option("password", "root")
				  .load();
		
		return dataSet;
	}
	
	public static List<PipelineStage> pipelinestages() {
		
		StringIndexer indexer1= new StringIndexer();StringIndexer indexer2= new StringIndexer();StringIndexer indexer3= new StringIndexer();
		StringIndexer indexer4= new StringIndexer();StringIndexer indexer5= new StringIndexer();StringIndexer indexer6= new StringIndexer();
		
		OneHotEncoder encoder1=new OneHotEncoder();OneHotEncoder encoder2=new OneHotEncoder();OneHotEncoder encoder3=new OneHotEncoder();
		OneHotEncoder encoder4=new OneHotEncoder();OneHotEncoder encoder5=new OneHotEncoder();OneHotEncoder encoder6=new OneHotEncoder();
		
		VectorAssembler assembler = new VectorAssembler();
		List<PipelineStage> pipelinestages= new ArrayList<PipelineStage>();
		
		indexer1.setInputCol("cc_num").setOutputCol("cc_num_indexed");indexer2.setInputCol("category").setOutputCol("category_indexed");
		indexer3.setInputCol("merchant").setOutputCol("merchant_indexed");indexer4.setInputCol("distance").setOutputCol("distance_indexed");
		indexer5.setInputCol("amt").setOutputCol("amt_indexed");indexer6.setInputCol("age").setOutputCol("age_indexed");
		
		encoder1.setInputCol("cc_num_indexed").setOutputCol("cc_num_indexed_encoded");encoder2.setInputCol("category_indexed").setOutputCol("category_indexed_encoded");	
		encoder3.setInputCol("merchant_indexed").setOutputCol("merchant_indexed_encoded");	encoder4.setInputCol("distance_indexed").setOutputCol("distance_indexed_encoded");	
		encoder5.setInputCol("amt_indexed").setOutputCol("amt_indexed_encoded");encoder6.setInputCol("age_indexed").setOutputCol("age_indexed_encoded");	
		
		
		assembler.setInputCols(new String[] {encoder1.getOutputCol(),encoder2.getOutputCol(),encoder3.getOutputCol(),encoder4.getOutputCol(),encoder5.getOutputCol(),encoder6.getOutputCol()})
				.setOutputCol("features");
		
		pipelinestages.add(indexer1);pipelinestages.add(indexer2);pipelinestages.add(indexer3);
		pipelinestages.add(indexer4);pipelinestages.add(indexer5);pipelinestages.add(indexer6);
		
		pipelinestages.add(encoder1);pipelinestages.add(encoder2);pipelinestages.add(encoder3);
		pipelinestages.add(encoder4);pipelinestages.add(encoder5);pipelinestages.add(encoder6);
		
		pipelinestages.add(assembler);
		return pipelinestages;
	}
	
	public static RandomForestClassificationModel randomForestAlgorithm(Dataset<Row> balancedDataSet) {
		Dataset<Row> CastedDS = balancedDataSet.withColumn("label", col("label").cast(DataTypes.IntegerType));

		Dataset<Row> trainedDS= CastedDS.randomSplit(new double[]{0.7, 0.3})[0];
		Dataset<Row> testDS= CastedDS.randomSplit(new double[]{0.7, 0.3})[1];
		RandomForestClassifier randomForestEstimator = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setMaxBins(700).setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043);
		RandomForestClassificationModel finalModel= randomForestEstimator.fit(trainedDS);
		Dataset<Row>	 transactionwithPrediction = finalModel.transform(testDS);
		
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
			      .setLabelCol("label")
			      .setPredictionCol("prediction")
			      .setMetricName("accuracy");
		double accuracy = evaluator.evaluate(transactionwithPrediction);
			    System.out.println("Test Error = " + (1.0 - accuracy));
		
		return finalModel;
	}
	
	public static Dataset<Row> createDataSet(JavaRDD<Row> data,StructType schema) {
		
		Dataset<Row> dataSet=session.createDataFrame(data, schema); 
		
		return dataSet;
	}
	
	public static SparkSession getSession() {
		return session;
	}

}
