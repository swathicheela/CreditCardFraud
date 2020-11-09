package com.myhu.cheela.model;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.List;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.myhu.cheela.utils.ApplicationUtil;

public class CreditCardMachineLearningModel {

	public static void main(String[] args) throws IOException {
		
		Dataset<Row> fraud= ApplicationUtil.readDataFromMySQL
		("jdbc:mysql://localhost:3306/transactions_db?useSSL=false","fraud")
		.select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud");
		
		Dataset<Row> nonfraud= ApplicationUtil.readDataFromMySQL
		("jdbc:mysql://localhost:3306/transactions_db?useSSL=false","nonfraud")
		.select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud");
			
		Dataset<Row> transDataSet=nonfraud.union(fraud);
		transDataSet.cache();
		
		List<PipelineStage> pipelinestages = ApplicationUtil.pipelinestages();
		PipelineStage[] pipelinestage = pipelinestages.stream().toArray(PipelineStage[]::new);
		Pipeline pipeline = new Pipeline().setStages(pipelinestage);
		PipelineModel model= pipeline.fit(transDataSet);
		model.write().overwrite().save("src/main/resources/models/preprocessingModel");
		Dataset<Row> featureDS = model.transform(transDataSet);
		
		Dataset<Row> fraudDS = featureDS
				.filter(col("is_fraud").equalTo("1"))
				.withColumnRenamed("is_fraud", "label")
				.select("features","label");
		
		Dataset<Row> nonFraudDS = featureDS
				.filter(col("is_fraud").equalTo("0"))
				.withColumnRenamed("is_fraud", "label")
				.select("features","label");
		
		
		Dataset<Row> nonFraudDS_sample=nonFraudDS.sample(false, 0.0395);
		Dataset<Row> balancedDataSet=fraudDS.union(nonFraudDS_sample);
		ApplicationUtil.randomForestAlgorithm(balancedDataSet).write().overwrite()
		.save("src/main/resources/models/RandomForestModel");
	}

}
