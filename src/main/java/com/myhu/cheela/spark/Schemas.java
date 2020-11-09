package com.myhu.cheela.spark;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Schemas {
	
	public static StructType custSchema() {
	
		StructType cust_Schema = new StructType().add("cc_num", DataTypes.StringType, true)
	    		.add("first", DataTypes.StringType, true)
	    		.add("last", DataTypes.StringType, true)
	    		.add("gender", DataTypes.StringType, true)
	    		.add("street", DataTypes.StringType, true)
	    		.add("city", DataTypes.StringType, true)
	    		.add("state", DataTypes.StringType, true)
	    		.add("zip", DataTypes.StringType, true)
	    		.add("lat", DataTypes.StringType, true)
	    		.add("long", DataTypes.StringType, true)
	    		.add("job", DataTypes.StringType, true)
	    		.add("dob", DataTypes.StringType, true);
		return cust_Schema;
	}
	
	public static StructType transSchema() {
		StructType trans_Schema = new StructType().add("cc_num", DataTypes.StringType, true)
	    		.add("first", DataTypes.StringType, true)
	    		.add("last", DataTypes.StringType, true)
	    		.add("trans_num", DataTypes.StringType, true)
	    		.add("trans_date", DataTypes.StringType, true)
	    		.add("trans_time", DataTypes.StringType, true)
	    		.add("unix_time", DataTypes.StringType, true)
	    		.add("category", DataTypes.StringType, true)
	    		.add("merchant", DataTypes.StringType, true)
	    		.add("amt", DataTypes.StringType, true)
	    		.add("merch_lat", DataTypes.StringType, true)
	    		.add("merch_long", DataTypes.StringType, true)
	    		.add("is_fraud", DataTypes.StringType, true);
		return trans_Schema;
	}
	public static StructType realtimeTransSchema() {
		StructType trans_Schema = new StructType()
			.add("trans_num", DataTypes.StringType, true)
			.add("category", DataTypes.StringType, true)
			.add("cc_num", DataTypes.StringType, true)
	    		.add("first", DataTypes.StringType, true)
	    		.add("last", DataTypes.StringType, true)
	    		.add("merch_lat", DataTypes.StringType, true)
	    		.add("merch_long", DataTypes.StringType, true)
	    		.add("merchant", DataTypes.StringType, true)
	    		.add("amt", DataTypes.StringType, true) 		
	    		.add("trans_time", DataTypes.StringType, true)
	    		.add("unix_time", DataTypes.StringType, true);  		
		return trans_Schema;
	}
}
