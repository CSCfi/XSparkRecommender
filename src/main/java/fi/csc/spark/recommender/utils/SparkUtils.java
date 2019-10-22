package fi.csc.spark.recommender.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public final class SparkUtils {

	private static SparkSession spark;

	private static JavaSparkContext jsc;
	
	public static SparkSession getSparkSession() {
		return spark;
	}

	public static void setSparkSession(SparkSession spark) {
		SparkUtils.spark = spark;
		SparkUtils.setJsc(spark);
	}

	public static JavaSparkContext getJsc() {
		
		return jsc;
	}

	public static void setJsc(SparkSession spark) {
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		
		SparkUtils.jsc = jsc;
	}
	
	
}
