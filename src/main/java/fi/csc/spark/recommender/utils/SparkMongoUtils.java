package fi.csc.spark.recommender.utils;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;

public final class SparkMongoUtils {

	public static ReadConfig getRatingsReadConfig() {
		Map<String, String> readOverrides = new HashMap<String, String>();
		//readOverrides.put("uri", "mongodb://localhost/sampledb");
		readOverrides.put("database", "sampledb");
		readOverrides.put("collection", "ratings");
		// readConfig.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(SparkUtils.getJsc()).withOptions(readOverrides);
		
		return readConfig;
	}
	
	public static ReadConfig getMoviesReadConfig() {
		Map<String, String> readOverrides = new HashMap<String, String>();
		//readOverrides.put("uri", "mongodb://localhost/sampledb");
		readOverrides.put("database", "sampledb");
		readOverrides.put("collection", "movies");
		// readConfig.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(SparkUtils.getJsc()).withOptions(readOverrides);
		
		return readConfig;
	}
	
	public static WriteConfig getTestWriteConfig() {
		Map<String, String> writeOverrides = new HashMap<String, String>();
		writeOverrides.put("database", "sampledb");
		writeOverrides.put("collection", "test");
		WriteConfig writeConfig = WriteConfig.create(SparkUtils.getJsc()).withOptions(writeOverrides);
		
		return writeConfig;
	}
}
