package fi.csc.spark.recommender.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.spark.MongoSpark;

import fi.csc.spark.recommender.models.MovieRating;
import fi.csc.spark.recommender.models.MovieRecommendations;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

public final class SparkMovieUtils {

	
	private static ALS als;


	public static ALS getALS() {
		return als;
	}

	public static void setALS(ALS als) {
		SparkMovieUtils.als = als;

	}

	
	/*
	 * {'userId': 700, 'movieId': 1, 'rating': 5.0 }
	 */
	public static MovieRating getMovieRatingRecord(String json_message) {

		JsonParser parser = new JsonParser();
		JsonObject obj = (JsonObject) parser.parse(json_message);
		Integer userId = obj.get("userId").getAsInt();
		Integer movieId = obj.get("movieId").getAsInt();
		Double rating = obj.get("rating").getAsDouble();

		MovieRating mrating = new MovieRating();
		mrating.setUserId(userId);
		mrating.setMovieId(movieId);
		mrating.setRating(rating);

		return mrating;

	}

	public static void createModel() {

		ALS als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId")
				.setRatingCol("rating");

		SparkMovieUtils.setALS(als);

	}

	
	public static MovieRecommendations transformMovieIdsToNames(Row row, Broadcast<Map<Integer,String>> movieBroadcast) {
		
		List<String> recommendationNames = new ArrayList<String>();
		//Map<Integer,String> movieNames = SparkMovieUtils.getMovieBroadcast().getValue();
		Map<Integer,String> movieNames = movieBroadcast.getValue();
		
		Integer userId = (Integer)row.getAs("userId");
		@SuppressWarnings("unchecked")
		
		WrappedArray<GenericRowWithSchema> recommendationIDs = (WrappedArray<GenericRowWithSchema>) row.getAs("recommendations");
		
		
		Iterator<GenericRowWithSchema> it = recommendationIDs.iterator();
		while(it.hasNext()) {
			GenericRowWithSchema id_score  = it.next();
			recommendationNames.add(movieNames.get(id_score.get(0)));
		}
		return new MovieRecommendations(userId, recommendationNames);
		
	}
	
	public static void mainFun(JavaRDD<MovieRating> rdd, Broadcast<Map<Integer,String>> movieBroadcast) {

		Dataset<Row> newRatingsDFRow = SparkUtils.getSparkSession().createDataFrame(rdd, MovieRating.class);
		Dataset<MovieRating> newRatingsDF = newRatingsDFRow.as(Encoders.bean(MovieRating.class));
		
		newRatingsDF.show();
		
		//if(newRatingsDF.count() >0) {
				
			Dataset<MovieRating> originalRatingsDF = MongoSpark.load(SparkUtils.getSparkSession(), SparkMongoUtils.getRatingsReadConfig(), MovieRating.class);

			originalRatingsDF.show(10);
		
			Dataset<MovieRating> finalRatingsDF = newRatingsDF.union(originalRatingsDF);

			finalRatingsDF.show(10);
		
			finalRatingsDF.cache();
			ALSModel model = SparkMovieUtils.als.fit(finalRatingsDF);
			model.setColdStartStrategy("drop");


			Dataset<Row> usersDF = newRatingsDF.select("userId").distinct();
			Dataset<Row> usersRawRecommendations = model.recommendForUserSubset(usersDF, 50);
		
			//usersRawRecommendations.show();
		
			Dataset<MovieRecommendations> usersRecommendations = usersRawRecommendations.map(
				(MapFunction<Row, MovieRecommendations>) row -> transformMovieIdsToNames(row, movieBroadcast), 
				Encoders.bean(MovieRecommendations.class));
		
		
			//usersRecommendations.show();
			finalRatingsDF.unpersist();
			System.out.println("writing in mongo");
			//MongoSpark.save(usersRecommendations, SparkMongoUtils.getTestWriteConfig());
			//MongoSpark.save(usersRecommendations.write().option("collection", "test").mode("overwrite");
			MongoSpark.save(usersRecommendations.write().mode("overwrite"), SparkMongoUtils.getTestWriteConfig());
			System.out.println("Wrote the recommendations in MongoDB, now running the next round of spark streaming");
		//}
		//else {
		//	System.out.println("Empty run");
		//}
		 
	}

}
