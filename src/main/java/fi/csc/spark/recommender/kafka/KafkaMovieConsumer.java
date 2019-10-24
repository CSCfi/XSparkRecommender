package fi.csc.spark.recommender.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import fi.csc.spark.recommender.AppConstants;
import fi.csc.spark.recommender.models.MovieRating;
import fi.csc.spark.recommender.utils.SparkMovieUtils;
import scala.Tuple13;
import scala.Tuple2;
import scala.Tuple3;

public class KafkaMovieConsumer {

	
	
	public void consume(JavaStreamingContext streamingContext) throws InterruptedException {
	
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", AppConstants.kafkabroker);
		kafkaParams.put("key.deserializer", LongDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "movielens");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("ratings");

		JavaInputDStream<ConsumerRecord<Long, String>> kafkaStream =
				KafkaUtils.createDirectStream(
						streamingContext,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<Long, String>Subscribe(topics, kafkaParams)
						);
		
		
		
		
		JavaDStream<String> rawStream = kafkaStream.map(record -> record.value());
		
		rawStream.print();
		
		JavaDStream<MovieRating> rowStream = rawStream.map(jsonString -> SparkMovieUtils.getMovieRatingRecord(jsonString));
		
		//rowStream.print();
		
		rowStream.foreachRDD(rdd-> SparkMovieUtils.mainFun(rdd));
		
		streamingContext.start();
		System.out.println("Started Spark Streaming");
		
		streamingContext.awaitTermination();
		
	}
	
}
