package fi.csc.spark.recommender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import fi.csc.spark.recommender.kafka.KafkaMovieConsumer;
import fi.csc.spark.recommender.models.Movie;
import fi.csc.spark.recommender.utils.SparkMongoUtils;
import fi.csc.spark.recommender.utils.SparkMovieUtils;
import fi.csc.spark.recommender.utils.SparkUtils;
import scala.Tuple2;


public class SparkApp 
{
    public static void main( String[] args ) throws InterruptedException
    {

    	SparkConf sparkConf = new SparkConf().setAppName("Recommender System");

//    	//Dev
//    	
//    	sparkConf.setMaster("local[*]");
//    	sparkConf.set("spark.driver.host", "localhost");
//    	
    	
    	sparkConf.set("spark.mongodb.input.uri",AppConstants.mongoURI);
        sparkConf.set("spark.mongodb.output.uri", AppConstants.mongoURI);
        
    	
    	SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
    	spark.sparkContext().setLogLevel("ERROR");
    	SparkUtils.setSparkSession(spark);

    	SparkMovieUtils.createModel();
    	
    	Dataset<Movie> moviesDF = MongoSpark.load(spark, SparkMongoUtils.getMoviesReadConfig(), Movie.class);
    	JavaPairRDD<Integer, String> moviesRDD = moviesDF.javaRDD().mapToPair
    			(movie -> new Tuple2<Integer, String>(movie.getMovieId(), movie.getTitle()));
    	
    	Map<Integer, String> movies = moviesRDD.collectAsMap();
    	
    	System.out.println(movies.size());
    	
    	Broadcast<Map<Integer,String>> movieBroadcast = SparkUtils.getJsc().broadcast(movies);
    	//SparkMovieUtils.setMovieBroadcast(movieBroadcast); 
    	
    	int mins = 1;
    	JavaStreamingContext ssc = new JavaStreamingContext(SparkUtils.getJsc(), new Duration(mins * 60 * 1000));
    	
    	KafkaMovieConsumer kfc = new KafkaMovieConsumer();
    	
    	kfc.consume(ssc, movieBroadcast);
    	
    	
    	
    	spark.close();

    	
    }
}
