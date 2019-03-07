import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter_Stream implements Serializable
{	
	
 public static void main(String[] args) throws TwitterException, InterruptedException {	
	 //Turn off output from spark as it was hard to see program output in console
	 Logger.getLogger("org").setLevel(Level.OFF);
	 Logger.getLogger("akka").setLevel(Level.OFF);	
	 
	 //Set up spark and streaming contexts	
	 SparkConf conf = new SparkConf().setAppName("K-Means Implementation").setMaster("local[*]");
	 JavaSparkContext sc = new JavaSparkContext(conf);
	 JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(100));
	 
	// These variables store the running content size values.
	 final AtomicLong runningCount_WordCount = new AtomicLong(0);
	 final AtomicLong runningSum_WordCount = new AtomicLong(0);
	 
	 final AtomicLong runningCount_CharacterCount = new AtomicLong(0);
	 final AtomicLong runningSum_CharacterCount = new AtomicLong(0);
	 
	 // Configuration builder used to hold the twitter api keys
	 Configuration cb = new ConfigurationBuilder()
	   .setOAuthConsumerKey("ZStuvVxLujg2xphLESZ7MzCNm")
	   .setOAuthConsumerSecret("akZuT68RM7eAwMWLkVbdbLqt1dBxp97WkeyeKhQqAqajj3UMwx")
	   .setOAuthAccessToken("907064885191647233-spDX3rrIcuV5SVEYup0fAOjB6TPmfcP")
	   .setOAuthAccessTokenSecret("65EsOWa5bYXUhWxaF2CnIUIRMyY60R89Xl810BPtrenFN").build();
	 
	 //Setting up twitter objects to create twitter stream
	 TwitterFactory tf = new TwitterFactory(cb);
	 Twitter twitter = tf.getInstance();
	 
	// Create the twitter stream, passed the twitter object to get authorisation from twitter api to stream
    JavaDStream<String> tweets = TwitterUtils.createStream(jssc,twitter.getAuthorization())
    										//.map get the tweets and gets the text for them
    										 .map(tw -> tw.getText());   
    //Print the tweets
    tweets.print();
    
    // create a pair rdd, that holders the tweet and the length of the tweet(characters) 
    JavaPairDStream<String, Integer> TweetCharacterCount = tweets.mapToPair
			(a -> new Tuple2<>(a, a.length()));	
    // prints the following (tweets, number of characters of tweet)
	TweetCharacterCount.print();
    
	// create a rdd pair to hold the tweet and the word count.
	// split on the whitespace and count the words then in the array created, 
	//can use length of the array to give us the amount of words
	JavaPairDStream<String, Integer> TweetWordCount = tweets.mapToPair
			(a -> new Tuple2<>(a, a.split("\\s").length));	
	
	// prints the following (tweets, number of words of tweet)
	TweetWordCount.print();	
	
	// get the hastags by splitting on whitspace and keeping only words
	// that start with #
	JavaDStream<String> TweetHashTags = tweets.flatMap(s -> Arrays.asList(s.split("\\s")).iterator())
											  .filter(c -> c.startsWith("#"));	
	//print the hastags
	TweetHashTags.print();
	
	// isolate the number of the characters in each tweet to be used to get average length of tweets
	JavaDStream<Integer> TweetCharacterLength = tweets.map(a -> a.length());	
	// use a for each rdd and using running some reduce it and store the value
	// devide this by the amount of rdd's to get the average
	TweetCharacterLength.foreachRDD(x -> {
		  if (x.count() > 0) {
		    runningSum_CharacterCount.getAndAdd(x.reduce((a, b) -> a+b));
		    runningCount_CharacterCount.getAndAdd(x.count());
		    System.out.println("Average Characters per Tweet: " +  runningSum_CharacterCount.get() / runningCount_CharacterCount.get());		    
		  }
		});
	
	// isolate the number of words in each tweet to be used to get average length of tweets
	JavaDStream<Integer> TweetWordLength = tweets.map(a -> a.split("\\s").length);
	// use a for each rdd and using running some reduce it and store the value
	// divide this by the amount of rdd's to get the average
	TweetWordLength.foreachRDD(y -> {
		  if (y.count() > 0) {
		    runningSum_WordCount.getAndAdd(y.reduce((a, b) -> a + b));
		    runningCount_WordCount.getAndAdd(y.count());
		    System.out.println("Average Words per Tweet: " +  runningSum_WordCount.get() / runningCount_WordCount.get());	
		    
		  }
		});
	
	// count the hastags, get the hastags and add them to a new tuple with value
	// hastag, 1
	JavaPairDStream<String, Integer> TweetHashTagsCount = TweetHashTags.mapToPair((a -> new Tuple2<>(a, 1)));
	// reduce the hastags to get the count, and do this by a window, that is recalcuated every Duration specified
	JavaPairDStream<String, Integer> HashTagCounts = 
			TweetHashTagsCount.reduceByKeyAndWindow((i, j)-> i+j, new Duration(60 * 5 * 1000), new Duration(1*1000));
	// Print the hastag counts
	HashTagCounts.print();
	// Swap the Pair RDD from string integer to interger string
	JavaPairDStream<Integer, String> swappedCounts = HashTagCounts.mapToPair(s -> s.swap());
	JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(s -> s.sortByKey(false));

	// get the top 10 hastags over the duraiton specified
	// and plrint them to the console
    sortedCounts.foreachRDD(s -> {
         String out = "\nTop 10 hashtags:\n";
         for (Tuple2<Integer, String> t: s.take(10)) {
           out = out + t.toString() + "\n";
         }
         System.out.println(out);
       	 });		
	
    // Start the computation
    jssc.start();
    jssc.awaitTermination();
	        
	    }
	}