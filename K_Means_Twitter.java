import java.io.Serializable;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

public class K_Means_Twitter implements Serializable {	
	

	public static void main(String[] args) {
		
		//Initialize the spark contexts 
		SparkConf conf = new SparkConf().setAppName("K-Means Implementation").setMaster("local[4]").set("spark.executor.memory","1g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Path to twitter data text file, this is the location on my computer
		String path = "twitter2D.txt";
		
		// JavaRDD of type string to hold the contents of the text file of twitter data
		JavaRDD<String> data = sc.textFile(path);	
		
		// JavaRDD Vector created to operate on the textfile (data)
		// .map operation used to work on the contents of the file
		JavaRDD<Vector> parsedData = data.map( 
						// In this occurance it made sense to operate on on the file using a code block instead of numerous
						// other functional statements
						a -> {
								// String array used to hold the output of the text file when split on a comma ,
								// file is comma delimited hence why ',' is used
								String[] sarray = a.split(",");
								// double assigned to hold the latitude value which is at sarray[0]
								// Also using parseDouble wrapper to convert from string to a double
								Double co_ordinate_1 = Double.parseDouble(sarray[0]);
								// double assigned to hold the longitude value which is at sarray[1]
								// Also using parseDouble wrapper to convert from string to a double
								Double co_ordinate_2 = Double.parseDouble(sarray[1]);
								// Double array used to store both lat_ and long_ values
								double [] values = {co_ordinate_1, co_ordinate_2};
								// in order to be passed to the kmeans algorithm the values must be vectorised
								// returning here a dense vector of the values array
								return Vectors.dense(values);								
							 });
		
		// cache the data from above
		parsedData.cache();
		// number of clustors is defined in the assignment, needed 4 clustors
		int numClusters = 4;
		// I am setting the number of iterations at 40
		int numIterations = 40;
		
		// Initialize the Kmeans algorithm and train it on the parsedData (formatted as an rdd, hence .rdd())
		// and also pass, the number of clustors and the number of iterations
		KMeansModel twitter_clustors = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		
		// In order to get the desired output that was requested where we have the tweet and its location in the clustor
		// we do the following
		// we must apply an operation on each value of the text file
		JavaPairRDD<Integer, String> tweets = data.mapToPair(
				
				b -> {
						// Same operation as above only change is on the following line and below:: String tweet = sarray[sarray.length -1];
						String[] sarray = b.split(",");
						Double co_ordinate_1 = Double.parseDouble(sarray[0]);
						Double co_ordinate_2 = Double.parseDouble(sarray[1]);
						double [] values = {co_ordinate_1, co_ordinate_2};						
						// to get the tweet data we simply get the value at the last location on the sarray array
						// sarray.length - 1 returns the tweet
						String tweet = sarray[sarray.length -1];
						
						// this time we must create a stand alone Vector of the values array
						// this is done so we can pass this to a method of the Kmeans algorithm later
						Vector values_vec = Vectors.dense(values);						
						// .predict does the following:: returns the cluster label, which is the index of nearest centroid.
						// we assign the clustor and the tweet to a tuple that is returned to be used later
						return new Tuple2<>(twitter_clustors.predict(values_vec), tweet);
						
					 });
		
		
		// Sort the JavaPairRDD by key and print the required result
		// Here we print the desired outcome, we print the tweet and using twitter_clustors.predict we get the
		// location of each tweet in the clustor
		tweets.sortByKey().collect().stream()
							.forEach(c -> 
							System.out.println("Tweet " + "\"" + c._2() + "\" is in cluster " + c._1()));
		// Stop and Close the spark context
		sc.stop();
		sc.close();

	}

}
