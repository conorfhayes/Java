import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import scala.Tuple2;
import java.util.*;

public class Spark_SVM implements Serializable 
{
	// NB, method is called from testing class Assignment_3_Test, by running this class both
	// Part 1 and 2 of this assignment are called
	
	public static void machinelearning_IMDB() 
	
	{
	// Create New HashingTF to be used later on to convert strings to vectors
	final HashingTF tf = new HashingTF(100);
	
	// Initialize and create SparkConf and JavaSparkContext
	SparkConf sparkConf = new SparkConf().setAppName("SVM Implementation").setMaster("local[4]").set("spark.executor.memory","1g");
	JavaSparkContext sc = new JavaSparkContext(sparkConf);					
	
	//Path to where my imdb_labelled.txt file
	String path = "./imdb_labelled.txt";
	// Create a JavaRDD<LabelledPoint> that reads in the txt file and prepares it for input to SVM algorithm
	JavaRDD<LabeledPoint> data  = 
	// read in text file
	sc.textFile(path)		
	// map function to split the text file when tab occurs
	.map(a -> a.split("\\t"))
	// map functions to create a new labelled point containing, the string after the split b[1] as the label
	// and the vector transform on b[0] the sentiment that is also split on whitespace between the words
	// this creates a label and a vector that can be passed to the SVM algorithm
	.map(b -> new LabeledPoint(Double.parseDouble(b[1]),(tf.transform(Arrays.asList(b[0].split(" "))))));

	// training data is a sample of 60% of the whole data
	JavaRDD<LabeledPoint> trainingData = data.sample(false, 0.6);
	// cache the training data
    trainingData.cache();
    // testing data is the remaining 40% of the whole data
	JavaRDD<LabeledPoint> testingData =  data.subtract(trainingData);	
	
	// set numItertations to be used to tell the algorithm how many times to run the training data
	int numIterations = 100000;
	// create the SVM algorithm instance and pass the trainingdata as an rdd and the number of iterations
	SVMModel model = SVMWithSGD.train(trainingData.rdd(), numIterations);
	// clear the threshold for prediciotns
	model.clearThreshold();	
	
	// for Part 2 we must first test the algorithm on some instances of the test data
	// testing_1 gets this data from 5% of the testing data 
	JavaRDD<LabeledPoint> testing_1 =  testingData.sample(false, 0.05);
	
	// create a new RDD that calls on the model to test on the testing_1 data and predict the labels
	JavaRDD<Tuple2<Object, Object>> FeaturesAndLabels_Test = 
	testing_1.map(p -> new Tuple2<>(model.predict(p.features()), p.label()));
	
	// Print to let the user know when section is about to be outputted to the console
	System.out.println("Assignment 3: Part 2 (a)::  ");
	
	FeaturesAndLabels_Test.collect()
	// Line to print each label and its vecotrised feature that was predicted
	.forEach(p -> System.out.println("Feature " + p._1 + " " + "Label " + p._2));
		
    // another RDD created to hold the preditions on the testingData
	JavaRDD<Tuple2<Object, Object>> FeaturesAndLabels = 
	testingData.map(p -> new Tuple2<>(model.predict(p.features()), p.label()));
    
    BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(FeaturesAndLabels));
    // collect the auROC values and assign them to a variable 
    double auROC = metrics.areaUnderROC();
    
    // Print to let the user know when section is about to be outputted to the console
 	System.out.println("Assignment 3: Part 2 (b)::  ");
 	
    // Print the auROC, I have ran the algorithm many times and I have seen the results come back between 60% and 75%
    System.out.println("Area under ROC = " + auROC);	
	
    // stop and close the java spark context
	sc.stop();
	sc.close();	
	return;
	
	}
}
