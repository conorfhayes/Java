//Written by Conor Hayes 10354355
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class WeatherStation implements Serializable
{
	double maxTemp;
	double t1, t2, r;
	ArrayList<Double> maxTemp_count = new ArrayList<Double>();
	String city;
	String ws_city;
	static ArrayList<WeatherStation> stations = new ArrayList<WeatherStation>();
	ArrayList<WeatherStation> ws_stations = new ArrayList<WeatherStation>();
	List<Measurement> measurementList  = new ArrayList<Measurement>();
	List<Measurement> ws_measurementList  = new ArrayList<Measurement>();
	
	

	
// Weather Station constructor that takes a string city, and a list of measurements	
public WeatherStation(String city, List<Measurement> measurementList) 
{
	this.city = city;
	this.measurementList = measurementList;
	setCity(city);
	setMeasurementList(measurementList);
			
}

	//setter for city
	public void setCity(String city)
	{
		ws_city = city;
	}
	
	//setter for measurementlist
	public void setMeasurementList(List<Measurement> measurementList)
	{
		ws_measurementList = measurementList;	
	}
	
	//getter for city
	public String getCity()
	{
		
	 return city;
	 
	}

	//getter for measurementlist 
	public List<Measurement> getMeasurementList()
	{
		
	 return measurementList;
	 
	}
	
	//Method to all the program to add another weather station
	public static void addStation(WeatherStation weatherstation)
	{
		stations.add(weatherstation);
		
	}

	
	public static int countTemperature(double t)
	
	{
		// create t_plus and t_min, intervals as specified by the assingment sheet
		double t_plus = t + 1;
		double t_minus = t - 1;
		
		// create the spark contenxts 
		SparkConf sparkConf = new SparkConf().setAppName("Temp Interval Count").setMaster("local[4]").set("spark.executor.memory","1g");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		// parallelize stations to work on it
		JavaRDD<WeatherStation> m_list = sc.parallelize(stations);
		// create a JavaRDD to hold the out
		// flat map m_list to get the measurements
		JavaRDD<Double> measure = m_list.flatMap(s -> s.getMeasurementList().iterator())
		// use map to access the getTemp() method
										.map(s -> s.getTemp())
		// filter the results so that the value is checked if it falls between the 
		// the two intervals
										.filter(m -> m >= t_minus && m <= t_plus);
		// count the amount of values that meet the criteria
		int output = (int)measure.count();
		
		// stop and close the spark context
		sc.stop();
		sc.close();
		
		// reuturn the count
		return output;

		
		
	}
		
		
}

	
	



	
