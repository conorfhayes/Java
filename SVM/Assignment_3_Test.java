import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

//Test Class
//Written by Conor Hayes 10354355

public class Assignment_3_Test implements Serializable {
	static double maxTemp;
	static ArrayList<Double> maxTemp_count = new ArrayList<Double>();
	static String city;
	static List<Measurement> sf_measurementList  = new ArrayList<Measurement>();
	static List<Measurement> berk_measurementList  = new ArrayList<Measurement>();
	static List<Measurement> pa_measurementList  = new ArrayList<Measurement>();

	public static void main(String[] args) 
	{
		// create measurements for weather station San Fran
		Measurement sf_m1 = new Measurement(1,20.0); Measurement sf_m2 = new Measurement(2,11.7); 
		Measurement sf_m3 = new Measurement(3,-5.4);
		Measurement sf_m4 = new Measurement(4,20.0); Measurement sf_m5 = new Measurement(5,18.7);
		
		//add each measurement from above to a measurement list
		sf_measurementList.add(sf_m1); sf_measurementList.add(sf_m2);
		sf_measurementList.add(sf_m3);sf_measurementList.add(sf_m4);
		sf_measurementList.add(sf_m5);
		
		// create measurements for weather station Berkeley
		Measurement berk_m1 = new Measurement(1,8.4); 
		Measurement berk_m2 = new Measurement(2,19.2); 
		Measurement berk_m3 = new Measurement(3,7.2);
		
		//add each measurement from above to a measurement list
		berk_measurementList.add(berk_m1); berk_measurementList.add(berk_m2);
		berk_measurementList.add(berk_m3);
		
		// create measurements for weather station Palo Alto
		Measurement pa_m1 = new Measurement(1,9.65); 
		Measurement pa_m2 = new Measurement(2,10.67); 
		Measurement pa_m3 = new Measurement(3,10.66);
		Measurement pa_m4 = new Measurement(4,9); 
		Measurement pa_m5 = new Measurement(5,10.6); 
		
		//add each measurement from above to a measurement list
		pa_measurementList.add(pa_m1); 
		pa_measurementList.add(pa_m2);
		pa_measurementList.add(pa_m3); 
		pa_measurementList.add(pa_m4);
		pa_measurementList.add(pa_m5);
		
		// Create the weather stations with their corresponding string value and measurement list
		WeatherStation SanFran = new WeatherStation("San Fran", sf_measurementList);
		WeatherStation Berkeley = new WeatherStation("Berkeley", berk_measurementList);
		WeatherStation Palo_Alto = new WeatherStation("Palo Alto", pa_measurementList);
		
		// Add each weatherstation to a list of weatherstation objects
		WeatherStation.addStation(SanFran);
		WeatherStation.addStation(Berkeley);
		WeatherStation.addStation(Palo_Alto);
		
		// Print to let the user know when section is about to be outputted to the console
	 	System.out.println("Assignment 3: Part 1 Test 1::  ");
	 	//Call the countTemperature Method, pass in 10
	    System.out.println(WeatherStation.countTemperature(10));
	    
	    System.out.println("Assignment 3: Part 1 Test 2::  ");
	 	//Call the countTemperature Method, pass in 5
	    System.out.println(WeatherStation.countTemperature(5));
	    
	    System.out.println("Assignment 3: Part 1 Test 3::  ");
	 	//Call the countTemperature Method, pass in -5
	    System.out.println(WeatherStation.countTemperature(-5));
	    
	    //Call the SVM Implementation code
	    Spark_SVM.machinelearning_IMDB();
		
	}

}
