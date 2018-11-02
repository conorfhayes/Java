//Written by Conor Hayes 10354355
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class WeatherStation 
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
	List<MappedHolder> Map_mapped = new ArrayList<MappedHolder>();
	
	

	
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

	//Method maxtempeerature, this method returns the max temp from a meausrement list between two time measurements
	public double maxTemperature(int startTime, int endTime)
	{
		//creates a parrellstream on measurementList
		measurementList.parallelStream()
		//returns only the values that meet the operation critera
		// here we filter the measurementlist to give us the results that fall in between the desired times
		.filter(m -> m.getTime() >= startTime && m.getTime() <= endTime)
		//allows the user to perform an operation on each element of the stream that meets the filter criteria
		// here we are adding all temps that meet the criteria between start and end time to a list
		.forEach(m-> maxTemp_count.add(m.getTemp())
		);
		
		// max temp is created to hold max temperautre of the list created above, colections.max returns the max value in the list
		// hence this is the max temp between the desired time intervals
		maxTemp = Collections.max(maxTemp_count);
		return maxTemp;
	}
	 
	// Method to find how often temperatures occur between intervals
	// t1-r and t1+ r and also interval t2-r and t2+r
	public static void countTemperature(double t1, double t2, double r)
	{
		//setting variables to hold intervals
		double t1_plus = t1 + r;
		double t1_minus = t1 - r;
		double t2_plus = t2 + r;
		double t2_minus = t2 - r;
		
		// List of MappedHolder opjects. This is created to hold the key value pairs for map reduce
		List<MappedHolder> Map_mapped = new ArrayList<MappedHolder>();
		
		// Passing empty mapped holder to my map function
		// In map reduce the point of the map phase is to collect your data set and clean it up. Most examples
		// ensure that one list of data is the output with key value pairs to all the shuffle and reduce operations
		// to be stream lines, that is the purpose of my mapped phase, as I have a single list, with key value pairs as output
		Map(Map_mapped);
		
		// passing our intervales and the map_mapped that was returned from our mapped phase
		// in order to make the shuffle operation generic I had to cast it to a list of list of object mapped holder
		// this allowed me to return non static values and use the mapreduce multiple times form my test class
		
		// Shuffle phase is used generally to perform an operation to optain a desired outcome.
		// Usually in mapreduce the difference tasks are passed to may many machines to be shuffled,
		// not possible in this example but I have spit the task into segments to display how this could be done
		List<List<MappedHolder>> ListShuffle = Shuffle(t1_plus, t1_minus, t2_plus, t2_minus, Map_mapped);
		
		// passing our map_mapped lists that were returned from our shuffle phase
		// in order to make the reduce operation generic I had to cast it to a list of ints called array holder
		// this allowed me to return non static values and use the mapreduce multiple times from my test class
		
		// the point of reduce is to aggregate the data from the previous phase
		// in our example we are looking to 'count' the numbers of our value in our key value pairs
		// but first we must 'reduce' the key values, this is done by checking to see if any value occur more than once
		// if so we simply remove the extra key,value or key,values and increment the value on the key we are checking
		int [] array_holder = Reduce(ListShuffle.get(0), ListShuffle.get(1));
		
		//create a new list to hold the desired assignment out put format
		List<double[]> MapReduce_values = new ArrayList<double[]>(2);

		// add t1, and its corresponding summed value from the reduce method
      	double[] element1 = new double[] {t1, array_holder[0]};
     // add t1, and its corresponding summed value from the reduce method
      	double[] element2 = new double[] {t2, array_holder[1]};

      	// add those values to our list this allows us to output the value as required for the assignment
      	MapReduce_values.add(element1);
      	MapReduce_values.add(element2);
      	
      	// print the values to the console, when called
      	for (double[] MapReduce : MapReduce_values) {
	    	System.out.println("(" + MapReduce[0] + ", " + MapReduce[1] + ")");
			
		    }
		
      	//return
		return;

		
	}
		
		public static List<MappedHolder> Map(List<MappedHolder> Map_mapped)
		{
			// Creating a Stream on my List of weatherstation objects, stations
			// a parallel stream was not an appropriate operation here
			stations.stream()
			// since stations is a list of objects that contains a string and a list of objects 
			// we must perform plat map on the list, this will 'flatten' the list stations and allow us to 
			// create an list of key value pairs, with key of temp, and value of 1
			// we perform the flat map on get measurement list, this returns the measurement list and allows us to access 
			//each value in each measurement list in stations
			.flatMap(p -> p.getMeasurementList().stream())
			// a simple for each allows us to add the temp values from each measurementlist as key and 1 as value
			.forEach(b -> Map_mapped.add(new MappedHolder(b.getTemp(), 1)));
			
			//now we return a List of object mappedholder, that contains key value pairs
			// with key = all temperature values for all weatherstations and value of 1 on each
			return Map_mapped;
			
		}
		
		public static List<List<MappedHolder>> Shuffle(double t1_p, double t1_m, double t2_p, double t2_m, List<MappedHolder> Map_mapped)
		{
			//in order to simplifiy this operation I created a list for each interval t1, and t2
			List<MappedHolder> t1_mapped = new ArrayList<MappedHolder>();
			List<MappedHolder> t2_mapped = new ArrayList<MappedHolder>();
			
			//stream on mapped map
			Map_mapped.stream()
			// Using map to get the key(temp)
			.map(p -> p.getKey())
			// where the key meets the criteria that it falls between t1 interval
			.filter(a -> a >= t1_m && a <= t1_p)
			// we add it to a new mappedmap instance called t1_mapped, which again 
			// holds temp and value 1 but only the temps that fall into the t1 interval
			.forEach(a -> t1_mapped.add(new MappedHolder(a, 1)));
			
			//stream on mapped map
			Map_mapped.stream()
			// Using map to get the key(temp)
			.map(p -> p.getKey())
			// where the key meets the criteria that it falls between t1 interval
			.filter(b -> b >= t2_m && b <= t2_p)
			// we add it to a new mappedmap instance called t1_mapped, which again 
			// holds temp and value 1 but only the temps that fall into the t1 interval
			.forEach(b -> t2_mapped.add(new MappedHolder(b, 1)));
		
			//Operations to show the output of each stream
			//t1_mapped.stream()
			//.forEach(m -> System.out.println(m.getKey()));
			
			//t2_mapped.stream()
			//.forEach(m -> System.out.println(m.getKey()));
		
			// Return a list containing, t1_mapped and t2_mapped
			return Arrays.asList(t1_mapped, t2_mapped);
			
		}
		
		public static int [] Reduce(List<MappedHolder> t1_mapped, List<MappedHolder> t2_mapped)
		{	
			int t1_frequency =0;
			int t2_frequency =0;
			int t1_value_sum = 0;
			int t2_value_sum = 0;
			
			int [] value_sum = new int [2];
			
			//Create new key value pair object lists to hold t1_mapped and t2_mapped, after they have been 'reduced'
			List<MappedHolder> t1_reduced = new ArrayList<MappedHolder>();
			List<MappedHolder> t2_reduced = new ArrayList<MappedHolder>();
			
			// to find the duplicated we must fist isolate the unique keys
			Double[] t1_uniqueKeys = t1_mapped.parallelStream()
					//here we get the key and check using distinct, if it is unique
					.map(m -> m.getKey()).distinct()
					//if so add it to the unique keys array
					.toArray(size -> new Double[size]);
			
			// then we need to get a list of all the keys
			List<Double> t1_Keys = new ArrayList<>();
					t1_mapped.stream()
					//get the key from the stream
					.map(m -> m.getKey())
					// add it to list keys
					.forEach(b -> t1_Keys.add(b));
			
			// below we repeat the same operation as above for t1 now on t2
			Double[] t2_uniqueKeys = t2_mapped.parallelStream()
					.map(m -> m.getKey()).distinct()
					.toArray(size -> new Double[size]);
			
			List<Double> t2_Keys = new ArrayList<>();
					t2_mapped.stream()
					.map(m -> m.getKey())
					.forEach(b -> t2_Keys.add(b));
					
			//using a simple for loop we use the collections. frequency to count how often a key occur in thearray
			for (Double t1_uniqueKey : t1_uniqueKeys) 
			{
	            t1_frequency = Collections.frequency(t1_Keys, t1_uniqueKey);
	            //System.out.println(t1_uniqueKey +" occured " + t1_frequency + " times");
	            
	            // we then add the unique key into a new object list t1_reduced
	            // here is the main reduction phase, as for example if value 10, 1 occurs twice
	            // it will now only occur once in the object list but it will have a value of 10,2
	            t1_reduced.add(new MappedHolder(t1_uniqueKey, t1_frequency));
	        }
				
			// we now do the same operation for t2 as we did for t1
			for (Double t2_uniqueKey : t2_uniqueKeys) 
			{
	            t2_frequency = Collections.frequency(t2_Keys, t2_uniqueKey);
	            //System.out.println(t2_uniqueKey +" occured " + t2_frequency + " times");
	            t2_reduced.add(new MappedHolder(t2_uniqueKey, t2_frequency));
	        }
			
			// finally we perform a reduce operation on t1_reduce
				t1_value_sum = t1_reduced.parallelStream()
				// here we get the values from the key value pair in t1_reduce
				.mapToInt(o -> o.getValue())
				// reduce allow us to perform an operation on the overall stream (aritmetic)
				// this code allows us to sum all the values in the t1_reduce object list and assign it to an in call
				// t1_value
				.reduce(0, (x, y) -> x + y)
				;
				
				// again we perform the same operation on t2_reduced
				t2_value_sum = t2_reduced.parallelStream()
				.mapToInt(o -> o.getValue())
				.reduce(0, (x, y) -> x + y)
				;
				
				// we add the sum values for both operations to an array called value sum
				value_sum[0] = t1_value_sum;
				value_sum[1] = t2_value_sum;
		
			// we return value sum to be used in out counttemperature method
			return value_sum;
			
		}
		
		
}

	
	



	
