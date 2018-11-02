//Written by Conor Hayes 10354355
public class MappedHolder {
	
	double key;
	int value;
	
	double mh_key;
	int mh_value;
	
	
	
	// Class created to hold key, value pairs to ensure mapreduce program can be used in the desired way
	// below there is just simple getting and setters to allow you to access the values for each instance of the class
	
	public MappedHolder(double key, int value)
	{
		this.key = key;
		this.value = value;
		setKey(key);
		setValue(value);
		
	}
	
	public void setKey(double key)
	{
		mh_key = key;
	}
	
	public void setValue(int value)
	{
		mh_value = value;
	}
	

	public double getKey()
	{
		 return mh_key;
	}
	
	public int getValue()
	{
		 return mh_value;
	}

	
	
	
}
