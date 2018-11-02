//Written by Conor Hayes 10354355
public class Measurement 
{
	
	int time;
	int m_time;
	double temperature;
	double m_temp;
	
	// Class created to hold measurements
	// below there is just simple getting and setters to allow you to access the values for each instance of the class
	
	public Measurement(int time, double temperature)
	{
		this.time = time;
		this.temperature = temperature;
		setTime(time);
		setTemperature(temperature);
	}
	
	public void setTime(int time)
	{
		m_time = time;
		
	}
	
	public void setTemperature(double temperature)
	{
		m_temp = temperature;
	}
	
	public int getTime()
	{
		return m_time;
	}
	
	public double getTemp()
	{
		return m_temp;
	}

}