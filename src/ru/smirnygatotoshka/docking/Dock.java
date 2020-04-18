
package ru.smirnygatotoshka.docking;

import java.io.BufferedReader;
import java.io.IOException;


import ru.smirnygatotoshka.exception.GPFError;

/**
 * @author SmirnygaTotoshka
 *
 */
public class Dock implements Launchable<Task[]> {

	private Parameters parameters;
	private DockingClient client;
	/**
	 * Must call before using class
 	* */
	public void setClient(DockingClient client) {
		this.client = client;
	}

	private DockingProperties dockingProperties;
	private Task[] tasks;

	/**
	 * 
	 */
	public Dock(DockingProperties properties, Parameters parameters)
	{
		this.dockingProperties = properties;
		tasks = new Task[properties.getDivisionNumber()];
		this.parameters = parameters;
	}

	/**
	 * @return the dockingProperties
	 */
	public DockingProperties getDockingProperties()
	{
	    return dockingProperties;
	}

	/**
	 * */
	@Override
	public Task[] launch()
	{
	    try
		{
			Vector3 start = parseGPF(dockingProperties.getStartGPFPath());
			Vector3 finish = parseGPF(dockingProperties.getFinishGPFPath());
			Vector3 increment = new Vector3(0,0,0);
			if (dockingProperties.getDivisionNumber() != 1)
				increment = increment.add(finish.minus(start)).divideIntoNumber(dockingProperties.getDivisionNumber()-1);// -1 �.� ���� ��� ��������� �����
			//messageToServer +=
			for (int i = 0; i < tasks.length;i++)
			{
				tasks[i] = new Task(dockingProperties,start.add(increment.multipyByNumber(i)),i,parameters);
			}
			return tasks;
	    }
	    catch(ArithmeticException e)
	    {
            client.addMessage(e.getMessage());
            return null;
	    }	
	}

	private Vector3 parseGPF(String path)
	{
		try
		{
	         BufferedReader br = FileUtils.readFile(path, parameters.getHDFS());
	         String line;
	         final String seeking = "gridcenter";
	         Vector3 vector = new Vector3(0,0,0);
	         while ((line = br.readLine()) != null)
	         {
	        	 if (line.contains(seeking))
	        	 {
	        		 String[] content = line.split(" ");
	        		 if (content[1].contentEquals("auto")) throw new GPFError("Координаты грида должны быть заданы явно.");
	        		 	vector = new Vector3(Float.parseFloat(content[1]),Float.parseFloat(content[2]),Float.parseFloat(content[3]));
	        		 break;
	        	 }
	         }
	         br.close();
	         return vector;
		}
		catch(IOException | GPFError e)
		{
			client.addMessage(e.getMessage());
			return new Vector3(0,0,0);

		}
	}
}
