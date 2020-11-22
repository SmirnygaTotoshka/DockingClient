
package ru.smirnygatotoshka.docking;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import ru.smirnygatotoshka.exception.TaskException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author SmirnygaTotoshka
 * TODO - formCommand
 * - launchCommand
 * -isSuccess
 */
public class Dock implements Writable {

	private DockingClient client;
	private DockingProperties dockingProperties;
	private ClusterProperties clusterProperties;
	private transient Runtime runtime = Runtime.getRuntime();

	/**
	 * Парсит строку из файла с описанием задач
	 */
	public Dock(Text line, DockingClient client, ClusterProperties clusterProp) {
		String[] description = line.toString().split(",");

		String path = description[0];
		String r = description[1];
		String rFlex = description[2];
		String lig = description[3];
		String gpfName = description[4];
		String gpfParam = description[5];
		String dpfName = description[6];
		String dpfParam = description[7];

		this.dockingProperties = new DockingProperties(path, r, rFlex, lig, gpfName, gpfParam, dpfName, dpfParam);
		this.client = client;
		this.clusterProperties = clusterProp;
	}

	/**
	 * @return the dockingProperties
	 */
	public DockingProperties getDockingProperties()
	{
	    return dockingProperties;
	}

	/**
	 * start on local node pipe of scripts
	 * 1) prepare_gpf4.py
	 * 2) autogrid
	 * 3) prepare_dpf42.py
	 * 4) autodock
	 * @return path to DLF file in hdfs
	 * */
	public String launch() {
		try {
//TODO
		} catch (Exception e) {

		}
	}

	private int launchCommand(String cmd) throws TaskException {
		try {
			Process process = runtime.exec(cmd);
			return process.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();//TODO
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		client.write(dataOutput);
		dockingProperties.write(dataOutput);
		clusterProperties.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		client.readFields(dataInput);
		dockingProperties.readFields(dataInput);
		clusterProperties.readFields(dataInput);
	}
}
