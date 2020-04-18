
package ru.smirnygatotoshka.docking;

import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.smirnygatotoshka.exception.TaskException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * @author SmirnygaTotoshka
 *
 */
public class DockJob extends Configured implements Tool
{
	private enum Counters{
		GOOD,BAD,ALL
	}

	public DockJob()
	{
	}
	public static final String INPUT = "input";
	public static final String OUTPUT = "output";
	public static final String NAME = "name";
	public static final String WORKSPACE = "workspace";
	public static final String MGLTOOLS = "mgltools";
	public static final String HOST = "host";
	public static final String NUM_REDUCERS = "nRed";
	public static final String PREPARE_GPF = "prepare_gpf";

	private static RunningJob job;

	/**
	 * Make object of Parameters from command line arguments. Need call for mapper and reducer before starting
	 * */
	public static Parameters configure(JobConf conf)
	{
		String in = conf.get(INPUT);
		String out = conf.get(OUTPUT);
		String name = conf.get(NAME);
		String work = conf.get(WORKSPACE);
		String mgl = conf.get(MGLTOOLS);
		String host = conf.get(HOST);
		String nRed = conf.get(NUM_REDUCERS);
		String pGPF = conf.get(PREPARE_GPF);

		String[] param ={in,out,name,mgl,work,host,nRed,pGPF};
		return new Parameters(param);
	}

	/**
	 * @author SmirnygaTotoshka
	 *
	 */
	public static class DockMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Task>
	{
		private Parameters parameters;
		private DockingClient client;
		private long numTasks;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			parameters = DockJob.configure(job);
			client = DockingClient.createClient(parameters);
			numTasks = getNumberTasks(job);
		}

		@Override
		public void map(LongWritable arg0, Text arg1, OutputCollector<Text, Task> arg2, Reporter arg3) throws IOException
		{
			//client.addMessage(client.SERVICE + "JOB ID:" + job.getID().toString()+"\n");
			client.addMessage(client.SERVICE+"MAP TASK=" + client.getNameSlave()+"\n");
			Dock dock = new ConfigParser(parameters).parse(arg1,client);
			Task[] tasks = dock.launch();
			for(int i = 0;i < tasks.length;i++)
			{
				Text key = new Text(arg0.toString() + "_" + i);
				//client.addMessage(client.SERVICE+"ID TASK=" + tasks[i].getNameTaskFolder() + ":GRIDCENTER="+tasks[i].getVector().toString()+"\n");
				arg2.collect(key,tasks[i]);
			}
			arg3.incrCounter(Counters.ALL,tasks.length);
			client.addMessage(client.SERVICE+"ALL=" + numTasks+"\n");
			client.send(parameters);

		}

		private long getNumberTasks(JobConf job) {
			try{
				BufferedReader reader = FileUtils.readFile(job.get(INPUT),parameters.getHDFS());
				return reader.lines().count();
			} catch (IOException e) {
				client.addMessage(client.SERVICE+"Cannot count input file lines.");
				return 1;
			}
		}
	}



	/**
	 * @author SmirnygaTotoshka
	 *
	 */
	public static class DockReducer extends MapReduceBase implements Reducer<Text, Task, Text, Text>
	{
		private DockingClient client;
		private Parameters parameters;
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			parameters = DockJob.configure(job);
			client = DockingClient.createClient(parameters);
		}

		@Override
		public void reduce(Text arg0, Iterator<Task> arg1, OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException
		{
			while(arg1.hasNext())
			{
				Task task = arg1.next();
				task.setParameters(parameters);
				task.setClient(client);
				String pathToDlg = task.launch();
				client.addMessage(client.SERVICE + "REDUCE TASK=" + client.getNameSlave()+":ID="+task.getNameTaskFolder() + ":PATH=" + pathToDlg+":STATUS="+task.isSuccessful()+"\n");
				arg2.collect(arg0, new Text(pathToDlg));
				if (!task.isSuccessful()) {
					arg3.incrCounter(Counters.BAD, 1);
					client.addMessage("TASK " + task.getNameTaskFolder() + " IS BAD!"+"\n");
				}
				else
					arg3.incrCounter(Counters.GOOD, 1);
			}
			client.send(parameters);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg0) throws Exception
	{
		Configuration conf = getConf();
		Path in = new Path(arg0[0]);
		Path out = new Path(arg0[1]);
		JobConf job_conf = new JobConf(conf,DockJob.class);

		FileInputFormat.setInputPaths(job_conf, in);
		FileOutputFormat.setOutputPath(job_conf,out);

		if (arg0.length != 7 & arg0.length != 8) throw new TaskException("Неправильное количество аргументов");
		job_conf.setJobName(arg0[2]);
		job_conf.set(INPUT,arg0[0]);
		job_conf.set(OUTPUT,arg0[1]);
		job_conf.set(NAME,arg0[2]);
		job_conf.set(MGLTOOLS,arg0[3]);
		job_conf.set(WORKSPACE,arg0[4]);
		job_conf.set(HOST,arg0[5]);
		job_conf.set(NUM_REDUCERS,arg0[6]);
		if (arg0.length == 8) {
			job_conf.set(PREPARE_GPF, arg0[7]);
		}
		else {
			job_conf.set(PREPARE_GPF, "");
		}

		job_conf.setMapperClass(DockMapper.class);
		job_conf.setMapOutputKeyClass(Text.class);
		job_conf.setMapOutputValueClass(Task.class);
		job_conf.setReducerClass(DockReducer.class);
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(Text.class);
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setNumReduceTasks(Integer.parseInt(arg0[6]));
		job_conf.set("mapreduce.task.timeout","0");
		job = JobClient.runJob(job_conf);

		return 0;
	}
	/**
	 * CMD arguments
	 * 0 - input csv in hdfs
	 * 1 - output path in hdfs
	 * 2 - task id
	 * 3 - path to MGLTools(local)
	 * 4 - output path(local)
	 * 5 - address to master node
	 * 6 - number of reduse task
	 * 7 - is needed prepare_gpf
	 * */
	public static void main(String[] args) throws Exception {
		Date start = new Date(System.currentTimeMillis());
		int res = ToolRunner.run(new Configuration(), new DockJob(), args);
		SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
		Date finish = new Date(System.currentTimeMillis());
		String message = "Task " + args[2] + " has completed with code " + res + ".\n" +
				"Statistics:\n" +
				"Start time " + formatter.format(start) + "\n" +
				"Success = " + job.getCounters().findCounter(Counters.GOOD).getValue() + "\n" +
				"Fail = " + job.getCounters().findCounter(Counters.BAD).getValue()  + "\n" +
				"All = " + job.getCounters().findCounter(Counters.ALL).getValue() + ".\n" +
				"Finish time " + formatter.format(finish) +
				". With the best wishes,Hadoop cluster of bioinformatics department MBF RNRMU.";
		SenderMail senderMail = new SenderMail(message);
		senderMail.send();

		System.exit(res);
	}
}