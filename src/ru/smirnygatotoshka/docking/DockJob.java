
package ru.smirnygatotoshka.docking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import ru.smirnygatotoshka.exception.TaskException;

import java.io.IOException;
import java.util.Iterator;

/**
 * Основная логика Map/Reduce
 * Map - Проведение докинга
 * Reduce - Парсинг dlg и объединение результатов
 *
 * @author SmirnygaTotoshka
 */
public class DockJob extends Configured implements Tool {
	RunningJob getJob() {
		return job;
	}

	public DockJob() {
	}

	private static RunningJob job;

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path in = new Path(arg0[0]);//TODO - fix param
		Path out = new Path(arg0[1]);
		JobConf job_conf = new JobConf(conf, DockJob.class);

		FileInputFormat.setInputPaths(job_conf, in);
		FileOutputFormat.setOutputPath(job_conf, out);

		if (arg0.length != 7 || arg0.length != 8) throw new TaskException("Неправильное количество аргументов");
		job_conf.setJobName(arg0[2]);
		job_conf.set(INPUT, arg0[0]);
		job_conf.set(OUTPUT, arg0[1]);
		job_conf.set(NAME, arg0[2]);
		job_conf.set(MGLTOOLS, arg0[3]);
		job_conf.set(WORKSPACE, arg0[4]);
		job_conf.set(HOST, arg0[5]);
		job_conf.set(NUM_REDUCERS, arg0[6]);
		if (arg0.length == 8) {
			job_conf.set(PREPARE_GPF, arg0[7]);
		} else {
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
		job_conf.set("mapreduce.task.timeout", "0");
		job = JobClient.runJob(job_conf);

		return 0;
	}

	enum Counters {
		GOOD, BAD, ALL
	}


	/**
	 * @author SmirnygaTotoshka
	 *
	 */
	public static class DockReducer extends MapReduceBase implements Reducer<Text, Task, Text, Text> {
		private DockingClient client;
		private Parameters parameters;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			parameters = DockJob.configure(job);
			client = DockingClient.createClient(parameters);
		}

		@Override
		public void reduce(Text arg0, Iterator<Task> arg1, OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
			while (arg1.hasNext()) {
				Task task = arg1.next();
				task.setParameters(parameters);
				task.setClient(client);
				String pathToDlg = task.launch();
				client.addMessage(client.SERVICE + "REDUCE TASK=" + client.getNameSlave() + ":ID=" + task.getNameTaskFolder() + ":PATH=" + pathToDlg + ":STATUS=" + task.isSuccessful() + "\n");
				arg2.collect(arg0, new Text(pathToDlg));
				if (!task.isSuccessful()) {
					arg3.incrCounter(Counters.BAD, 1);
					client.addMessage("TASK " + task.getNameTaskFolder() + " IS BAD!" + "\n");
				} else
					arg3.incrCounter(Counters.GOOD, 1);
			}
			client.send(parameters);
		}
	}

	/**
	 * First argument - dock id, type Text
	 * Second argument - dock, type Dock
	 * Third argument - dock id, type Text
	 * Fourth argument - path to dlg file in hdfs, type Text
	 *
	 * @author SmirnygaTotoshka
	 */
	public static class DockMapper extends MapReduceBase implements Mapper<Text, Dock, Text, Text> {
		private DockingClient client;
		private ClusterProperties clusterProperties;

		@Override
		public void configure(JobConf job) {
			this.clusterProperties = new ClusterProperties(job);
			this.client = DockingClient.createClient(this.clusterProperties);
		}

		@Override
		public void map(Text text, Dock dock, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
			client.addMessage(client.SERVICE + "MAP TASK=" + client.getNameSlave() + "\n");
			Dock d = new Dock(text, client);
			String path = d.launch();
		}

		/*private Parameters parameters;
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
		}*/
	}

}