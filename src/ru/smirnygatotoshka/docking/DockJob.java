
package ru.smirnygatotoshka.docking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import ru.smirnygatotoshka.exception.TaskException;

import java.io.IOException;
import java.util.ArrayList;
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
	private JobConf jobConf;

	//private final String
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path in = new Path(arg0[0]);//TODO - fix param
		Path out = new Path(arg0[1]);
		jobConf = new JobConf(conf, DockJob.class);

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
		job_conf.setNumMapTasks(Integer.parseInt(arg0[6]));
		job_conf.set("mapreduce.task.timeout", "0");
		job = JobClient.runJob(jobConf);

		return 0;
	}

	enum Counters {
		SUCCESS, FAIL, EXCEPTION, ALL
	}


	/**
	 * First argument - dock id, type Text
	 * * Second argument - dock result without success and energy in normal, type Dock result
	 * * Third argument - dock id, type Text
	 * * Fourth argument - path to dlg file in hdfs, type Text
	 *
	 * @author SmirnygaTotoshka
	 */
	public static class DockReducer extends MapReduceBase implements Reducer<Text, DockResult, Text, Text> {
		private DockingClient client;
		private ClusterProperties clusterProperties;
		private FileSystem hdfs;
		private String error_message;

		@Override
		public void configure(JobConf job) {
			this.clusterProperties = new ClusterProperties(job);
			this.client = new DockingClient(this.clusterProperties);
			try {
				this.hdfs = FileSystem.get(job);
				this.error_message = "";
			} catch (IOException e) {
				error_message = "Не удалось установить контакт с файловой системой.";
			}
		}

		@Override
		public void reduce(Text text, Iterator<DockResult> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
			while (iterator.hasNext()) {
				DockResult result = iterator.next();
				if (error_message.contentEquals("")) {
					if (hasSuccessDLG(result.getPathDLGinHDFS())) {
						float energy = parseHistogram(result.getPathDLGinHDFS());
						if (energy == DockResult.FAILED_ENERGY) {
							result.fail("Не найдено значение энергии");
							reporter.incrCounter(Counters.FAIL, 1);
						} else if (energy == DockResult.FAILED_ENERGY - 1) {
							result.fail("Не удалось открыть DLG.");
							reporter.incrCounter(Counters.FAIL, 1);
						} else if (energy == DockResult.FAILED_ENERGY - 2) {
							result.fail("Не удалось считать энергию.");
							reporter.incrCounter(Counters.FAIL, 1);
						} else {
							result.success(energy);
							reporter.incrCounter(Counters.SUCCESS, 1);
						}
					} else {
						result.fail("Провал autodock.");
						reporter.incrCounter(Counters.EXCEPTION, 1);
					}
				} else {
					result.fail(error_message);
					reporter.incrCounter(Counters.FAIL, 1);
				}
				outputCollector.collect(text, result.getText());
			}
		}

		private boolean hasSuccessDLG(String pathToDLG) {
			try {
				ArrayList<String> lines = FileUtils.readFile(pathToDLG, hdfs);
				if (lines.size() == 0)
					return false;
				else {
					for (int i = lines.size() - 1; i > lines.size() - 21; i--)
						if (lines.get(i).contains("Successful Completion"))
							return true;
					return false;
				}
			} catch (IOException e) {
				return false;
			}
		}

		/**
		 * @return DockResult.FAILED_ENERGY - 2 - <code>NumberFormatException</code>
		 */
		private float parseHistogram(String pathToDLG) {
			try {
				/**
				 * Ключевая фраза,после которой начинается искомая таблица с энергиями
				 */
				final String START_HISTOGRAM = "CLUSTERING HISTOGRAM";
				ArrayList<String> lines = FileUtils.readFile(pathToDLG, hdfs);
				int i = -1;
				boolean findIt = false;
				for (int j = lines.size() - 1; j >= 0; j--) {
					if (lines.get(j).contains(START_HISTOGRAM))
						findIt = true;
					if (findIt)
						i++;
					if (i == 9) // Данные таблицы начинаются на 9 строке после ключевой фразы
					{
						String[] data = lines.get(j).split("\\s+\\|");
						return Float.parseFloat(data[1].replace(" ", ""));
					}
				}
				return DockResult.FAILED_ENERGY;
			} catch (IOException e) {
				return DockResult.FAILED_ENERGY - 1;
			} catch (NumberFormatException e) {
				return DockResult.FAILED_ENERGY - 2;
			}
		}
	}
	/*

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
		}*/


	/**
	 * First argument - dock id, type Text
	 * Second argument - dock, type Dock
	 * Third argument - dock id, type Text
	 * Fourth argument - path to dlg file in hdfs, type Text
	 *
	 * @author SmirnygaTotoshka
	 */
	public static class DockMapper extends MapReduceBase implements Mapper<Text, Dock, Text, DockResult> {
		private DockingClient client;
		private ClusterProperties clusterProperties;

		@Override
		public void configure(JobConf job) {
			this.clusterProperties = new ClusterProperties(job);
			this.client = new DockingClient(this.clusterProperties);
		}

		@Override
		public void map(Text text, Dock dock, OutputCollector<Text, DockResult> outputCollector, Reporter reporter) throws IOException {
			Dock d = new Dock(text, client, clusterProperties);
			DockResult result = d.launch();
			outputCollector.collect(text, result);
			if (!result.isSuccess())
				reporter.incrCounter(Counters.EXCEPTION, 1);
			reporter.incrCounter(Counters.ALL, 1);
		}

		/*
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
*/
	}

}