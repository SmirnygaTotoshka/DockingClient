
package ru.smirnygatotoshka.docking;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Random;

/**
 * Основная логика Map/Reduce
 * Map - Проведение докинга
 * Reduce - Парсинг dlg и объединение результатов
 *
 * @author SmirnygaTotoshka
 */
public class DockJob extends Configured implements Tool {

	public DockJob(ClusterProperties cluster) {
		this.cluster = cluster;
		this.jobConf = cluster.getJobConf();
	}

	private JobConf jobConf;
	private ClusterProperties cluster;
	//private final String
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg0) throws IOException, TaskException {
		Path in = new Path(arg0[0]);//TODO - fix param
		Path out = new Path(cluster.getOutputPath());

		FileInputFormat.setInputPaths(jobConf, in);
		FileOutputFormat.setOutputPath(jobConf, out);

		jobConf.set(ClusterProperties.OUTPUT, cluster.getOutputPath());
		jobConf.set(ClusterProperties.NAME, cluster.getTaskName());
		jobConf.set(ClusterProperties.WORKSPACE, cluster.getWorkspaceLocalDir());
		jobConf.set(ClusterProperties.MGLTOOLS, cluster.getPathToMGLTools());
		jobConf.set(ClusterProperties.HOST, cluster.getIpAddressMasterNode());
		jobConf.set(ClusterProperties.PORT, Integer.toString(cluster.getPort()));
		jobConf.set(ClusterProperties.NUM_MAPPERS, cluster.getMapperNumber());

		jobConf.setJobName(cluster.getTaskName());
		jobConf.setMapperClass(DockMapper.class);
		jobConf.setMapOutputKeyClass(LongWritable.class);
		jobConf.setMapOutputValueClass(Text.class);
		//jobConf.setReducerClass(DockReducer.class);
		jobConf.setOutputKeyClass(LongWritable.class);
		jobConf.setOutputValueClass(Text.class);
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setNumMapTasks(Integer.parseInt(cluster.getMapperNumber()));
		jobConf.setNumReduceTasks(0);
		jobConf.set("mapreduce.task.timeout", "0");
        Statistics.getInstance().incrCounter(Statistics.Counters.ALL,FileUtils.readFile(arg0[0],FileSystem.get(jobConf)).size());
		RunningJob job = JobClient.runJob(jobConf);

		return 0;
	}

	/**
	 * First argument - dock id, type Text
	 * Second argument - dock, type Dock
	 * Third argument - dock id, type Text
	 * Fourth argument - docking result , type DockResult
	 *
	 * @author SmirnygaTotoshka
	 */
	public static class DockMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		private DockingClient client;
		private ClusterProperties clusterProperties;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			this.clusterProperties = new ClusterProperties(job);
			try {
				this.client = new DockingClient(this.clusterProperties,"map_" + (new Random().nextInt(10000)));
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		@Override
		public void map(LongWritable id, Text line, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
			Dock d = new Dock(line, clusterProperties, id);
			if (!d.hasTrouble()) {
				DockResult result = d.launch();
				if (result.isSuccess()) {
					outputCollector.collect(result.getKey(), result.getText());
					client.send(Statistics.Counters.SUCCESS, 1);
				} else
					client.send(Statistics.Counters.FAILED, 1);
				System.out.println(result.toString());
			}
			else{
				System.out.println(d.getTrouble());
			}
			client.close();
		}
	}
	/**
	 * First argument - dock id, type Text
	 * * Second argument - dock result without success and energy in normal, type Dock result
	 * * Third argument - dock id, type LongWritable
	 * * Fourth argument - full dock result, type Text
	 *
	 * @author SmirnygaTotoshka
	 */
/*	public static class DockReducer extends MapReduceBase implements Reducer<Text, DockResult, NullWritable, Text> {

		private DockingClient client;
		private ClusterProperties clusterProperties;
		private FileSystem hdfs;
		private String error_message;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			super.configure(job);
			this.clusterProperties = new ClusterProperties(job);
			try {
				this.client = new DockingClient(this.clusterProperties,"reduce_" + (new Random().nextInt(10000)));
				this.hdfs = FileSystem.get(job);
				this.error_message = "";
			} catch (IOException e) {
				error_message = "Не удалось установить контакт с файловой системой.";
			}
		}

		@Override
		public void reduce(Text text, Iterator<DockResult> iterator, OutputCollector<NullWritable, Text> outputCollector, Reporter reporter) throws IOException {
			while (iterator.hasNext()) {
				DockResult result = iterator.next();
				if (result.isSuccess() && error_message.contentEquals("")) {
					if (error_message.contentEquals("")) {
						if (result.hasSuccessDLG(hdfs)) {
							float energy = parseHistogram(result.getPathDLGinHDFS());
							if (energy == -100000F) {
								result.fail("Не найдено значение энергии");
								client.send(Statistics.Counters.ANALYZE_FAIL, 1);
								reporter.incrCounter(Statistics.Counters.ANALYZE_FAIL,1);
							} else if (energy == -100000F - 1) {
								result.fail("Не удалось открыть DLG.");
								client.send(Statistics.Counters.ANALYZE_FAIL, 1);
								reporter.incrCounter(Statistics.Counters.ANALYZE_FAIL,1);
							} else if (energy == -100000F - 2) {
								result.fail("Не удалось считать энергию.");
								client.send(Statistics.Counters.ANALYZE_FAIL, 1);
								reporter.incrCounter(Statistics.Counters.ANALYZE_FAIL,1);
							} else {
								result.success(energy);
								client.send(Statistics.Counters.SUCCESS, 1);
								reporter.incrCounter(Statistics.Counters.SUCCESS,1);
							}
						}
						else {
							result.fail("Провал autodock.");
							client.send(Statistics.Counters.EXECUTION_FAIL, 1);
							reporter.incrCounter(Statistics.Counters.EXECUTION_FAIL,1);
						}
					} else {
						result.fail(error_message);
						client.send(Statistics.Counters.ANALYZE_FAIL, 1);
						reporter.incrCounter(Statistics.Counters.ANALYZE_FAIL,1);
					}
				}
				System.out.println(result.toString());
				outputCollector.collect(result.getKey(), result.getText());
			}
		}
		*//**
		 * @return DockResult.FAILED_ENERGY - 2 - <code>NumberFormatException</code>
		 *//*
		private float parseHistogram(String pathToDLG) {
			try {
				*//**
				 * Ключевая фраза,после которой начинается искомая таблица с энергиями
				 *//*
				final String START_HISTOGRAM = "CLUSTERING HISTOGRAM";
				ArrayList<String> lines = FileUtils.readFile(pathToDLG, hdfs);
				int i = -1;
				boolean findIt = false;
				for (int j = 0; j < lines.size(); j++) {
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
				return -100000F;
			} catch (IOException e) {
				return -100000F - 1;
			} catch (NumberFormatException e) {
				return -100000F - 2;
			}
		}

	}*/



}