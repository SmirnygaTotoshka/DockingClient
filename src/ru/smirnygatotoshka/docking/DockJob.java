
package ru.smirnygatotoshka.docking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

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

	public DockJob(ClusterProperties cluster) {
		this.cluster = cluster;
	}

	private static RunningJob job;
	private JobConf jobConf;
	private ClusterProperties cluster;
	//private final String
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path in = new Path(arg0[0]);//TODO - fix param
		Path out = new Path(cluster.getOutputPath());
		jobConf = new JobConf(conf, DockJob.class);

		FileInputFormat.setInputPaths(jobConf, in);
		FileOutputFormat.setOutputPath(jobConf, out);

		jobConf.set(ClusterProperties.OUTPUT, cluster.getOutputPath());
		jobConf.set(ClusterProperties.NAME, cluster.getTaskName());
		jobConf.set(ClusterProperties.WORKSPACE, cluster.getWorkspaceLocalDir());
		jobConf.set(ClusterProperties.MGLTOOLS, cluster.getPathToMGLTools());
		jobConf.set(ClusterProperties.HOST, cluster.getIpAddressMasterNode());
		jobConf.set(ClusterProperties.NUM_MAPPERS, cluster.getMapperNumber());

		jobConf.setJobName(cluster.getTaskName());
		jobConf.setMapperClass(DockMapper.class);
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(DockResult.class);
		jobConf.setReducerClass(DockReducer.class);
		jobConf.setOutputKeyClass(LongWritable.class);
		jobConf.setOutputValueClass(Text.class);
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setNumMapTasks(Integer.parseInt(cluster.getMapperNumber()));
		jobConf.set("mapreduce.task.timeout", "0");
        Statistics.getInstance().setNumTasks(FileUtils.readFile(arg0[0],FileSystem.get(conf)).size());
        job = JobClient.runJob(jobConf);

		return 0;
	}

	/**
	 * First argument - dock id, type Text
	 * Second argument - dock, type Dock
	 * Third argument - dock id, type Text //TODO
	 * Fourth argument - docking result , type DockResult
	 *
	 * @author SmirnygaTotoshka
	 */
	public static class DockMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DockResult> {
		private DockingClient client;
		private ClusterProperties clusterProperties;

		@Override
		public void configure(JobConf job) {
			this.clusterProperties = new ClusterProperties(job);
			this.client = new DockingClient(this.clusterProperties);
		}

		@Override
		public void map(LongWritable id, Text line, OutputCollector<Text, DockResult> outputCollector, Reporter reporter) throws IOException {
			Dock d = new Dock(line, client, clusterProperties, id);
			DockResult result = d.launch();
			outputCollector.collect(new Text(result.getId()), result);
			if (!result.isSuccess())
				client.send(Statistics.Counters.EXECUTION_FAIL, 1);
			client.send(Statistics.Counters.ALL, 1);
		}
	}
	/**
	 * First argument - dock id, type Text
	 * * Second argument - dock result without success and energy in normal, type Dock result
	 * * Third argument - dock id, type Text TODO
	 * * Fourth argument - path to dlg file in hdfs, type Text
	 *
	 * @author SmirnygaTotoshka
	 */
	public static class DockReducer extends MapReduceBase implements Reducer<Text, DockResult, LongWritable, Text> {

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
		public void reduce(Text text, Iterator<DockResult> iterator, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
			while (iterator.hasNext()) {
				DockResult result = iterator.next();
				if (result.isSuccess()) {
					if (error_message.contentEquals("")) {
						if (hasSuccessDLG(result.getPathDLGinHDFS())) {
							float energy = parseHistogram(result.getPathDLGinHDFS());
							if (energy == -100000000F) {
								result.fail("Не найдено значение энергии");
								client.send(Statistics.Counters.ANALYZE_FAIL, 1);
							} else if (energy == -100000000F - 1) {
								result.fail("Не удалось открыть DLG.");
								client.send(Statistics.Counters.ANALYZE_FAIL, 1);
							} else if (energy == -100000000F - 2) {
								result.fail("Не удалось считать энергию.");
								client.send(Statistics.Counters.ANALYZE_FAIL, 1);
							} else {
								result.success(energy);
								client.send(Statistics.Counters.SUCCESS, 1);
							}
						} else {
							result.fail("Провал autodock.");
							client.send(Statistics.Counters.EXECUTION_FAIL, 1);
						}
					} else {
						result.fail(error_message);
						client.send(Statistics.Counters.ANALYZE_FAIL, 1);
					}
				}
				outputCollector.collect(result.getKey(), result.getText());
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
				return -100000000F;
			} catch (IOException e) {
				return -100000000F - 1;
			} catch (NumberFormatException e) {
				return -100000000F - 2;
			}
		}
	}



}