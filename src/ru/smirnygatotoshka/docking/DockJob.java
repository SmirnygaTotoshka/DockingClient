
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
		jobConf.setMaxMapAttempts(5);
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
		}

		@Override
		public void map(LongWritable id, Text line, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
			Dock d = new Dock(line, clusterProperties, id);
			DockResult result = new DockResult(d.getDockingProperties().getId(),d.getDockingProperties().getPathToFiles(),id,clusterProperties.getJobConf());
			if (!d.hasTrouble()) {
				result = d.launch();
				outputCollector.collect(result.getKey(), result.getText());
				try {
					client = new DockingClient(this.clusterProperties,"map_" + (new Random().nextInt(10000)));
					client.send(result);
					client.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else{
				result.fail(d.getTrouble());
				d.dispose();
			}
			System.out.println(result.toString());
		}
	}
}