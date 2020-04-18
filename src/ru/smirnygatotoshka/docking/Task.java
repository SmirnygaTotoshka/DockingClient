package ru.smirnygatotoshka.docking;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.WritableComparable;
import ru.smirnygatotoshka.exception.GPFError;
import ru.smirnygatotoshka.exception.TaskException;


/**
 * There is class containing main logic. Launch on datanode scripts for autogrid, autogrid,
 * scripts for autodock and autodock. Step of reduce task.
 *
 * @author SmirnygaTotoshka
 */
public class Task implements Launchable<String>,WritableComparable<Task> {

	private Path gpf;
	private DockingProperties properties;
	private Vector3 vector;
	private boolean launchable = true;



	private boolean successful = false;
	private int num;//step number
	private String local_task_dir;
	private Parameters parameters;
	private DockingClient client;
	public boolean isSuccessful() {
		return successful;
	}
	/**
	 * Must call before using class
	 * */
	public void setClient(DockingClient client) {
		this.client = client;
	}

	public Task()
	{
	    //parameters = parameters; parameters get from Dock.java(mapper class)
		//gpf = parameters.getHDFS().getHomeDirectory();
		properties = new DockingProperties();
		vector = new Vector3(0,0,0);
		num = 0;
		//local_task_dir = parameters.getPathToWorkspace() + File.separator + getNameTaskFolder();
	}


    public Task(DockingProperties properties, Vector3 vector, int i, Parameters parameters)
	{
	    this.parameters = parameters;
		this.num = i;
		this.properties = properties;
		this.vector = vector;
		this.gpf = createGPF();
		this.local_task_dir = this.parameters.getPathToWorkspace() + File.separator + getNameTaskFolder();
	}



    private Path createGPF()
	{
		try
		{
			Path p = new Path(properties.getStartGPFPath());
			Path task_path = new Path(p.getParent().toString() + File.separator +getNameTaskFolder());
			parameters.getHDFS().mkdirs(task_path);
			Path new_gpf_path = new Path(task_path.toString() + File.separator+"task" + num + ".gpf");
			parameters.getHDFS().createNewFile(new_gpf_path);

			return new_gpf_path;
		}
		catch(IOException | GPFError f)
		{
			f.printStackTrace();
			launchable = false;
			return null;
		}
	}

	/**
	 * Main method. Launch all needed scripts
	 * @return path to dlg file in hdfs system
	 */
	@Override
	public String launch() {
		String path = "";
		String messageToServer = "";
		try
		{
			if(launchable)
			{
				//send to server(name node) all steps of work.
				messageToServer = "Reduce task\n" + properties.toString() + "\n"
						+ vector.toString() + "\n"
						+ "num = " + num + "\n"
						+ "Launch Task:" + properties.getId() + "_" + num + "\n"
						+ "Start GPF = " + properties.getStartGPFPath() + "\n"
						+ "Current GPF = " + gpf.toString() + "\n"
						+ "Local path = " + local_task_dir + "\n";

				Runtime rt = Runtime.getRuntime();
				//copy files to special dir in  hdfs
				copyNeededFiles();
				//copy this dir to local node
				copyToLocal();
				processingFile(local_task_dir + File.separator + properties.getStartGPF(), local_task_dir + File.separator + gpf.getName(), new String[]{"gridfld", "receptor", "map", "elecmap", "dsolvmap"}, local_task_dir, parameters.getLocal());

				int res_preparing = 0;
				String command = "";

				if (parameters.getArgs()[7].contentEquals("prepare_gpf"))
				{
					res_preparing = prepareGPF();
					messageToServer += formCommandLineForGPF() + "\n" + formCommandLineForConvertGPF() +"\n";
				}
				else
				{
					//if don`t needed prepare_gpf copy exiting gpf file to current4.gpf
					java.nio.file.Path src = Paths.get(local_task_dir + gpf.getName());
					java.nio.file.Path dst = Paths.get(local_task_dir + "current4.gpf");
					Files.createFile(Paths.get(local_task_dir + "current4.gpf"));
					Files.copy(src,dst);
				}
				if (res_preparing == 0)
				{
					processingFile(local_task_dir + File.separator + "current4.gpf", local_task_dir + File.separator + "current4_processed.gpf", new String[]{"gridfld", "receptor", "map", "elecmap", "dsolvmap"}, local_task_dir, parameters.getLocal());
					command = formCommandLineForGrid();
					messageToServer += command + "\n";
					final Process grid = rt.exec(command);//launch autogrid
					if (grid.waitFor() == 0)
					{
						command = formCommandLineForDPF();
						messageToServer += command + "\n";
						final Process dpf = rt.exec(command);//launch prepare_dpf
						if (dpf.waitFor() == 0)
						{
							processingFile(local_task_dir + File.separator + "docking.dpf", local_task_dir + File.separator + "docking_processed.dpf"
										, new String[]{"fld", "move", "map", "elecmap", "desolvmap"}, local_task_dir, parameters.getLocal());
							command = formCommandLineForDock();
							messageToServer += command + "\n";
							final Process dock = rt.exec(command);//launch autodock
							if (dock.waitFor() == 0)
							{
								FileUtils.copy(parameters.getLocal(), local_task_dir + File.separator + getNameTaskFolder() + "_result.dlg"
											, parameters.getHDFS(), gpf.getParent().toString());//copy result to hdfs
								path = gpf.getParent().toString() + "/" + getNameTaskFolder() + "_result.dlg";
								messageToServer += path + "\n";
							} else throw new TaskException("Don`t started docking.Error code =  " + dock.exitValue() + ".Id Task = " + properties.getId());
						} else throw new TaskException("Can`t make DPF.Error code = " + dpf.exitValue() + ".Id Task = " + properties.getId());
					} else throw new TaskException("Can`t make electron maps.Error code = " + grid.exitValue() + ".Id Task = " + properties.getId());
				} else throw new TaskException("Can`t prepare gpf.Id Task = " + properties.getId());
			}else throw new TaskException("Can`t launch task.Id Task = " + properties.getId());
		}
		catch (Exception  e)
		{
			e.printStackTrace();
			messageToServer += e.getMessage() + "\n";
		}
		finally
		{
			// delete all intermediate file. Only dlg file should exist.
			Path local = new Path(local_task_dir + File.separator + getNameTaskFolder());
			Path hdfs = new Path(gpf.getParent().toString());

			try {
				if (FileUtils.exist(local,parameters.getLocal()))
					dispose(local,parameters.getLocal());
				if (FileUtils.exist(hdfs,parameters.getHDFS()))
					dispose(hdfs,parameters.getHDFS());

			}
			catch (IOException e)
			{
				messageToServer += e.getMessage() + "\n";
			}
				messageToServer += local_task_dir + File.separator + getNameTaskFolder() + "_result.dlg";
				client.addMessage(messageToServer);
				if (!path.isEmpty() & isSuccessfulDLG(local_task_dir + File.separator+getNameTaskFolder() + "_result.dlg"))
					successful = true;
				else
					successful = false;
		}
		return path;
	}

	private boolean isSuccessfulDLG(String s) {
		try{
			BufferedReader reader = new BufferedReader(new FileReader(s));
			ArrayList<String> file = new ArrayList<>();
			String tmp;
			while ((tmp = reader.readLine()) != null)
				file.add(tmp);
			for (int i = file.size() - 1;i >= 0;i--)
			{
				if (file.get(i).contains("autodock4: Successful Completion on"))
					return true;
				if (i < file.size() - 20) break;
			}
			return false;
		}  catch (Exception e) {
			return false;
		}
	}

	/**
	 * Prepare GPF file from reference gpf and convert to gpf4
	 * */
	private int prepareGPF() throws IOException,InterruptedException
	{
		Process prepare_gpf,convert_gpf;
		Runtime rt = Runtime.getRuntime();
		String command = formCommandLineForGPF();
		prepare_gpf = rt.exec(command);//launch prepare_gpf
		if (prepare_gpf.waitFor() == 0) {
				command = formCommandLineForConvertGPF();
				parameters.getLog().log(Level.INFO, command);
				convert_gpf = rt.exec(command);//launch gpf3_to_gpf4
				return convert_gpf.waitFor();
		}
		else
			return prepare_gpf.waitFor();
	}

	private void dispose(Path path,FileSystem sys)
	{
		try
		{
			FileUtils.deleteFolder(path,sys);
		}
		catch(TaskException | IOException e)
		{
			e.printStackTrace();
			parameters.getLog().log(Level.WARNING,e.getMessage());
		}
	}
	private String formCommandLineForConvertGPF() {


		return "python "  + parameters.getPathToMGLTools()+ File.separator+
				"Lib"+File.separator+"site-packages"+File.separator+"AutoDockTools"+File.separator+"Utilities24"+File.separator+"gpf3_to_gpf4.py" +
				" -s " + local_task_dir +File.separator+ "current"+
				" -l " + local_task_dir + File.separator + properties.getLigand()
				+ " -r " + local_task_dir + File.separator + properties.getReceptor()
				+ " -o " + local_task_dir +File.separator+ "current4.gpf";
	}

	private String formCommandLineForGPF() {
		return "python "  + parameters.getPathToMGLTools()+ File.separator+
				"Lib"+File.separator+"site-packages"+File.separator+"AutoDockTools"+File.separator+"Utilities24"+File.separator+"prepare_gpf.py" +
				" -i " + local_task_dir + File.separator + gpf.getName()+
				" -l " + local_task_dir + File.separator + properties.getLigand()
				+ " -r " + local_task_dir + File.separator + properties.getReceptor()
				+ " -o " + local_task_dir +File.separator+ "current.gpf";
	}

	/**
	 * Изменение в gpf файле имени файлов,т.е указание полного метоположения,где они лежат или должны лежать.
	 * @param line - Строка из файла
	 * @param dir - Местоположение файлов
	 * @param signature - Слова,после которых надлежит вставлять путь dir
	 * @return Отредактированная строка
	 * */
	private String editNameFiles(String line,String dir,String[] signature)
	{
		StringBuilder b = new StringBuilder(line);
		String[] components = line.split(" ");
		for (String s:signature)
		{
			if(components[0].contentEquals(s))
				return b.insert(s.length()+1,dir + File.separator).append("\n").toString();
		}
		return b.append("\n").toString();
	}
	/**Копирует в рабочую папку данной задачи файлы рецептора,его гибкой части(если есть) и лиганда*/
	private void copyNeededFiles() throws IOException
	{
		FileUtils.copy(properties.getReceptorPath(), gpf.getParent().toString() + "/" + properties.getReceptor(), parameters.getHDFS());

		FileUtils.copy(properties.getLigandPath(), gpf.getParent().toString() + "/" + properties.getLigand(), parameters.getHDFS());

		FileUtils.copy(properties.getStartGPFPath(),gpf.getParent().toString() + "/" + properties.getStartGPF(),parameters.getHDFS());

		if(!properties.getReceptorFlexiblePart().equals("")) {
			FileUtils.copy(properties.getReceptorFlexiblePartPath(), gpf.getParent().toString() + "/" + properties.getReceptorFlexiblePart(), parameters.getHDFS());

		}
	}
	/**Копирует необходимые файлы в локальную систему. Необходимо для докинга.
	 * @see Task#copyNeededFiles() */
	private void copyToLocal() throws IOException
	{
		FileStatus[] fileStatuses = parameters.getHDFS().listStatus(gpf.getParent());
		Path local_dir = new Path(parameters.getPathToWorkspace() + File.separator + getNameTaskFolder());
		parameters.getLocal().mkdirs(local_dir);
		for(FileStatus status: fileStatuses) {
			FileUtils.copy(parameters.getHDFS(), status.getPath().toString(), parameters.getLocal(), local_dir.toString());
		}
	}
	/**Редактирвание файлов GPF и DPF. Прописывание полного пути к файлам,а в GPF - ещё и подстановка нужного вектора*/
	private void processingFile(String in, String out, String[] signature, String path, FileSystem sys) throws IOException
	{
		String line;
		final String seeking = "gridcenter";
		BufferedReader br = FileUtils.readFile(in,sys);
		BufferedWriter bw = FileUtils.writeFile(out,sys);

		while ((line = br.readLine()) != null)
		{
			if (line.contains(seeking))
				bw.write(seeking+" " + vector.getX() + " " + vector.getY() + " " + vector.getZ() + "\n");
			else
				bw.write(editNameFiles(line, path, signature));
		}
		bw.close();
		br.close();
	}

	private String formCommandLineForDock(){


		return (/*parameters.getPathToWorkspace() +File.separator+"autodock4.exe*/"autodock4 -p "+ local_task_dir +File.separator+ "docking_processed.dpf"
				+ " -l " +  local_task_dir +File.separator + getNameTaskFolder() + "_result.dlg")/*.split("\\s+")*/;
	}

	private String formCommandLineForGrid() {
		return (/*parameters.getPathToWorkspace() +File.separator+"autogrid4.exe*/"autogrid4 -p "+ local_task_dir + File.separator + "current4_processed.gpf"
				+ " -l " +  local_task_dir+File.separator+"current_grid.glg");
	}

	private String formCommandLineForDPF() {
		String command;
		String optional =  !(properties.getOptionalString().equals("-"))? properties.getOptionalString():"";
		if (properties.getReceptorFlexiblePart().equals("")) {
			command = (/*parameters.getPathToMGLTools() + File.separator+*/"python "  + parameters.getPathToMGLTools()+ File.separator+
					"Lib"+File.separator+"site-packages"+File.separator+"AutoDockTools"+File.separator+"Utilities24"+File.separator+"prepare_dpf4.py -l "
					+ local_task_dir + File.separator + properties.getLigand()
					+ " -r " + local_task_dir + File.separator + properties.getReceptor()
					+ " -o " + local_task_dir +File.separator+ "docking.dpf " + optional);
		}
		else
		{
			command = (/*parameters.getPathToMGLTools()  + File.separator+*/ "python "  + parameters.getPathToMGLTools() + File.separator
					+ "Lib"+File.separator+"site-packages"+File.separator+"AutoDockTools"+File.separator+"Utilities24"+File.separator+"prepare_dpf4.py -l "
					+ local_task_dir + File.separator + properties.getLigand()
					+ " -r " +local_task_dir + File.separator + properties.getReceptor()
					+ " -x " +local_task_dir + File.separator + properties.getReceptorFlexiblePart()+
					" -o " + local_task_dir +File.separator+ "docking.dpf " + optional);
		}
		return command;
	}
	/**
	 * @return the vector - центр грида
	 */
	public Vector3 getVector()
	{
		return vector;
	}
	/**
	 * @return имя папки,в которой будут находится необходимые для докинга данные
	 * Прим. возращает именно имя папки,а не путь к файлам
	 **/
	public String getNameTaskFolder()
	{
		return properties.getId() + "_" + num;
	}
/**
 * Must be called before use class**/
	public void setParameters(Parameters parameters) {
		this.parameters = parameters;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException
	{
		String path = arg0.readUTF();
		gpf = new Path(path);
		properties = new DockingProperties();
		properties.readFields(arg0);
		num = arg0.readInt();
		launchable = arg0.readBoolean();
		vector = new Vector3(0,0,0);
		vector.readFields(arg0);
		local_task_dir = arg0.readUTF();
		successful = arg0.readBoolean();
	}

	@Override
	public void write(DataOutput arg0) throws IOException
	{
		String path = gpf.toString();
		arg0.writeUTF(path);
		properties.write(arg0);
		arg0.writeInt(num);
		arg0.writeBoolean(launchable);
		vector.write(arg0);
		arg0.writeUTF(local_task_dir);
		arg0.writeBoolean(successful);

	}

	@Override
	public int compareTo(Task o) {
		if(!properties.getId().contentEquals(o.properties.getId()))
			return 0;
		else
			return (num > o.num) ? 1 : -1;
	}
}