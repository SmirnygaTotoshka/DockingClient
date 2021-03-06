
package ru.smirnygatotoshka.docking;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * Класс, содержащий основную логику приложения.
 * Запуск необходимых скриптов для докинга в Autodock.
 * Последовательность описана в
 * @see Pipeline
 * Требования: Ubuntu, на всех узлах должен быть установлен autodock4,autogrid4, должна быть папка с MGLTools
 * @author SmirnygaTotoshka
 * Не предназначен для запуска на windows
 */
public class Dock {

	private enum Pipeline {
		PREPARE_GPF,
		CONVERT_GPF,
		AUTOGRID,
		PREPARE_DPF,
		AUTODOCK,
		ANALYZE
	}

	private final String[] GPF_signature = new String[]{"gridfld", "receptor", "map", "elecmap", "dsolvmap"};
	private final String[] DPF_signature = new String[]{"fld", "move", "map", "elecmap", "desolvmap"};
	private DockResult dockResult;
	private LongWritable key;

	private DockingProperties dockingProperties;
	private ClusterProperties clusterProperties;
	private Runtime runtime = Runtime.getRuntime();
	private String errorMessage;
	private ArrayList<String> msg;//if haven`t errors - empty string;
	private String localSep = File.separator;
	private String localDir;
	private FileSystem local, hdfs;
	private Log log;


	public Dock(Text line, ClusterProperties clusterProp, LongWritable key) {

		this.dockingProperties = splitProperties(line);
		this.key = key;
		this.clusterProperties = clusterProp;
		this.errorMessage = "";
		this.localDir = clusterProperties.getWorkspaceLocalDir() + localSep + dockingProperties.getId();
		this.msg = new ArrayList<>();
		try {
			this.local = FileSystem.getLocal(clusterProperties.getJobConf());
			this.hdfs = FileSystem.get(clusterProperties.getJobConf());
			dockResult = new DockResult(dockingProperties.getId(), dockingProperties.getPathToFiles(), key, clusterProperties.getJobConf());
		}
		catch (IOException e) {
			this.errorMessage = "Ошибка при получения доступа к файловым системам:" + e.getMessage();
		}
		try {
			copyToLocal();
		} catch (IOException e) {
			errorMessage = "Ошибка при копировании файла "+e.getMessage();
		}
		try {
			this.log = new Log(clusterProperties.getWorkspaceLocalDir() + localSep + dockingProperties.getId() + ".log");
		} catch (IOException e) {
			errorMessage = "Ошибка при создании лога "+e.getMessage();
		}
		if (dockResult.hasSuccessDLGinHDFS())
			errorMessage = "Уже имеется посчитанные результаты для этой пары.";
	}

	public Dock() {
		//for tests
	}
	/**
	 * Парсит строку из файла с описанием задач
	 */
	private DockingProperties splitProperties(Text line){
		String[] description = line.toString().trim().split(";");

		String path = description[0];
		String r = description[1];
		String rFlex = description[2];
		String lig = description[3];
		String gpfName = description[4];
		String gpfParam = description[5];
		String dpfName = description[6];
		String dpfParam = description[7];

		return new DockingProperties(path, r, rFlex, lig, gpfName, gpfParam, dpfName, dpfParam);
	}

	/**
	 * start on local node pipe of scripts
	 * 1) prepare_gpf4.py
	 * 2) gpf3_to_gpf4.py
	 * 2) autogrid4
	 * 3) prepare_dpf42.py
	 * 4) autodock4
	 *
	 * @return path to DLF file in hdfs
	 */
	public DockResult launch() {
		try {
			if (!hasTrouble()) {
				if (isSuccessLaunchCommand(Pipeline.PREPARE_GPF,getGPFLocalPath())) {
					if (isSuccessLaunchCommand(Pipeline.CONVERT_GPF, getGPFLocalPath())) {
						processingFile(getGPFLocalPath(), GPF_signature);
						if (isSuccessLaunchCommand(Pipeline.AUTOGRID, getGLGLocalPath())) {
							if (isSuccessLaunchCommand(Pipeline.PREPARE_DPF, getDPFLocalPath())) {
								processingFile(getDPFLocalPath(), DPF_signature);
								if (isSuccessLaunchCommand(Pipeline.AUTODOCK,getDLGLocalPath())) {
									FileUtils.copy(local, getDLGLocalPath(), hdfs, dockResult.getPathDLGinHDFS());
									if (isSuccessLaunchCommand(Pipeline.ANALYZE, getAnalyzeOutLocalPath())){
										dockResult.success(getAnalyzeOutLocalPath(), local);
										msg.add("SUCCESS\n");
									} else throw new TaskException("Не удалось проанализировать.");
								} else throw new TaskException("Неудача на этапе Autodock");
							} else throw new TaskException("Неудача на этапе подготовке DPF");
						} else throw new TaskException("Неудача на этапе Autogrid");
					} else throw new TaskException("Неудача на этапе конвертации GPF");
				} else throw new TaskException("Неудача на этапе подготовке GPF");
			} else throw new TaskException(getTrouble());
		}
		catch (TaskException e) {
			dockResult.fail("TaskException:" + e.getMessage());
			msg.add("TaskException:" + e.getMessage());
		}
		catch (IOException e){
			dockResult.fail("IOException: " + e.getMessage());
			msg.add("IOException: " + e.getMessage());
		}
		catch (Error | Exception e){
			dockResult.fail("Strange things: " + e.getMessage());
			msg.add("Strange things: " + e.getMessage());
		}
		finally {
			dispose();
			return dockResult;
		}
	}

	public void dispose(){
		try {
			Path wd = new Path(localDir);
			if (FileUtils.exist(wd,local))
				FileUtils.deleteFolder(wd, local);

			if (hasTrouble()) {
				msg.add(dockingProperties.toString());
				msg.add(errorMessage);
			}
			log.writeRecord(msg);
			log.close();
		}
		catch (IOException | TaskException e) {
			String s = "Не удалось высвободить ресурсы для " + dockingProperties.getId() + "\n" + e.getMessage();
			System.out.println(s);
			e.printStackTrace();
		}
	}
	public boolean hasTrouble(){
		return !errorMessage.isEmpty();
	}
	public String getTrouble(){
		return getTime() + "\t" + errorMessage + "\t" + dockingProperties.getId();
	}
	/**Формирует текст команды для запуска в командной строке*/
	private String formCommand(Pipeline action) {
		String cmd;
		String flex = dockingProperties.getReceptorFlexiblePart().isEmpty() ? "" :
				" -x " + getReceptorFlexiblePartLocalPath();
		switch (action) {
			case PREPARE_GPF:
				if (localSep.contentEquals("/"))
					cmd = "python " + getScriptsPath() +
							"prepare_gpf.py" +
							" -l " + getLigandLocalPath() +
							" -r " + getReceptorLocalPath() + flex +
							" -o " + getGPFLocalPath() + " " +
							dockingProperties.getGpfParameters() +
							" -v";
				else
					cmd = clusterProperties.getPathToMGLTools() + localSep + "python " + getScriptsPath() +
						"prepare_gpf.py" +
						" -l " + getLigandLocalPath() +
						" -r " + getReceptorLocalPath() + flex +
						" -o " + getGPFLocalPath() + " " +
						dockingProperties.getGpfParameters() +
						" -v";
				break;
			case CONVERT_GPF:
					cmd = "python " + getScriptsPath() +
						"gpf3_to_gpf4.py" +
						" -s " + getGPFLocalPath().substring(0, getGPFLocalPath().indexOf('.')) +
						" -l " + getLigandLocalPath() +
						" -r " + getReceptorLocalPath() +
						" -o " + getGPFLocalPath() +
						" -v";
				break;
			case AUTOGRID:
				if (localSep.contentEquals("/"))// it mean node on Unix OS
					cmd = "autogrid4 -p " + getGPFLocalPath() + " -l " + getGLGLocalPath();
				else
					//on Win we start exe. it has to be in localDir
					cmd = "autogrid4.exe -p " + getGPFLocalPath() + " -l " + getGLGLocalPath();
				break;
			case PREPARE_DPF:
				if (localSep.contentEquals("/"))
					cmd = "python " + getScriptsPath() +
							"prepare_dpf4.py" +
							" -l " + getLigandLocalPath() +
							" -r " + getReceptorLocalPath() + flex +
							" -o " + getDPFLocalPath() + " " +
							dockingProperties.getDpfParameters() +
							" -v";
				else
					cmd = clusterProperties.getPathToMGLTools() + localSep + "python " + getScriptsPath() +
						"prepare_dpf4.py" +
						" -l " + getLigandLocalPath() +
						" -r " + getReceptorLocalPath() + flex +
						" -o " + getDPFLocalPath() + " " +
						dockingProperties.getDpfParameters() +
						" -v";
				break;
			case AUTODOCK:
				if (localSep.contentEquals("/"))// it mean node on Unix OS
					cmd = "autodock4 -p " + getDPFLocalPath() + " -l " + getDLGLocalPath();
				else
					//on Win we start exe. it has to be in localDir
					cmd = "autodock4.exe -p " + getDPFLocalPath() + " -l " + getDLGLocalPath();
				break;
			case ANALYZE:
				cmd = "python " + getScriptsPath() +
						"summarize_docking.py" +
						" -l " + getDLGLocalPath() +
						" -r " + getReceptorLocalPath() +
						" -o " + getAnalyzeOutLocalPath() +
						" -v -b -k";

				break;
			default:
				cmd = "";
		}
		msg.add(cmd + "\n");
		return cmd;
	}

	/**
	 * Копирует необходимые файлы из папки в hdfs в локальную рабочую папку
	 * */
	private void copyToLocal() throws IOException {
		if (!local.exists(new Path(localDir)))
			local.mkdirs(new Path(localDir));
		FileUtils.copy(hdfs, dockingProperties.getReceptorPath(), local, getReceptorLocalPath());
		FileUtils.copy(hdfs, dockingProperties.getLigandPath(), local, getLigandLocalPath());
		if (!dockingProperties.getReceptorFlexiblePart().isEmpty())
			FileUtils.copy(hdfs, dockingProperties.getReceptorFlexiblePartPath(), local, getReceptorFlexiblePartLocalPath());

	}

	/**Запускает скрипты и проверяет успешность их запуска.*/
	private boolean isSuccessLaunchCommand(Pipeline cmd, String pathToCheck){
		int code = launchCommand(formCommand(cmd));
		if (code != 0)
			return false;
		else {
			try {
				ArrayList<String> lines = FileUtils.readFile(pathToCheck, local);
				if (cmd == Pipeline.AUTOGRID || cmd == Pipeline.AUTODOCK) {
					for (int i = lines.size() - 1; i > lines.size() - 21; i--)
						if (lines.get(i).contains("Successful Completion"))
							return true;
					return false;
				}
				if (lines.size() <= 1)
					return false;
				return true;
			} catch (IOException e) {
				return false;
			}
		}
	}
	/**
	 * Запускает команду в командной строке
	 * @param cmd  - текст команды
	 * @return код выполнения
	 * */
	private int launchCommand(String cmd) {
		try {
			Process process = runtime.exec(cmd);
			log.redirect(process.getInputStream());
			return process.waitFor();
		}
		catch (InterruptedException | IOException e) {
			this.errorMessage = e.getMessage();
			return -1001;
		}
	}

	/**
	 * Редактирвание файлов GPF и DPF. Прописывание полного пути к файлам.
	 *
	 * @param file      Путь к изменяемому gpf/dpf файлу
	 * @param signature Названия изменяемых параметров
	 */
	private void processingFile(String file, String[] signature) throws IOException {
		ArrayList<String> lines = FileUtils.readFile(file, local);
		ArrayList<String> newLines = new ArrayList<>();
		for (String l : lines)
			newLines.add(editNameFiles(l, localDir, signature));
		FileUtils.writeFile(newLines, file, local);
	}

	/**
	 * Изменение в gpf,dpf файле имени файлов,т.е указание полного метоположения,где они лежат или должны лежать.
	 *
	 * @param line      - Строка из файла
	 * @param dir       - Местоположение файлов
	 * @param signature - Слова,после которых надлежит вставлять путь dir
	 * @return Отредактированная строка
	 */
	private String editNameFiles(String line, String dir, String[] signature) {
		StringBuilder b = new StringBuilder(line);
		String[] components = line.split(" ");
		for (String s : signature) {
			if (components[0].contentEquals(s))
				return b.insert(s.length() + 1, dir + File.separator).append("\n").toString();
		}
		return b.append("\n").toString();
	}

	/**
	 * @return the dockingProperties
	 */
	public DockingProperties getDockingProperties() {
		return dockingProperties;
	}


	private String getTime(){
		SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
		Date start = new Date(System.currentTimeMillis());
		return format.format(start);
	}

	private String getDLGLocalPath() {
		return localDir + localSep + dockingProperties.getId() + ".dlg";
	}

	private String getGLGLocalPath() {
		return localDir + localSep + "resAutogrid.glg";
	}

	public String getScriptsPath() {
		return clusterProperties.getPathToMGLTools() +
				localSep + "Lib" + localSep + "site-packages" + localSep + "AutoDockTools" + localSep + "Utilities24" + localSep;
	}

	public String getReceptorLocalPath() {
		return localDir + localSep + dockingProperties.getReceptor();
	}
	public String getAnalyzeOutLocalPath(){
		return localDir + localSep + "result.txt";
	}
	public String getGPFLocalPath() {
		return localDir + localSep + dockingProperties.getGpfName();
	}

	public String getDPFLocalPath() {
		return localDir + localSep + dockingProperties.getDpfName();
	}

	/**
	 * @return путь к файлу лиганда в локальной системе
	 */
	public String getLigandLocalPath() {
		return localDir + localSep + dockingProperties.getLigand();
	}

	/**
	 * @return путь к файлу гибкой части рецептора в локальной системе
	 */
	public String getReceptorFlexiblePartLocalPath() {
		if (dockingProperties.getReceptorFlexiblePart().contentEquals(""))
			return "";
		return localDir + localSep + dockingProperties.getReceptorFlexiblePart();
	}

}
