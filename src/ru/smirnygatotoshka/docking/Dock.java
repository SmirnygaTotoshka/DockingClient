
package ru.smirnygatotoshka.docking;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author SmirnygaTotoshka
 * TODO - не предназначен для запуска на windows
 *
 */
public class Dock {

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

	/**
	 * Парсит строку из файла с описанием задач
	 */
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
	}

	public Dock() {
		//for tests TODO - delete
	}
//TODO - make private
	public DockingProperties splitProperties(Text line){
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
			default:
				cmd = "";
		}
		msg.add(cmd + "\n");
		return cmd;
	}

	/**
	 * @return the dockingProperties
	 */
	public DockingProperties getDockingProperties() {
		return dockingProperties;
	}

	/**
	 * start on local node pipe of scripts
	 * 1) prepare_gpf4.py
	 * 2) autogrid
	 * 3) prepare_dpf42.py
	 * 4) autodock
	 *
	 * @return path to DLF file in hdfs
	 */
	public DockResult launch() {
		try {
			dockResult = new DockResult(dockingProperties.getId(), dockingProperties.getPathToFiles(), key);
			if (errorMessage.isEmpty()) {
				if (isSuccessPrepareGpf()) {
					if (isSuccessConvertGpf()) {
						processingFile(getGPFLocalPath(), GPF_signature);
						if (isSuccessAutogrid()) {
							if (isSuccessPrepareDpf()) {
								processingFile(getDPFLocalPath(), DPF_signature);
								if (isFinihedAutodock()) {
									FileUtils.copy(local, getDLGLocalPath(), hdfs, dockResult.getPathDLGinHDFS());
								} else throw new TaskException("Неудача на этапе Autodock");
							} else throw new TaskException("Неудача на этапе подготовке DPF");
						} else throw new TaskException("Неудача на этапе Autogrid");
					} else throw new TaskException("Неудача на этапе конвертации GPF");
				} else throw new TaskException("Неудача на этапе подготовке GPF");
			} else throw new TaskException(errorMessage);
		}
		catch (TaskException e) {
			dockResult.fail("TaskException:" + e.getMessage());
			msg.add("TaskException:" + e.getMessage());
		}
		catch (IOException e){
			dockResult.fail("IOException: " + e.getMessage());
			msg.add("IOException: " + e.getMessage());
		}
		finally {
			try {
				Path wd = new Path(localDir);
				if (FileUtils.exist(wd,local))
					FileUtils.deleteFolder(wd, local);
			}
			catch (IOException | TaskException e) {
				String s = "Не удалось высвободить ресурсы для " + dockingProperties.getId() + "\n" + e.getMessage();
				msg.add(s);
				msg.add(dockingProperties.toString());

            }
			if (!errorMessage.isEmpty()) {
				msg.add(dockingProperties.toString());
				msg.add(errorMessage);
            }
			log.writeRecord(msg);
			try {
				log.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return dockResult;
		}
	}

	private void copyToLocal() throws IOException {
		if (!local.exists(new Path(localDir)))
			local.mkdirs(new Path(localDir));
		FileUtils.copy(hdfs, dockingProperties.getReceptorPath(), local, getReceptorLocalPath());
		FileUtils.copy(hdfs, dockingProperties.getLigandPath(), local, getLigandLocalPath());
		if (!dockingProperties.getReceptorFlexiblePart().isEmpty())
			FileUtils.copy(hdfs, dockingProperties.getReceptorFlexiblePartPath(), local, getReceptorFlexiblePartLocalPath());

	}

	private boolean isFinihedAutodock() {
		int code = launchCommand(formCommand(Pipeline.AUTODOCK));
		return code == 0;
	}

	private boolean isSuccessPrepareDpf() {
		int code = launchCommand(formCommand(Pipeline.PREPARE_DPF));
		if (code != 0)
			return false;
		else {
			try {
				ArrayList<String> lines = FileUtils.readFile(getDPFLocalPath(), local);
				if (lines.size() == 0)
					return false;
				return true;
			} catch (IOException e) {
				return false;
			}
		}
	}

	private boolean isSuccessAutogrid() {
		int code = launchCommand(formCommand(Pipeline.AUTOGRID));
		if (code != 0)
			return false;
		else {
			try {
				ArrayList<String> lines = FileUtils.readFile(getGLGLocalPath(), local);
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
	}

	private boolean isSuccessPrepareGpf() {
		int code = launchCommand(formCommand(Pipeline.PREPARE_GPF));
		if (code != 0)
			return false;
		else {
			try {
				ArrayList<String> lines = FileUtils.readFile(getGPFLocalPath(), local);
				if (lines.size() == 0)
					return false;
				return true;
			} catch (IOException e) {
				return false;
			}
		}
	}
	private boolean isSuccessConvertGpf() {
		int code = launchCommand(formCommand(Pipeline.CONVERT_GPF));
		if (code != 0)
			return false;
		else {
			try {
				ArrayList<String> lines = FileUtils.readFile(getGPFLocalPath(), local);
				if (lines.size() == 0)
					return false;
				return true;
			} catch (IOException e) {
				return false;
			}
		}
	}
	/*private boolean hasSignature(ArrayList<String> lines, String[] signature) {
		boolean f = true;
		for (String s : signature) {
			f = false;
			for (String l : lines) {
				if (l.contains(s)) {
					f = true;
					break;
				}
			}
			if (!f) break;
		}
		return f;
	}*/

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

	private enum Pipeline {
		PREPARE_GPF,
		CONVERT_GPF,//TODO
		AUTOGRID,
		PREPARE_DPF,
		AUTODOCK
	}
}
