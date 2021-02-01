package ru.smirnygatotoshka.docking;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * Класс, содержащий информацию о выполнении докинга. Служит результатом Map и ключом Reduce.
 * На этапе Map - получение идентификатора задачи и пути к файлу содержащий результаты
 * На этапе Reduce - определяется успешность выполнения и счиывается минимальная энергия.
 *
 * @author SmirnygaTotoshka
 */
public class DockResult {
    private FileSystem hdfs;

    public String getId() {
        return id;
    }

    private DockResult(String node, String id, String pathDLGinHDFS, String target, String ligand, String flexiblePart, String runs, String energy, String rmsd, String numHBonds, String HBonds, boolean success, String causeFail) {
        this.node = node;
        this.id = id;
        this.pathDLGinHDFS = pathDLGinHDFS;
        this.target = target;
        this.ligand = ligand;
        this.flexiblePart = flexiblePart;
        this.runs = runs;
        this.energy = energy;
        this.rmsd = rmsd;
        this.numHBonds = numHBonds;
        this.HBonds = HBonds;
        this.success = success;
        this.causeFail = causeFail;
    }

    private String node;
    private String id;
    private String pathDLGinHDFS;
    private String target;
    private String ligand;
    private String flexiblePart;
    private String runs;
    private String energy;
    private String rmsd;
    private String numHBonds;
    private String HBonds;
    private boolean success;
    private String causeFail;
    //Из идентификатора для мап-таск(метод мап - 1 параметр)
    private LongWritable key;


    public DockResult(String id, String pathToHDFS, LongWritable key, JobConf conf) throws IOException {
        this.hdfs = FileSystem.get(conf);
        this.id = id;
        this.runs = "10";
        this.rmsd = "0";
        this.numHBonds = "0";
        this.HBonds = "";
        this.pathDLGinHDFS = pathToHDFS + "/" + id + ".dlg";
        String[] parts = id.split("_");
        this.target = parts[0];
        this.ligand = parts[1];
        this.flexiblePart = parts[2];
        this.key = key;
        this.energy = "-1000.001";
        this.success = false;
        this.causeFail = "Unknown status";
        try {
            this.node = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            this.node = "Unknown";
        }
    }

    public void fail(String cause) {
        if (hasSuccessDLGinHDFS()){
            success = true;
            causeFail = "Ложно-отрицательный. Есть DLG, но не удалось считать данные.";
            energy = "999.999";
        }
        else{
            success = false;
            energy = "-100000";//Warning dont use static constant
            causeFail = cause;
            pathDLGinHDFS = "None";
        }
    }

    public void success(String pathToResult,FileSystem system) throws IOException, TaskException {
        if (!hasSuccessDLGinHDFS()){
            fail("Ложно-положительный. Отсутствует DLG.");
        }
        else {
            success = true;
            causeFail = "Not Fail";
            ArrayList<String> lines = FileUtils.readFile(pathToResult, system);
            try {
                String[] data = lines.get(1).split(",");
                runs = data[1];
                energy = data[4];
                rmsd = data[5];
                HBonds = data[6];
                numHBonds = data[7];
            } catch (Exception | Error e) {
                throw new TaskException("TaskException: Cant parse success, cause " + e.getMessage());
            }
        }
    }

    public boolean hasSuccessDLGinHDFS() {
        try {
            ArrayList<String> lines = FileUtils.readFile(pathDLGinHDFS, hdfs);
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

    @Override
    public String toString() {
        String path = success ? pathDLGinHDFS : "None";
        return getTime() + "\t" + node + "\t" + id + "\t" + success + "\t" +
                causeFail + "\t" + path + "\t" + target + "\t" + ligand + "\t" +flexiblePart + "\t" + energy + "\t" + rmsd + "\t" +
                numHBonds + "\t" +HBonds + "\t" + runs;
    }

    public void setHdfs(FileSystem hdfs) {
        this.hdfs = hdfs;
    }

    public static DockResult fromString(String info, ClusterProperties cluster) throws IOException {
        String[] comp = info.split("\t");
        DockResult result = new DockResult(comp[1],comp[2],comp[5],comp[6],comp[7],
                comp[8],comp[13].trim(),comp[9],comp[10],comp[11],comp[12],Boolean.parseBoolean(comp[3]),comp[4]);
        result.setHdfs(FileSystem.get(cluster.getJobConf()));
        return result;
    }
    /**
     * Вернуть конечное значение редьюсера
     */
    public Text getText() {
        return new Text(this.toString());
    }


    private String getTime(){
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
        Date start = new Date(System.currentTimeMillis());

        return format.format(start);
    }

    public String getPathDLGinHDFS() {
        return pathDLGinHDFS;
    }

    public boolean isSuccess() {
        return success;
    }

    public LongWritable getKey() {
        return key;
    }

}