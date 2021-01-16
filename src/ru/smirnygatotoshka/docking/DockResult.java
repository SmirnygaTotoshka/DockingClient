package ru.smirnygatotoshka.docking;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

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
    private String node;

    public DockResult(String id, String pathToHDFS, LongWritable key) {
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
        success = false;
        energy = "-100000";//Warning dont use static constant
        causeFail = cause;
        pathDLGinHDFS = "None";
    }

    public void success(String pathToResult,FileSystem system) throws IOException, TaskException {
        success = true;
        causeFail = "Not Fail";
        ArrayList<String> lines = FileUtils.readFile(pathToResult,system);
        try {
            String[] data = lines.get(1).split(",");
            runs = data[1];
            energy = data[4];
            rmsd = data[5];
            HBonds = data[6];
            numHBonds = data[7];
        }
        catch (Exception | Error e){
            throw new TaskException("TaskException: Cant parse success, cause " + e.getMessage());
        }
    }

    public boolean hasSuccessDLG(FileSystem sys) {
        try {
            ArrayList<String> lines = FileUtils.readFile(pathDLGinHDFS, sys);
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
                numHBonds + "\t" +HBonds + "\t" +runs;
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

    public String getNode() {
        return node;
    }

}