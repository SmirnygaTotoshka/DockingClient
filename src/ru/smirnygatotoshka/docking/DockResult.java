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
public class DockResult /*implements WritableComparable<LongWritable> */{
    private String id;
    private String pathDLGinHDFS;
    private String target;
    private String ligand;
    private String flexiblePart;
    private int runs;
    private float energy;
    private float rmsd;
    private int numHBonds;
    private String HBonds;
    private boolean success;
    private String causeFail;
    //Из идентификатора для мап-таск(метод мап - 1 параметр)
    private LongWritable key;
    private String node;

    public DockResult(String id, String pathToHDFS, LongWritable key) {
        this.id = id;
        this.runs = 10;
        this.rmsd = 0F;
        this.numHBonds = 0;
        this.HBonds = "";
        this.pathDLGinHDFS = pathToHDFS + "/" + id + ".dlg";
        String[] parts = id.split("_");
        this.target = parts[0];
        this.ligand = parts[1];
        this.flexiblePart = parts[2];
        this.key = key;
        this.energy = -1000.001F;
        this.success = true;
        this.causeFail = "Unknown status";
        try {
            this.node = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            this.node = "Unknown";
        }
    }

    public DockResult(){
        this.id = "id";
        this.pathDLGinHDFS = "/" + id + ".dlg";
        this.key = new LongWritable(0);
        this.energy = -1000.001F;
        this.success = true;
        this.causeFail = "Unknown status";
        this.node = "Unknown";
        this.runs = 10;
        this.rmsd = 0F;
        this.numHBonds = 0;
        this.HBonds = "";
        this.target = "None";
        this.ligand = "None";
        this.flexiblePart = "None";
    }
    /*@Override
    public int compareTo(LongWritable o) {
        return key.compareTo(o);
    }*/

    public void fail(String cause) {
        success = false;
        energy = -100000F;//Warning dont use static constant
        causeFail = cause;
    }

    public void success(String pathToResult,FileSystem system) throws IOException {
        success = true;
        parseResults(pathToResult,system);
        causeFail = "Not Fail";
    }

    private void parseResults(String path,FileSystem system) throws IOException {
        ArrayList<String> lines = FileUtils.readFile(path,system);
        String[] data = lines.get(1).split(",");
        runs = Integer.parseInt(data[1]);
        energy = Float.parseFloat(data[4]);
        rmsd = Float.parseFloat(data[5]);
        HBonds = data[6];
        numHBonds = Integer.parseInt(data[7]);
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
    /*  @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        pathDLGinHDFS = dataInput.readUTF();
        success = dataInput.readBoolean();
        energy = dataInput.readFloat();
        causeFail = dataInput.readUTF();
        key = new LongWritable();
        key.readFields(dataInput);
        node = dataInput.readUTF();
        rmsd = dataInput.readFloat();
        HBonds = dataInput.readUTF();
        numHBonds = dataInput.readInt();
        runs = dataInput.readInt();
        target = dataInput.readUTF();
        ligand = dataInput.readUTF();
        flexiblePart = dataInput.readUTF();
    }
  @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pathDLGinHDFS);
        dataOutput.writeBoolean(success);
        dataOutput.writeFloat(energy);
        dataOutput.writeUTF(causeFail);
        key.write(dataOutput);
        dataOutput.writeUTF(node);
        dataOutput.writeFloat(rmsd);
        dataOutput.writeUTF(HBonds);
        dataOutput.writeInt(numHBonds);
        dataOutput.writeInt(runs);
        dataOutput.writeUTF(target);
        dataOutput.writeUTF(ligand);
        dataOutput.writeUTF(flexiblePart);
    }*/
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
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy-hh-mm-ss");
        Date start = new Date(System.currentTimeMillis());
        return format.format(start);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    public String getPathDLGinHDFS() {
        return pathDLGinHDFS;
    }
    public void setPathDLGinHDFS(String pathDLGinHDFS) {
        this.pathDLGinHDFS = pathDLGinHDFS;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getLigand() {
        return ligand;
    }

    public void setLigand(String ligand) {
        this.ligand = ligand;
    }

    public String getFlexiblePart() {
        return flexiblePart;
    }

    public void setFlexiblePart(String flexiblePart) {
        this.flexiblePart = flexiblePart;
    }

    public int getRuns() {
        return runs;
    }

    public void setRuns(int runs) {
        this.runs = runs;
    }

    public float getEnergy() {
        return energy;
    }

    public void setEnergy(float energy) {
        this.energy = energy;
    }

    public float getRmsd() {
        return rmsd;
    }

    public void setRmsd(float rmsd) {
        this.rmsd = rmsd;
    }

    public int getNumHBonds() {
        return numHBonds;
    }

    public void setNumHBonds(int numHBonds) {
        this.numHBonds = numHBonds;
    }

    public String getHBonds() {
        return HBonds;
    }

    public void setHBonds(String HBonds) {
        this.HBonds = HBonds;
    }
    public boolean isSuccess() {
        return success;
    }
    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getCauseFail() {
        return causeFail;
    }

    public void setCauseFail(String causeFail) {
        this.causeFail = causeFail;
    }

    public LongWritable getKey() {
        return key;
    }

    public void setKey(LongWritable key) {
        this.key = key;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }



}
