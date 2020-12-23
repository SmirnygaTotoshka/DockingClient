package ru.smirnygatotoshka.docking;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Класс, содержащий информацию о выполнении докинга. Служит результатом Map и ключом Reduce.
 * На этапе Map - получение идентификатора задачи и пути к файлу содержащий результаты
 * На этапе Reduce - определяется успешность выполнения и счиывается минимальная энергия.
 *
 * @author SmirnygaTotoshka
 */
public class DockResult implements WritableComparable<LongWritable> {
    private String id;
    private String pathDLGinHDFS;
    private float energy;
    private boolean success;
    private String causeFail;
    private LongWritable key;
    private String node;

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public String getId() {
        return id;
    }

    public float getEnergy() {
        return energy;
    }

    public String getCauseFail() {
        return causeFail;
    }

    public LongWritable getKey() {
        return key;
    }

    public void setCauseFail(String causeFail) {
        this.causeFail = causeFail;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPathDLGinHDFS(String pathDLGinHDFS) {
        this.pathDLGinHDFS = pathDLGinHDFS;
    }

    public void setEnergy(float energy) {
        this.energy = energy;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setKey(LongWritable key) {
        this.key = key;
    }

    /**
     * Random key for comparing. From zero to 2<sup>30</sup>
     */


    public DockResult(String id, String pathToHDFS, LongWritable key) {
        this.id = id;
        this.pathDLGinHDFS = pathToHDFS + "/" + id + ".dlg";
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

    }
    @Override
    public int compareTo(LongWritable o) {
        return key.compareTo(o);
    }



    public void fail(String cause) {
        success = false;
        energy = -100000F;//TODO - Warning dont use static constant
        causeFail = cause;
    }

    public void success(float energy) {
        success = true;
        this.energy = energy;
        causeFail = "Not Fail";
    }


    public String getPathDLGinHDFS() {
        return pathDLGinHDFS;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        pathDLGinHDFS = dataInput.readUTF();
        success = dataInput.readBoolean();
        energy = dataInput.readFloat();
        causeFail = dataInput.readUTF();
        key = new LongWritable();
        key.readFields(dataInput);
        node = dataInput.readUTF();
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
    }
    @Override
    public String toString() {
        return id + "\t" + pathDLGinHDFS + "\t" + success + "\t" + energy + "\t" + causeFail + "\t" + node;
    }

    /**
     * Вернуть конечное значение редьюсера
     */
    public Text getText() {
        return new Text(this.toString());
    }

}
