package ru.smirnygatotoshka.docking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.*;

/**
 * Содержит информацию о кластере, на котором выполняется программа, и некоторую информацию, необходимую для работы.
 * @author SmirnygaTotoshka
 * */
public class ClusterProperties implements Writable {
//TODO - tests
    public static final String OUTPUT = "outputDir";
    public static final String NAME = "nameTask";
    public static final String WORKSPACE = "workspaceLocalDir";
    public static final String MGLTOOLS = "mgltoolsDir";
    public static final String HOST = "ipMaster";
    public static final String NUM_MAPPERS = "nMap";

    private String outputPath;
    private String taskName;
    private String workspaceLocalDir;
    private String pathToMGLTools;
    private String ipAddressMasterNode;
    private String mapperNumber;
    private JobConf jobConf;

    public ClusterProperties(String pathToConfig, Configuration conf) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(pathToConfig));
        this.outputPath = reader.readLine();
        this.taskName = reader.readLine();
        this.workspaceLocalDir = reader.readLine();
        this.pathToMGLTools = reader.readLine();
        this.ipAddressMasterNode = reader.readLine();
        this.mapperNumber = reader.readLine();
        this.jobConf = new JobConf(conf, DockJob.class);
        reader.close();
    }

    protected ClusterProperties() {

    }

    public JobConf getJobConf() {
        return jobConf;
    }

    public ClusterProperties(JobConf jobConf){
        this.outputPath = jobConf.get(OUTPUT);
        this.taskName = jobConf.get(NAME);
        this.workspaceLocalDir = jobConf.get(WORKSPACE);
        this.pathToMGLTools = jobConf.get(MGLTOOLS);
        this.ipAddressMasterNode = jobConf.get(HOST);
        this.mapperNumber = jobConf.get(NUM_MAPPERS);
        this.jobConf = jobConf;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getWorkspaceLocalDir() {
        return workspaceLocalDir;
    }

    public String getPathToMGLTools() {
        return pathToMGLTools;
    }

    public String getIpAddressMasterNode() {
        return ipAddressMasterNode;
    }

    public String getMapperNumber() {
        return mapperNumber;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(outputPath);
        dataOutput.writeUTF(taskName);
        dataOutput.writeUTF(workspaceLocalDir);
        dataOutput.writeUTF(pathToMGLTools);
        dataOutput.writeUTF(ipAddressMasterNode);
        dataOutput.writeUTF(mapperNumber);
        jobConf.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.outputPath = dataInput.readUTF();
        this.taskName = dataInput.readUTF();
        this.workspaceLocalDir = dataInput.readUTF();
        this.pathToMGLTools = dataInput.readUTF();
        this.ipAddressMasterNode = dataInput.readUTF();
        this.mapperNumber = dataInput.readUTF();
        jobConf = new JobConf();
        jobConf.readFields(dataInput);
    }


}
