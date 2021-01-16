package ru.smirnygatotoshka.docking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Содержит информацию о кластере, на котором выполняется программа, и некоторую информацию, необходимую для работы.
 * @author SmirnygaTotoshka
 * */
public class ClusterProperties {
    /** Константы-ключи, по которым восстанавливается экземпляр объекта на узлах из JobConf*/

    public static final String OUTPUT = "outputDir";
    public static final String NAME = "nameTask";
    public static final String WORKSPACE = "workspaceLocalDir";
    public static final String MGLTOOLS = "mgltoolsDir";
    public static final String HOST = "ipMaster";
    public static final String PORT = "port";
    public static final String NUM_MAPPERS = "nMap";

    /**Выходная папка в hdfs*/
    private String outputPath;
    /**Имя задачи*/
    private String taskName;
    /**Локальная рабочая папка. На всех узлах должен быть одинаковый путь.*/
    private String workspaceLocalDir;
    /**Путь к скриптам MGLTools. На всех узлах должен быть одинаковый путь.*/
    private String pathToMGLTools;
    /**Адрес мастерского узла, на котором запущен информационный сервер
     * @see DockingServer*/
    private String ipAddressMasterNode;
    /**Порт информационного сервера*/
    private int port;
    /**Число map tasks*/
    private String mapperNumber;

    private JobConf jobConf;
    /**
     * Конструткор для создания экземляра при запуске программы из файла-конфига на мастере
     * */
    public ClusterProperties(String pathToConfig, Configuration conf) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(pathToConfig));
        this.outputPath = reader.readLine();
        this.taskName = reader.readLine();
        this.workspaceLocalDir = reader.readLine();
        this.pathToMGLTools = reader.readLine();
        this.ipAddressMasterNode = reader.readLine();
        this.port = Integer.parseInt(reader.readLine());
        this.mapperNumber = reader.readLine();
        this.jobConf = new JobConf(conf, DockJob.class);
        reader.close();
    }


    public JobConf getJobConf() {
        return jobConf;
    }
    /**
     * Конструктор для восстановления объекта из JobConf
     * */
    public ClusterProperties(JobConf jobConf){
        this.outputPath = jobConf.get(OUTPUT);
        this.taskName = jobConf.get(NAME);
        this.workspaceLocalDir = jobConf.get(WORKSPACE);
        this.pathToMGLTools = jobConf.get(MGLTOOLS);
        this.ipAddressMasterNode = jobConf.get(HOST);
        this.port = Integer.parseInt(jobConf.get(PORT));
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

    public int getPort() {
        return port;
    }


}
