package ru.smirnygatotoshka.docking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

public class Parameters
{

    public String[] getArgs() {
        return args;
    }

    private String[] args;
    private String pathToMGLTools;
    private String pathToWorkspace;

    public String getMasterNode() {
        return masterNode;
    }

    private String masterNode;
    private FileSystem hdfs;
    private FileSystem local;
    private Logger log;


    public String getPathToMGLTools() {
        return pathToMGLTools;
    }

    public String getPathToWorkspace() {
        return pathToWorkspace;
    }

    public FileSystem getHDFS() {
        return hdfs;
    }

    public FileSystem getLocal() {
        return local;
    }

    public Logger getLog() {
        return log;
    }


    private void init()
{
    Configuration configuration = new Configuration();
    pathToMGLTools = args[3];
    pathToWorkspace = args[4];
    masterNode = args[5];
    try
    {
        hdfs = FileSystem.get(configuration);
        local = FileSystem.getLocal(configuration);
        if (hdfs == null || local == null) throw new NullPointerException("Неправильная инициализация файловых систем:"+configuration.toString());
    }
    catch (IOException e)
    {
        e.printStackTrace();
    }
    log = setupLog();
}


    public Parameters(String[] a)
    {
        args = a;
        init();
    }


    private Logger setupLog()
    {
        Logger l = Logger.getLogger("");
        String date = new SimpleDateFormat("dd-MM-yyyy-hh-mm-ss").format(new Date());
        String logFile = pathToWorkspace + File.separator + "dock_" + date + "_%u.log";
        try{
            FileHandler logHandler = new FileHandler(logFile,524288000,1,false);
            Level defaultLevel = Level.ALL;
            logHandler.setFormatter(new SimpleFormatter());
            logHandler.setLevel(defaultLevel);
            for(Handler h:l.getHandlers())
                l.removeHandler(h);
            l.addHandler(logHandler);
            return l;

        }
        catch (SecurityException | IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

}
