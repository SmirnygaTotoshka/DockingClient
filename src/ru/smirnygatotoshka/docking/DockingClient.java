package ru.smirnygatotoshka.docking;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;


public class DockingClient implements Writable {
    private Socket socket;
    private boolean hasConnection;
    private ClusterProperties clusterProperties;
    private Logger log;

    DockingClient() {

    }

    public DockingClient(ClusterProperties clusterProperties) {
        this.clusterProperties = clusterProperties;
        this.hasConnection = true;
        log = setupLog();
    }

    public void send(Statistics.Counters counter, int num){
        try{
            this.socket = new Socket(InetAddress.getByName(clusterProperties.getIpAddressMasterNode()), 4445);
            PrintWriter out = new PrintWriter(this.socket.getOutputStream(), true);
            String increment = formIncrement(counter, num);
            out.println(increment);
            out.flush();
            out.close();
            socket.close();
        } catch (IOException e) {
            log.warning("Can`t establish connection " + e.getMessage());
        }
    }

    private Logger setupLog()
    {
        Logger l = Logger.getLogger("");
        String date = new SimpleDateFormat("dd-MM-yyyy-hh-mm-ss").format(new Date());
        String logFile = clusterProperties.getWorkspaceLocalDir() + File.separator + "client_" + date + "_%u.log";
        try{
            FileHandler logHandler = new FileHandler(logFile,524288000,1,false);
            Level defaultLevel = Level.WARNING;
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

    public String formIncrement(Statistics.Counters counter, int num) {
        return counter.name() + "=" + num;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        clusterProperties.write(dataOutput);
        dataOutput.writeBoolean(hasConnection);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        clusterProperties = new ClusterProperties();
        clusterProperties.readFields(dataInput);
        socket = new Socket(InetAddress.getByName(clusterProperties.getIpAddressMasterNode()), 4445);
        hasConnection = dataInput.readBoolean();
    }

    public Logger getLog() {
        return log;
    }
}
