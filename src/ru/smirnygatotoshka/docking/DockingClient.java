package ru.smirnygatotoshka.docking;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import ru.smirnygatotoshka.exception.TaskException;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;


public class DockingClient implements Writable {
    /**
     * This string should be in start all servicing messages
     */
    private ArrayList<String> messages;
    private Socket socket;
    private String nameSlave;
    private boolean hasConnection;
    private volatile File logs;
    private String errorMessage;//if cant be establish connection
    private ClusterProperties clusterProperties;

    public String getNameSlave() {
        return nameSlave;
    }

    DockingClient() {

    }

    public DockingClient(ClusterProperties clusterProperties) {
        try {
            this.messages = new ArrayList<>();
            this.clusterProperties = clusterProperties;
            this.nameSlave = InetAddress.getLocalHost().getHostName();
            this.hasConnection = true;
            this.errorMessage = "";
            this.logs = new File(clusterProperties.getWorkspaceLocalDir() + File.separator + "clientLogs.txt");
        } catch (IOException e) {
            this.hasConnection = false;
            this.errorMessage = e.getMessage();
            messages.add(this.errorMessage);
        }
    }

    /**
     * Send all messages to server. Removing this messages after sending.
     */
    public void send() {
        try {
            this.socket = new Socket(InetAddress.getByName(clusterProperties.getIpAddressMasterNode()), 4445);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
            objectOutputStream.writeObject(Statistics.getInstance());
            objectOutputStream.close();
            if (!messages.removeAll(messages)) throw new TaskException("Cannot clear list of messages");
            socket.close();
        } catch (TaskException | IOException e) {
            messages.add(e.getMessage());
            try {
                writeToLog(messages);
            } catch (IOException ex) {
                ex.printStackTrace();// TODO - ?куда записывать?
            }
        }
    }

    private void writeToLog(ArrayList<String> lines) throws IOException {
        synchronized (logs) {
            FileUtils.writeFile(lines, logs.getAbsolutePath(), FileSystem.getLocal(clusterProperties.getJobConf()));
        }
    }

    public void addMessage(String msg) {
        messages.add(msg);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        clusterProperties.write(dataOutput);
        dataOutput.writeInt(messages.size());
        for (String s : messages)
            dataOutput.writeUTF(s);
        dataOutput.writeUTF(nameSlave);
        dataOutput.writeBoolean(hasConnection);
        dataOutput.writeUTF(errorMessage);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        clusterProperties = new ClusterProperties();
        clusterProperties.readFields(dataInput);
        socket = new Socket(InetAddress.getByName(clusterProperties.getIpAddressMasterNode()), 4445);
        for (int i = 0; i < dataInput.readInt(); i++) {
            messages.add(dataInput.readUTF());
        }
        nameSlave = dataInput.readUTF();
        hasConnection = dataInput.readBoolean();
        errorMessage = dataInput.readUTF();
    }
}
