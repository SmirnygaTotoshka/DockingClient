package ru.smirnygatotoshka.docking;

import org.apache.hadoop.io.Writable;
import ru.smirnygatotoshka.exception.TaskException;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.logging.Level;


public class DockingClient implements Writable {
    /**
     * This string should be in start all servicing messages
     */
    public final String SERVICE = "SERVICE:";
    private ArrayList<String> messages;
    private Socket socket;
    private String nameSlave;

    public String getNameSlave() {
        return nameSlave;
    }
    public static DockingClient createClient(Parameters parameters)
    {
        try {
            String master = parameters.getMasterNode().split(":")[0];
            return new DockingClient(InetAddress.getByName(master), 4445);
        }
        catch (Exception e) {
            parameters.getLog().log(Level.WARNING,e.getMessage());//Write to local log only and only if can`t connect to server
            return null;
        }
    }

    private DockingClient(InetAddress serverAddress, int serverPort) throws Exception {
        this.socket = new Socket(serverAddress, serverPort);
        this.nameSlave = InetAddress.getLocalHost().getHostName();
        this.messages = new ArrayList<>();
    }
    public void send(Parameters parameters)
    {
        try {
            send();
        }
        catch (IOException | TaskException e)
        {
            parameters.getLog().log(Level.WARNING,e.getMessage());//Write to local log only and only if can`t connect to server
        }
    }
    /**
     * Send all messages to server. Removing this messages after sending.
     * */
    public void send() throws IOException, TaskException {
        if (messages.isEmpty()) throw new TaskException("Trying send empty message");
        PrintWriter out = new PrintWriter(this.socket.getOutputStream(), true);
        for (String i: messages)
            out.println(i);
        out.flush();
        out.close();
        if (!messages.removeAll(messages)) throw new TaskException("Cannot clear list of messages");
    }

    public void addMessage(String msg)
    {
        messages.add(msg);
    }
}
