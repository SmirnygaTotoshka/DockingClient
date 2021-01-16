package ru.smirnygatotoshka.docking;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;


public class DockingClient {
    private Socket socket;
    private ClusterProperties clusterProperties;
    private Log log;

    public DockingClient(ClusterProperties clusterProperties, String id) throws IOException {
        this.clusterProperties = clusterProperties;
        log = new Log(clusterProperties.getWorkspaceLocalDir() + File.separator + id + "_client.log");
    }

    public void send(DockResult result){
        String increment = "";
        if (result.isSuccess())
            increment = formIncrement(Statistics.Counters.SUCCESS, 1);
        else
            increment = formIncrement(Statistics.Counters.FAILED, 1);
        try{
            this.socket = new Socket(InetAddress.getByName(clusterProperties.getIpAddressMasterNode()), clusterProperties.getPort());
            PrintWriter out = new PrintWriter(this.socket.getOutputStream(), true);
            out.println(increment);
            out.println(result.toString());
            out.flush();
            out.close();
            socket.close();
        }
        catch (IOException e) {
            log.write("Can`t establish connection " + e.getMessage()+"\n");
            log.write(increment+"\n");
        }
    }


    public String formIncrement(Statistics.Counters counter, int num) {
        return counter.name() + "=" + num;
    }
    public void close() throws IOException {
        log.close();
    }

}
