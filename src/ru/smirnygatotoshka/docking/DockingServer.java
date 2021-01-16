package ru.smirnygatotoshka.docking;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DockingServer extends Thread{
    private ServerSocket server;
    private ExecutorService executorService = Executors.newFixedThreadPool(50);
    private Statistics statistics = Statistics.getInstance();
    //write to server local filesystem
    private BufferedWriter backupResult;
    public DockingServer(ClusterProperties clusterProperties) throws IOException {
        this.server = new ServerSocket(clusterProperties.getPort(), 1, InetAddress.getByName(clusterProperties.getIpAddressMasterNode()));
        String pathToBackupResult = clusterProperties.getWorkspaceLocalDir() + File.separator + "result.tsv";
        this.backupResult = new BufferedWriter(new FileWriter(pathToBackupResult, true));
    }

    @Override
    public void run(){
        try {
            System.out.println("Server is started.");
            listen();
        }
        catch (Exception e) {
            System.out.println("Server is fall.");
            e.printStackTrace();
        }
    }
    private synchronized void listen() throws Exception {
        server.setSoTimeout(0);
        while (isAlive())
        {
            try {
                Socket client = server.accept();
                executorService.execute(() -> {
                    try {
                        String clientName = client.getInetAddress().getHostName();
                        System.out.println("New connection from " + clientName);
                        parseAnswer(client.getInputStream());
                        client.close();
                    }
                    catch (IOException | TaskException | NumberFormatException e) {
                        System.out.println("Возникла ошибка на сервере при обработке ответа:");
                        e.printStackTrace();
                    }
                });
            }
            catch (SocketException e)
            {
                System.out.println("Connection has closed.");
            }
        }
        backupResult.close();
        server.close();
        executorService.shutdown();
        System.out.println("Server is stopped.");
    }

    private void parseAnswer(InputStream stream) throws IOException, TaskException, NumberFormatException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String answer = reader.readLine();
        String[] comp = answer.split("=");
        int num = Integer.parseInt(comp[1]);
        Statistics.Counters c = Statistics.Counters.valueOf(comp[0]);
        synchronized (statistics) {
            statistics.incrCounter(c, num);
            String bar = getTime() + "\tExecution[";
            try {
                float progress = ((float)statistics.getSuccess() + statistics.getFailed()) / statistics.getAll() * 100;
                int symbols = Math.round(progress / 5);
                for (int i = 0; i < symbols; i++) {
                    bar += "+";
                }
                for (int i = symbols; i < 20; i++){
                    bar += "-";
                }
                bar += "]\t" + String.format("%.4f",progress) + "%\n" +
                        "Success = " + statistics.getSuccess() + "/" + statistics.getAll() + "; Failed = " +
                        statistics.getFailed() + "/" + statistics.getAll();
                System.out.println(bar);
            }
            catch (ArithmeticException e){
                bar += "]\t" + statistics.getSuccess();
                System.out.println(bar);
            }
        }
         synchronized (backupResult){
            backupResult.write(reader.readLine());
            backupResult.newLine();
            backupResult.flush();
        }
    }


    private String getTime(){
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
        Date start = new Date(System.currentTimeMillis());
        return format.format(start);
    }

}
