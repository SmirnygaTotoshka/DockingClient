package ru.smirnygatotoshka.docking;

import ru.smirnygatotoshka.exception.TaskException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DockingServer extends Thread{
    private ServerSocket server;
    private ExecutorService executorService = Executors.newFixedThreadPool(50);
    private Statistics statistics = Statistics.getInstance();

    public DockingServer(ClusterProperties clusterProperties) throws Exception {
        this.server = new ServerSocket(4445, 1, InetAddress.getByName(clusterProperties.getIpAddressMasterNode()));
    }

    @Override
    public void run(){
        try {
            listen();//TODO - Server fall after 1 connection closed. Repair + send one message not many
        }
        catch (Exception e) {
            System.err.println("Server is fall");
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
                    catch (IOException | TaskException e) {
                        System.err.println("Возникла ошибка на сервере при обработке ответа:");
                        e.printStackTrace();
                    }
                });
            }
            catch (SocketException e)
            {
                System.out.println("Connection has closed.");
            }
        }
        server.close();
        executorService.shutdown();
    }
    private void parseAnswer(InputStream stream) throws IOException, TaskException, NumberFormatException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String answer = reader.readLine();
        String[] comp = answer.split("=");
        int num = Integer.parseInt(comp[1]);
        Statistics.Counters c = Statistics.Counters.valueOf(comp[0]);
        statistics.incrCounter(c ,num);
        printAnswer(c);
    }

    private void printAnswer(Statistics.Counters counter){
        if (counter.equals(Statistics.Counters.ALL))
            printDockingProgress();
        else
            printAnalyzeProgress();
    }

    private void printDockingProgress(){
        synchronized (statistics) {
            String bar = "\rExecution[";
            int progress = (int) statistics.getAll() / statistics.getNumTasks() * 100;
            int symbols = progress / 5;
            for (int i = 0; i <= symbols; i++) {
                bar += "+";
            }
            bar += "]\t" + progress + "%\t" + statistics.getAll() + "/" + statistics.getNumTasks();
            System.out.println(bar);
        }
    }

    private void printAnalyzeProgress(){
        synchronized (statistics) {
            String bar = "\rAnalyze[";
            int progress = (int) (statistics.getExecuteFail() + statistics.getSuccess() + statistics.getAnalyzeFail()) / statistics.getNumTasks() * 100;
            int symbols = progress / 5;
            for (int i = 0; i <= symbols; i++) {
                bar += "+";
            }
            int success_procent = statistics.getSuccess() / statistics.getNumTasks() * 100;
            int fail_procent = (statistics.getExecuteFail() + statistics.getAnalyzeFail()) / statistics.getNumTasks() * 100;
            bar += "]\t" + progress + "%\t" +  "Успешно = " + success_procent + "\tПровалено = " + fail_procent;
            System.out.println(bar);
        }
    }
}