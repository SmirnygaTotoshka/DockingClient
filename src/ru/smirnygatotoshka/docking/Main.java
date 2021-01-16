package ru.smirnygatotoshka.docking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import javax.mail.MessagingException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {


    /**
     * @param args - Аргументы командной строки
     * 0 - Путь к файлу(hdfs), содержащий описание каждой отдельной задачи(докинга). Имеет формат csv с разделителем ';'.
     *    <p> Путь к файлу в hdfs; рецептор(pdbqt); гибкая часть рецептора(pdbqt);лиганд(pdbqt); имя gpf файла;параметры скрипта
     *             prepare_gpf4.py(MGLTools); имя dpf файла; параметры скрипта prepare_dpf42.py(MGLTools)</p>
     * 1 - Путь к файлу содержащий информацию о кластере и текущем запуске(local). Имеет формат csv с разделителем ';'.
     *             <p>Выходная директория; Имя запуска; Путь к MGLTools; Путь к рабочей папке на локальных машинах;
     *             ip адрес главного узла; Количество reducer</p>
     * */
    public static void main(String[] args) {
        int res = 0;
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
        Date start = new Date(System.currentTimeMillis());
        Date finish;
        ClusterProperties cluster = null;
        try {
            Configuration configuration = new Configuration();
            cluster = new ClusterProperties(args[1], configuration);
            DockingServer server = new DockingServer(cluster);
            DockJob dockJob = new DockJob(cluster);
            server.start();
            res = ToolRunner.run(configuration, dockJob, args);
        }
        catch (Exception e){
            res = -1;
            if (e instanceof NullPointerException)
                res = -2;
            if (e instanceof IOException)
                res = -3;
            e.printStackTrace();
        }
        finally {
            finish = new Date(System.currentTimeMillis());
            String message = "Task " + cluster.getTaskName() + " has completed with code " + res + ".\n" +
                    "Statistics:\n" +
                    "Start time " + format.format(start) + "\n" +
                    Statistics.getInstance().toString() +
                    "Finish time " + format.format(finish) +
                    ". With the best wishes,Hadoop cluster of bioinformatics department MBF PRNRMU.";
            SenderMail senderMail = new SenderMail(message);
            try {
                senderMail.send();
            }
            catch (MessagingException e) {
                e.printStackTrace();
            }
            System.out.println("Res="+res);
            System.exit(res);
        }
    }
}
