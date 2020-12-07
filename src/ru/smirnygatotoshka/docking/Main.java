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
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy-hh-mm-ss");
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
            String taskName = cluster.equals(null) ? "FAILED_TASK" : cluster.getTaskName();
            String message = "Task " + taskName + " has completed with code " + res + ".\n" +
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

    //private void

    /**
     * @deprecated
     * CMD arguments
     * 0 - input csv in hdfs
     * 1 - output path in hdfs
     * 2 - task id
     * 3 - path to MGLTools(local)
     * 4 - output path(local)
     * 5 - address to master node
     * 6 - number of reduse task
     * 7 - is needed prepare_gpf
     * */
  /*  public static void main(String[] args) throws Exception {
        Date start = new Date(System.currentTimeMillis());
        int res = ToolRunner.run(new Configuration(), new DockJob(), args);
        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
        Date finish = new Date(System.currentTimeMillis());
        String message = "Task " + args[2] + " has completed with code " + res + ".\n" +
                "Statistics:\n" +
                "Start time " + formatter.format(start) + "\n" +
                "Success = " + job.getCounters().findCounter(DockJob.Counters.GOOD).getValue() + "\n" +
                "Fail = " + job.getCounters().findCounter(DockJob.Counters.BAD).getValue()  + "\n" +
                "All = " + job.getCounters().findCounter(DockJob.Counters.ALL).getValue() + ".\n" +
                "Finish time " + formatter.format(finish) +
                ". With the best wishes,Hadoop cluster of bioinformatics department MBF RNRMU.";
        SenderMail senderMail = new SenderMail(message);
        senderMail.send();
        System.exit(res);
    }*/
}
