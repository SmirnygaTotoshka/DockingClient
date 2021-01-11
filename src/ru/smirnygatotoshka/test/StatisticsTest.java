package ru.smirnygatotoshka.test;

import org.junit.Before;
import org.junit.Test;
import ru.smirnygatotoshka.docking.Statistics;

import java.text.DecimalFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StatisticsTest {

    private Statistics statistics;
    @Before
    public void setUp() throws Exception {
        statistics = Statistics.getInstance();
       // statistics.setNumTasks(120);
        statistics.incrCounter(Statistics.Counters.ALL,100);
       // statistics.incrCounter(Statistics.Counters.ANALYZE_FAIL,5);
/*        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("Start");
                    statistics.incrCounter(Statistics.Counters.EXECUTION_FAIL,15);
                    statistics.incrCounter(Statistics.Counters.SUCCESS,20);
                } catch (TaskException e) {
                    e.printStackTrace();
                }
                finally {
                    System.out.println("Finish");

                }

            }
        });
        t.start();*/
       // statistics.incrCounter(Statistics.Counters.EXECUTION_FAIL,15);
        statistics.incrCounter(Statistics.Counters.SUCCESS,20);


    }

    @Test
    public void incrCounter() {
        assertTrue(statistics.getAll() == 100);
       // assertTrue(statistics.getAnalyzeFail() == 5);
       // assertTrue(statistics.getExecuteFail() == 15);
        assertTrue(statistics.getSuccess() == 20);
       // assertTrue(statistics.getNumTasks() == 120);
    }

    @Test
    public void testToString() {
        DecimalFormat format = new DecimalFormat("###.##");
        String f,s;
        s = "20";
        f = "20";
        assertEquals("All = 100\nExecute fail = 15\nAnalyse fail = 5\nThere are failed " + f + "%.\n"+
                "Success = " + statistics.getSuccess() + "; There are " + s + "%.\n",statistics.toString());
    }
}