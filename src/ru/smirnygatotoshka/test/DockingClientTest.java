package ru.smirnygatotoshka.test;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import ru.smirnygatotoshka.docking.ClusterProperties;
import ru.smirnygatotoshka.docking.DockingClient;
import ru.smirnygatotoshka.docking.Statistics;

import static org.junit.Assert.assertEquals;

public class DockingClientTest {

    private DockingClient client;
    @Before
    public void setUp() throws Exception {
        ClusterProperties clusterProperties = new ClusterProperties("D:\\cfg.txt", new Configuration());
        //client = new DockingClient(clusterProperties);
    }

    //@Test
    //public void setupLog() {
     //   assertNotNull(client.getLog());
   // }

    @Test
    public void formIncrement() {
        assertEquals("SUCCESS=5",client.formIncrement(Statistics.Counters.SUCCESS,5));
        assertEquals("ANALYZE_FAIL=1",client.formIncrement(Statistics.Counters.ANALYZE_FAIL,1));
        assertEquals("EXECUTION_FAIL=10",client.formIncrement(Statistics.Counters.EXECUTION_FAIL,10));
        assertEquals("ALL=200",client.formIncrement(Statistics.Counters.ALL,200));

    }
}