package ru.smirnygatotoshka.test;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import ru.smirnygatotoshka.docking.ClusterProperties;

import static org.junit.Assert.assertEquals;

public class ClusterPropertiesTest {

    private ClusterProperties cluster;
    @Before
    public void setUp() throws Exception {
        cluster = new ClusterProperties("D:\\cfg.txt",new Configuration());
    }

    @Test
    public void getOutputPath() {
        assertEquals("/user/hduser/MEK2/output",cluster.getOutputPath());
    }

    @Test
    public void getTaskName() {
        assertEquals("Test",cluster.getTaskName());
    }

    @Test
    public void getWorkspaceLocalDir() {
        assertEquals("/workspace",cluster.getWorkspaceLocalDir());
    }

    @Test
    public void getPathToMGLTools() {
        assertEquals("/MGLTools",cluster.getPathToMGLTools());
    }

    @Test
    public void getIpAddressMasterNode() {
        assertEquals("192.168.4.112",cluster.getIpAddressMasterNode());
    }

    @Test
    public void getMapperNumber() {
        assertEquals("20",cluster.getMapperNumber());
    }
}