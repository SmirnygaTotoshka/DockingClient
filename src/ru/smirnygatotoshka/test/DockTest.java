package ru.smirnygatotoshka.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import ru.smirnygatotoshka.docking.ClusterProperties;
import ru.smirnygatotoshka.docking.Dock;

import static org.junit.Assert.assertEquals;

public class DockTest {

    private Dock dock;
    @Before
    public void setUp() throws Exception {
        Text line = new Text("user/hduser/testNew;4apu_clear.pdbqt;;4apu_lig.pdbqt;4apu_lig.gpf;-p gridcenter=-1.206,14.731,9.641 -p npts=50,50,50;d.dpf;-p ga_run=20\n");
        ClusterProperties cluster = new ClusterProperties("D:\\cfg.txt",new Configuration());
        dock = new Dock(line,cluster,new LongWritable(1));
    }

    @Test
    public void splitProperties() {
        assertEquals("user/hduser/testNew",dock.getDockingProperties().getPathToFiles());
        assertEquals("4apu_clear.pdbqt",dock.getDockingProperties().getReceptor());
        assertEquals("",dock.getDockingProperties().getReceptorFlexiblePart());
        assertEquals("4apu_lig.pdbqt",dock.getDockingProperties().getLigand());
        assertEquals("4apu_lig.gpf",dock.getDockingProperties().getGpfName());
        assertEquals("-p gridcenter=-1.206,14.731,9.641 -p npts=50,50,50",dock.getDockingProperties().getGpfParameters());
        assertEquals("d.dpf",dock.getDockingProperties().getDpfName());
        assertEquals("-p ga_run=20",dock.getDockingProperties().getDpfParameters());
        
    }
}