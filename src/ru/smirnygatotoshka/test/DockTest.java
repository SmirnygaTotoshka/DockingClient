package ru.smirnygatotoshka.test;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import ru.smirnygatotoshka.docking.Dock;
import ru.smirnygatotoshka.docking.DockingProperties;

import static org.junit.Assert.assertEquals;

public class DockTest {

    @Test
    public void splitProperties() {
        Text line = new Text("user/hduser/testNew;4apu_clear.pdbqt;;4apu_lig.pdbqt;4apu_lig.gpf,-p gridcenter=-1.206,14.731,9.641 -p npts=50,50,50;d.dpf;-p ga_run=20\n");
        DockingProperties prop = new Dock().splitProperties(line);
        assertEquals("user/hduser/testNew",prop.getPathToFiles());
        assertEquals("4apu_clear.pdbqt",prop.getPathToFiles());
        assertEquals("",prop.getPathToFiles());
        assertEquals("4apu_lig.pdbqt",prop.getPathToFiles());
        assertEquals("",prop.getPathToFiles());
        assertEquals("",prop.getPathToFiles());
        assertEquals("",prop.getPathToFiles());
    }
}