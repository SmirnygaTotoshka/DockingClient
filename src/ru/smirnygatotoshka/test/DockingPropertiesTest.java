package ru.smirnygatotoshka.test;

import org.junit.Before;
import org.junit.Test;
import ru.smirnygatotoshka.docking.DockingProperties;

import static org.junit.Assert.assertEquals;

public class DockingPropertiesTest {

    private DockingProperties prop,prop1;

    @Before
    public void setUp() throws Exception {
        prop = new DockingProperties("/user/hduser/MEK2","mek2.pdbqt","","lig.pdbqt",
                "g.gpf","-p gridcenter 20,20,20 -y","d.dpf","-p ga_run 100");
        prop1 = new DockingProperties("/user/hduser/MEK2","mek2.pdbqt","flex.pdbqt","lig.pdbqt",
                "g.gpf","-p gridcenter 20,20,20 -y","d.dpf","-p ga_run 100");
    }

    @Test
    public void getId() {
        assertEquals("mek2_lig_None_g",prop.getId());
        assertEquals("mek2_lig_flex_g", prop1.getId());
    }

    @Test
    public void getReceptorPath() {
        assertEquals("/user/hduser/MEK2/mek2.pdbqt",prop.getReceptorPath());
    }

    @Test
    public void getLigandPath() {
        assertEquals("/user/hduser/MEK2/lig.pdbqt",prop.getLigandPath());
    }

    @Test
    public void getReceptorFlexiblePartPath() {
        assertEquals("",prop.getReceptorFlexiblePartPath());
        assertEquals("/user/hduser/MEK2/flex.pdbqt",prop1.getReceptorFlexiblePartPath());
    }
}