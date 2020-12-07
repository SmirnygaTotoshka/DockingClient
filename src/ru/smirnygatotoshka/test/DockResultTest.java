package ru.smirnygatotoshka.test;

import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;
import ru.smirnygatotoshka.docking.DockJob;
import ru.smirnygatotoshka.docking.DockResult;

import java.io.*;

import static org.junit.Assert.*;

public class DockResultTest {

    private DockResult result;
    private DataOutputStream dataOutput;
    private DataInputStream dataInput;
    @Before
    public void setUp() throws Exception {
        result = new DockResult("id","/user/hduser",new LongWritable(20));

    }

    @Test
    public void write() {
        assertEquals("id",result.getId());
        assertEquals("/user/hduser/id.dlg",result.getPathDLGinHDFS());
        assertTrue(-1000.001F == result.getEnergy());
        assertTrue(result.isSuccess());
        assertEquals(new LongWritable(20),result.getKey());
        assertEquals("Unknown status",result.getCauseFail());

    }

    @Test
    public void readFields() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        dataOutput = new DataOutputStream(buffer);
        result.write(dataOutput);

        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        dataInput = new DataInputStream(in);

        DockResult result1 = new DockResult("","",new LongWritable(1));
        result1.readFields(dataInput);

        assertEquals(result1.getId(),result.getId());
        assertEquals(result1.getPathDLGinHDFS(),result.getPathDLGinHDFS());
        assertTrue(result1.getEnergy() == result.getEnergy());
        assertTrue(result1.isSuccess() == result.isSuccess());
        assertEquals(result1.getKey(),result.getKey());
        assertEquals(result1.getCauseFail(),result.getCauseFail());

    }

    @Test
    public void testToString() {
        String expected = "id\t/user/hduser/id.dlg\ttrue\t-1000.001\t" + "Unknown status";
        assertEquals(expected,result.toString());
    }

}