package ru.smirnygatotoshka.test;

import org.junit.Test;
import ru.smirnygatotoshka.docking.DockResult;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class DockResultTest {

    @Test
    public void fromString() throws IOException {
        String testStr = "31-01-2021-13-56-50\tbislave2\tmek2_STOCK5S-99272_None_g\ttrue\tNot Fail\t/user/hduser/SC_PART8/mek2_STOCK5S-99272_None_g.dlg\tmek2\tSTOCK5S-99272\tNone\t -9.1600\t 61.5530\t 4\t<Atom instance> mek2:B:ASN82:HN~[<Atom instance> /home/hduser/SC8/mek2_STOCK5S-99272_None_g/STOCK5S-99272: :LIG1:O]@<Atom instance> mek2:B:ASN82:HD22~[<Atom instance> /home/hduser/SC8/mek2_STOCK5S-99272_None_g/STOCK5S-99272: :LIG1:O]@<Atom instance> mek2:B:LYS101:HZ2~[<Atom instance> /home/hduser/SC8/mek2_STOCK5S-99272_None_g/STOCK5S-99272: :LIG1:O]@<Atom instance> mek2:B:LYS196:HZ1~[<Atom instance> /home/hduser/SC8/mek2_STOCK5S-99272_None_g/STOCK5S-99272: :LIG1:O]@\t10";
        DockResult actual = DockResult.fromString(testStr, null);
        assertEquals(testStr, actual.toString());
    }
}