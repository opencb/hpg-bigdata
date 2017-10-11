package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Test;

public class VariantStatsCLITest {
    //@Test
    public void stats() {
        try {

            VariantRvTestsCLITest rvTestsCLITest = new VariantRvTestsCLITest();
            rvTestsCLITest.init();

            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant stats");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(rvTestsCLITest.avroPath);
            //commandLine.append(" --rvtests-path ../../../soft/rvtests/executable/rvtest");
            //commandLine.append(" -Dsingle=wald");
            //commandLine.append(" -Dout=/tmp/out.wald");
            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
