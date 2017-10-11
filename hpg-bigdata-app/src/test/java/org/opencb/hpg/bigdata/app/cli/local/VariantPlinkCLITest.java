package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Test;

public class VariantPlinkCLITest {
    //@Test
    public void assoc() {
        try {
            VariantRvTestsCLITest cli = new VariantRvTestsCLITest();
            cli.init();

            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant plink");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" --dataset ").append(cli.datasetName);
            commandLine.append(" -i ").append(cli.avroPath);
            commandLine.append(" --plink-path ../../../soft/plink/plink");
            commandLine.append(" -Dassoc=");
            commandLine.append(" -Dout=/tmp/out.plink");
            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
