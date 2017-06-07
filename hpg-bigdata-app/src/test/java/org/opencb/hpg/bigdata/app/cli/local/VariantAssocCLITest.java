package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by joaquin on 1/19/17.
 */
public class VariantAssocCLITest {
    Path inPath;
    Path outPath;
    Path confPath;

    private void init() throws URISyntaxException {
        String root = "/home/jtarraga//data150/test/assoc";
        inPath = Paths.get(root + "/test.vcf.avro");
        outPath = Paths.get(root + "/assoc.out");
    }

    @Test
    public void assoc() {
        try {
            init();

            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant association");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(inPath);
            commandLine.append(" -o ").append(outPath);
            commandLine.append(" --dataset noname");
            //commandLine.append(" --logistic");
            commandLine.append(" --linear");
            commandLine.append(" --pheno Age:s");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
