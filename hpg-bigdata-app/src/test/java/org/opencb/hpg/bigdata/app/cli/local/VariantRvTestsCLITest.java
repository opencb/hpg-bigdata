package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by joaquin on 1/19/17.
 */
public class VariantRvTestsCLITest {
    Path inPath;
    Path outPath;
    Path confPath;

    private void init() throws URISyntaxException {
        inPath = Paths.get("/home/jtarraga/data/vcf/skat/example.vcf.avro");
        outPath = Paths.get("/home/jtarraga/data/vcf/skat/out");
        confPath = Paths.get("/home/jtarraga/data/vcf/skat/skat.params");
    }

//    @Test
    public void skat() {

        try {
            init();

            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant rvtests");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(inPath);
            commandLine.append(" -o ").append(outPath);
            commandLine.append(" -c ").append(confPath);
            commandLine.append(" --dataset noname");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
