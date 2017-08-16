package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Before;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by jtarraga on 10/01/17.
 */
public class VariantConvertCLITest {

    Path vcfPath;
    Path pedPath;
    Path avroPath;
    Path metaPath;

    @Before
    public void init() throws URISyntaxException {
        vcfPath = Paths.get(getClass().getResource("/test.vcf").toURI());
        pedPath = Paths.get(getClass().getResource("/test.ped").toURI());
        avroPath = Paths.get("/tmp/test.vcf.avro");
        metaPath = Paths.get("/tmp/test.vcf.avro.meta.json");
    }

    //@Test
    public void convert() {
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(avroPath);
            commandLine.append(" --dataset test-dataset");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
