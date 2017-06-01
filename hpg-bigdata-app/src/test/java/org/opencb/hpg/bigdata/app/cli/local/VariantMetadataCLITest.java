package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by jtarraga on 10/01/17.
 */
public class VariantMetadataCLITest {

    Path vcfPath;
    Path pedPath;
    Path avroPath;

    private void init() throws URISyntaxException {
        vcfPath = Paths.get(getClass().getResource("/test.vcf").toURI());
        pedPath = Paths.get(getClass().getResource("/test.ped").toURI());
        avroPath = Paths.get("/tmp/test.vcf.avro");
    }

    @Test
    public void loadPedigree() {

        try {
            init();

            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(avroPath);
            commandLine.append(" --dataset testing-pedigree");

            VariantQueryCLITest.execute(commandLine.toString());

            commandLine.setLength(0);
            commandLine.append(" variant metadata");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(avroPath);
            commandLine.append(" --load-pedigree ").append(pedPath);
            commandLine.append(" --dataset testing-pedigree");

            VariantQueryCLITest.execute(commandLine.toString());

            commandLine.setLength(0);
            commandLine.append(" variant metadata");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(avroPath);
            commandLine.append(" --save-pedigree ").append(Paths.get("/tmp/test.vcf.ped"));
            commandLine.append(" --dataset testing-pedigree");

            VariantQueryCLITest.execute(commandLine.toString());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
