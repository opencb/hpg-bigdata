package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by joaquin on 1/19/17.
 */
public class VariantRvTestsCLITest {
    String datasetName = "test";

    String vcfFilename = "../hpg-bigdata-app/src/test/resources/example.vcf";
    String phenoFilename = "../hpg-bigdata-app/src/test/resources/pheno";
    String outDir = "/tmp/";

    Path vcfPath;
    Path phenoPath;
    Path avroPath;
    Path metaPath;

    private void init() throws Exception {
        vcfPath = Paths.get(vcfFilename);
        phenoPath = Paths.get(phenoFilename);
        avroPath = Paths.get(outDir + "/" + vcfPath.getFileName() + ".avro");
        metaPath = Paths.get(outDir + "/" + vcfPath.getFileName() + ".avro.meta.json");

        avroPath.toFile().delete();
        metaPath.toFile().delete();

        // convert vcf to avro
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant convert");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(vcfPath);
        commandLine.append(" -o ").append(avroPath);
        commandLine.append(" --dataset ").append(datasetName);
        VariantQueryCLITest.execute(commandLine.toString());

        // load pedigree file
        commandLine.setLength(0);
        commandLine.append(" variant metadata");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(avroPath);
        commandLine.append(" --load-pedigree ").append(phenoPath);
        commandLine.append(" --dataset ").append(datasetName);
        VariantQueryCLITest.execute(commandLine.toString());
    }

    @Test
    public void skat() {
        try {
            init();

            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant rvtests");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" --dataset ").append(datasetName);
            commandLine.append(" -i ").append(avroPath);
            commandLine.append(" -Dsingle=wald");
            commandLine.append(" -Dout=/tmp/out.wald");
            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
