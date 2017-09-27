package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Before;
import org.junit.Test;

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
    Path parquetPath;
    Path metaPath;

    @Before
    public void init() throws URISyntaxException {
        vcfPath = Paths.get(getClass().getResource("/test.vcf").toURI());
        pedPath = Paths.get(getClass().getResource("/test.ped").toURI());
        avroPath = Paths.get("/tmp/test.vcf.avro");
        parquetPath = Paths.get("/tmp/test.vcf.parquet");
        metaPath = Paths.get("/tmp/test.vcf.avro.meta.json");
    }

    @Test
    public void vcf2avro() {
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(avroPath);
            commandLine.append(" --to avro");
            commandLine.append(" --dataset test-dataset");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void vcf2parquet() {
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(parquetPath);
            commandLine.append(" --to parquet");
            commandLine.append(" --dataset test-dataset");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void avro2parquet() {
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(avroPath);
            commandLine.append(" -o ").append(parquetPath);
            commandLine.append(" --from avro");
            commandLine.append(" --to parquet");
            commandLine.append(" --dataset test-dataset");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
