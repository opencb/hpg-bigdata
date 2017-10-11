package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
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
//        vcfPath = Paths.get(getClass().getResource("/test.vcf").toURI());
//        pedPath = Paths.get(getClass().getResource("/test.ped").toURI());
//        avroPath = Paths.get("/tmp/test.vcf.avro");
//        parquetPath = Paths.get("/tmp/test.vcf.parquet");
//        metaPath = Paths.get("/tmp/test.vcf.avro.meta.json");
    }

    //@Test
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

    //@Test
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

    //@Test
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

    @Test
    public void avroAnnotate() {
        String vcfFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/utils/VariantContextBlockIteratorTest.vcf";
        vcfPath = Paths.get(vcfFilename);
        avroPath = Paths.get("/tmp/" + vcfPath.getFileName() + ".avro");

        new File(avroPath.toString()).delete();
        new File(avroPath.toString() + ".meta.json").delete();

        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(avroPath);
            commandLine.append(" --dataset test-dataset");
            commandLine.append(" --annotate");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void avroAnnotateParallel() {
        String vcfFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/utils/VariantContextBlockIteratorTest.vcf";
        vcfPath = Paths.get(vcfFilename);
        avroPath = Paths.get("/tmp/" + vcfPath.getFileName() + ".avro");

        new File(avroPath.toString()).delete();
        new File(avroPath.toString() + ".meta.json").delete();

        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(avroPath);
            commandLine.append(" --dataset test-dataset");
            commandLine.append(" --annotate");
            commandLine.append(" -t 4");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void parquetAnnotate() {
        String vcfFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/utils/VariantContextBlockIteratorTest.vcf";
        vcfPath = Paths.get(vcfFilename);
        Path parquetPath = Paths.get("/tmp/" + vcfPath.getFileName() + ".parquet");

        new File(parquetPath.toString()).delete();
        new File(parquetPath.toString() + ".meta.json").delete();

        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(parquetPath);
            commandLine.append(" --to parquet");
            commandLine.append(" --dataset test-dataset");
            commandLine.append(" --annotate");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void parquetAnnotateParallel() {
        String vcfFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/utils/VariantContextBlockIteratorTest.vcf";
        //String vcfFilename = "/home/jtarraga/data150/vcf/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf";
        //String vcfFilename = "/home/jtarraga/data150/vcf/2.vcf";
        //String vcfFilename = "/home/jtarraga/data150/vcf/5k.vcf";
        //String vcfFilename = "/home/jtarraga/data150/vcf/100k.vcf";
        vcfPath = Paths.get(vcfFilename);
        Path parquetPath = Paths.get("/tmp/" + vcfPath.getFileName() + ".parquet");

        new File(parquetPath.toString()).delete();
        new File(parquetPath.toString() + ".meta.json").delete();

        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(parquetPath);
            commandLine.append(" --to parquet");
            commandLine.append(" --dataset test-dataset");
            commandLine.append(" --annotate");
            commandLine.append(" -t 4");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
