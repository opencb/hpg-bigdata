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

    public Path vcfPath;
    public Path avroPath;
    public Path metaAvroPath;
    public Path parquetPath;
    public Path metaParquetPath;

    @Before
    public void init() throws URISyntaxException {
        String vcfFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/utils/VariantContextBlockIteratorTest.vcf";
        //String vcfFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/utils/VcfBlockIteratorTest.vcf";
        //String vcfFilename = "../../../data150/vcf/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf";
        //String vcfFilename = "../../../data150/vcf/2.vcf";
        //String vcfFilename = "../../../data150/vcf/5k.vcf";
        //String vcfFilename = "../../../data150/vcf/100k.vcf";

        vcfPath = Paths.get(vcfFilename);
        avroPath = Paths.get("/tmp/" + vcfPath.getFileName() + ".avro");
        metaAvroPath = Paths.get(avroPath + ".meta.json");
        parquetPath = Paths.get("/tmp/" + vcfPath.getFileName() + ".parquet");
        metaParquetPath = Paths.get(parquetPath + ".meta.json");

        // Delete output files before testing
        deleteFiles();
    }

    public void deleteFiles() {
        if (avroPath != null) { avroPath.toFile().delete(); }
        if (metaAvroPath != null) { metaAvroPath.toFile().delete(); }
        if (parquetPath != null) { parquetPath.toFile().delete(); }
        if (metaParquetPath != null) { metaParquetPath.toFile().delete(); }
    }

    @Test
    public void vcf2avro() {
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append("variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(avroPath);
            commandLine.append(" --to avro");
            commandLine.append(" --dataset test-dataset");
            commandLine.append(" -t 1");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void vcf2parquet() {
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append("variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(parquetPath);
            commandLine.append(" --to parquet");
            commandLine.append(" --dataset test-dataset");
            commandLine.append(" -t 1");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void avro2parquet() {
        try {
            vcf2avro();

            StringBuilder commandLine = new StringBuilder();
            commandLine.append("variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(avroPath);
            commandLine.append(" -o ").append(parquetPath);
            commandLine.append(" --from avro");
            commandLine.append(" --to parquet");
            commandLine.append(" --dataset test-dataset");
            commandLine.append(" -t 1");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void avroAnnotate() {
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append("variant convert");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(vcfPath);
            commandLine.append(" -o ").append(avroPath);
            commandLine.append(" --dataset test-dataset");
            commandLine.append(" --annotate");
            commandLine.append(" -t 1");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void avroAnnotateParallel() {
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append("variant convert");
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
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append("variant convert");
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
        try {
            StringBuilder commandLine = new StringBuilder();
            commandLine.append("variant convert");
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
