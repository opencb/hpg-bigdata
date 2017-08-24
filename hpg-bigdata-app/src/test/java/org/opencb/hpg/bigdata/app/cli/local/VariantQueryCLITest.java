package org.opencb.hpg.bigdata.app.cli.local;

import org.junit.Test;
import org.opencb.hpg.bigdata.app.cli.local.executors.VariantCommandExecutor;

/**
 * Created by jtarraga on 12/10/16.
 */
public class VariantQueryCLITest {

    //        String inputFilename = "/tmp/chr8.platinum.avro";
//    String inputFilename = "/home/jtarraga/data150/spark/chr8.platinum.avro";
//    String inputFilename = "/home/jtarraga/data150/spark/chr8.platinum.parquet";
    String inputFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/lib/100.variants.avro";
    String outputFilename = "/tmp/query.out";

    public static void execute(String commandLine) throws Exception {
        System.out.println("Executing:\n" + commandLine + "\n");
        LocalCliOptionsParser parser = new LocalCliOptionsParser();
        parser.parse(commandLine.split(" "));

        VariantCommandExecutor executor = new VariantCommandExecutor(parser.getVariantCommandOptions());
        executor.execute();
    }

    @Test
    public void query00() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --type INDEL");
        commandLine.append(" --limit 10");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query0() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query1() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --limit 3");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void query2() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query3() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --gene \"BIN3,ZNF517\"");
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query4() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --limit 100");
        commandLine.append(" --group-by gene");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query5() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --limit 10");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query6() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query7() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --gene \"BIN3,ZNF517\"");
        commandLine.append(" --count");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void query8() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --limit 100");
        commandLine.append(" --group-by gene");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query9() {

        inputFilename = "/home/jtarraga/data150/vcf/chr22.head1k.vcf.avro";
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"HG00100:1|0,1|1\"");
        commandLine.append(" --limit 100");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void query10() {
        VariantMetadataCLITest metaCLI = new VariantMetadataCLITest();
        metaCLI.loadPedigree();

        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(metaCLI.avroPath);
        commandLine.append(" --sample-genotype \"5:1|0\"");
        //commandLine.append(" --sample-filter \"Eyes==Green\"");
        commandLine.append(" --limit 100");

        try {
            execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
