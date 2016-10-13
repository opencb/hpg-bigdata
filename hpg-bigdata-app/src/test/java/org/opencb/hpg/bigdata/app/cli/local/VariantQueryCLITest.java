package org.opencb.hpg.bigdata.app.cli.local;

import org.apache.commons.jexl2.UnifiedJEXL;
import org.junit.Test;

/**
 * Created by jtarraga on 12/10/16.
 */
public class VariantQueryCLITest {

//        String inputFilename = "/tmp/chr8.platinum.avro";
//    String inputFilename = "/home/jtarraga/data150/spark/chr8.platinum.avro";
//    String inputFilename = "/home/jtarraga/data150/spark/chr8.platinum.parquet";
    String inputFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/lib/100.variants.avro";
    String outputFilename = "/tmp/query.out";

    private void execute(String commandLine) {
        System.out.println("Executing:\n" + commandLine + "\n");
        LocalCliOptionsParser parser = new LocalCliOptionsParser();
        parser.parse(commandLine.split(" "));

        VariantCommandExecutor executor = new VariantCommandExecutor(parser.getVariantCommandOptions());
        try {
            executor.execute();
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

        execute(commandLine.toString());
    }

    //    @Test
    public void query1() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --limit 3");

        execute(commandLine.toString());
    }

//    @Test
    public void query2() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --population-maf \"1000GENOMES_phase_3::ALL<0.01,EXAC::ALL<0.01\"");
        commandLine.append(" --protein-substitution \"sift<0.2\"");
        commandLine.append(" --consequence-type \"missense_variant\"");
        commandLine.append(" --count");

        execute(commandLine.toString());
    }

//    @Test
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

        execute(commandLine.toString());
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

        execute(commandLine.toString());
    }

//    @Test
    public void query5() {
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --sample-genotype \"0:0|0;3:1|0,1|1\"");
        commandLine.append(" --limit 10");

        execute(commandLine.toString());
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

        execute(commandLine.toString());
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

        execute(commandLine.toString());
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

        execute(commandLine.toString());
    }
}