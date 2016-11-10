package org.opencb.hpg.bigdata.app.cli.local;

import org.apache.commons.jexl2.UnifiedJEXL;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

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

    public class InputStreamReaderRunnable implements Runnable {
        private BufferedReader reader;
        private String name;
        public InputStreamReaderRunnable(InputStream is, String name) {
            this.reader = new BufferedReader(new InputStreamReader(is));
            this.name = name;
        }

        public void run() {
            System.out.println("InputStream " + name + ":");
            try {
                String line = reader.readLine();
                while (line != null) {
                    System.out.println(line);
                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void query00() {
        Process spark = null;
        try {
            spark = new SparkLauncher()
                    .setSparkHome("/home/jtarraga/soft/spark-2.0.0/")
                    .setAppResource("/home/jtarraga/.m2/repository/org/opencb/hpg-bigdata/hpg-bigdata-app/1.0.0-beta-nvm/hpg-bigdata-app-1.0.0-beta-nvm-jar-with-dependencies.jar")
                    .setMainClass("org.opencb.hpg.bigdata.app.LauncherMain")
//                    .setDeployMode("client")
                    .setDeployMode("cluster")
                    // to use yarn, you must: export HADOOP_CONF_DIR=/home/jtarraga/soft/spark-2.0.0/conf/
                    // otherwise:
                    // Exception in thread "main" java.lang.Exception: When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.
                    .setMaster("yarn").launch();

//            .setMaster("local[1]").launch();

//            spark = new SparkLauncher()
//                    .setSparkHome("C:\\spark-1.4.1-bin-hadoop2.6")
//                    .setAppResource("C:\\spark-1.4.1-bin-hadoop2.6\\lib\\spark-examples-1.4.1-hadoop2.6.0.jar")
//                    .setMainClass("org.apache.spark.examples.SparkPi").setMaster("yarn-cluster").launch();

            InputStreamReaderRunnable inputStreamReaderRunnable;
            inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");

            Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
            inputThread.start();

            InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
            Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
            errorThread.start();

            System.out.println("Waiting for finish...");
            int exitCode = spark.waitFor();
            System.out.println("Finished! Exit code:" + exitCode);
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
