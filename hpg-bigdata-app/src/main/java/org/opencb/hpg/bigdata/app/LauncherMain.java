package org.opencb.hpg.bigdata.app;

import org.opencb.hpg.bigdata.app.cli.local.LocalCliOptionsParser;
import org.opencb.hpg.bigdata.app.cli.local.VariantCommandExecutor;

/**
 * Created by jtarraga on 09/11/16.
 */
public class LauncherMain {
    public static void main(String[] args) {
        System.out.println("hello, world");

        String inputFilename = "../hpg-bigdata-core/src/test/resources/org/opencb/hpg/bigdata/core/lib/100.variants.avro";
        StringBuilder commandLine = new StringBuilder();
        commandLine.append(" variant query");
        commandLine.append(" --log-level ERROR");
        commandLine.append(" -i ").append(inputFilename);
        commandLine.append(" --count");

        System.out.println("Executing:\n" + commandLine + "\n");
        LocalCliOptionsParser parser = new LocalCliOptionsParser();
        parser.parse(commandLine.toString().split(" "));
        VariantCommandExecutor executor = new VariantCommandExecutor(parser.getVariantCommandOptions());
        try {
            executor.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(2);
    }
}
