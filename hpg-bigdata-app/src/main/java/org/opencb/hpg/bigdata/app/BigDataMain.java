package org.opencb.hpg.bigdata.app;

import org.opencb.hpg.bigdata.app.cli.*;

/**
 * Created by imedina on 15/03/15.
 */
public class BigDataMain {

    public static void main(String[] args) {
        CliOptionsParser cliOptionsParser = new CliOptionsParser();
        cliOptionsParser.parse(args);

        String parsedCommand = cliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
            }
            if (cliOptionsParser.getGeneralOptions().version) {
                System.out.println("version = 3.1.0");
            }
        } else {
            CommandExecutor commandExecutor = null;
            switch (parsedCommand) {
                case "fastq":
                    if (cliOptionsParser.getFastqCommandOptions().commonOptions.help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new FastqCommandExecutor(cliOptionsParser.getFastqCommandOptions());
                    }
                    break;
                case "bam":
                    if (cliOptionsParser.getBamCommandOptions().commonOptions.help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new BamCommandExecutor(cliOptionsParser.getBamCommandOptions());
                    }
                    break;
                case "ga4gh":
                    if (cliOptionsParser.getGa4ghCommandOptions().commonOptions.help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new Ga4ghCommandExecutor(cliOptionsParser.getGa4ghCommandOptions());
                    }
                    break;
                case "align":
                    if (cliOptionsParser.getAlignCommandOptions().commonOptions.help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new AlignCommandExecutor(cliOptionsParser.getAlignCommandOptions());
                    }
                    break;
                default:
                    break;
            }

            if (commandExecutor != null) {
                commandExecutor.execute();
            }
        }
    }
}
