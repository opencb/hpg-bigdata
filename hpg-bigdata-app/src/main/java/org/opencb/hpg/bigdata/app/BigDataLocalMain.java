package org.opencb.hpg.bigdata.app;

import com.beust.jcommander.ParameterException;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.local.LocalCliOptionsParser;
import org.opencb.hpg.bigdata.app.cli.local.VariantCommandExecutor;

/**
 * Created by hpccoll1 on 18/05/15.
 */
public class BigDataLocalMain {

    public static void main(String[] args) {
        LocalCliOptionsParser localCliOptionsParser = new LocalCliOptionsParser();

        if (args == null || args.length == 0) {
            localCliOptionsParser.printUsage();
        }

        try {
            localCliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            localCliOptionsParser.printUsage();
            System.exit(-1);
        }


//        String parsedCommand = localCliOptionsParser.getCommand();
//        if (parsedCommand == null || parsedCommand.isEmpty()) {
//            if (localCliOptionsParser.getGeneralOptions().help) {
//                localCliOptionsParser.printUsage();
//                System.exit(0);
//            }
//            if (localCliOptionsParser.getGeneralOptions().version) {
//                BigDataMain.printVersion();
//            }
//        }
        String parsedCommand = localCliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (localCliOptionsParser.getGeneralOptions().help) {
                localCliOptionsParser.printUsage();
                System.exit(0);
            }
            if (localCliOptionsParser.getGeneralOptions().version) {
//                printVersion();
            }
        } else {    // correct command exist
            CommandExecutor commandExecutor = null;
            // Check if any command or subcommand -h options are present
            if (localCliOptionsParser.getCommandOptions().help || localCliOptionsParser.getCommonCommandOptions().help) {
                localCliOptionsParser.printUsage();
            } else {
                // get the subcommand and printUsage if empty
                String parsedSubCommand = localCliOptionsParser.getSubCommand();
                if (parsedSubCommand == null || parsedSubCommand.isEmpty()) {
                    localCliOptionsParser.printUsage();
                } else {
                    switch (parsedCommand) {
                        case "sequence":
//                    commandExecutor = new SequenceCommandExecutor(cliOptionsParser.getSequenceCommandOptions());
                            break;
                        case "alignment":
//                    commandExecutor = new BamCommandExecutor(cliOptionsParser.getAlignmentCommandOptions());
                            break;
                        case "variant":
                            commandExecutor = new VariantCommandExecutor(localCliOptionsParser.getVariantCommandOptions());
                            break;
                        default:
                            System.out.printf("ERROR: not valid command: '" + parsedCommand + "'");
                            localCliOptionsParser.printUsage();
                            break;
                    }

                    if (commandExecutor != null) {
                        try {
                            commandExecutor.execute();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(1);
                        }
                    }
                }
            }
        }
    }
}
